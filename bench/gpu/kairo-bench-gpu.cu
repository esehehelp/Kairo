// kairo-bench-gpu: fixed, deterministic GPU workloads for Kairo's benchmark
// harness. One workload runs for a given duration and streams JSON lines
// (schema "kairo.bench/v1") to stdout. See --help for the contract.
//
// Structure (safety-relevant): main() only parses and validates arguments
// (pure C++). --help, --version and --validate-only return from main()
// before run_benchmark() is called; run_benchmark() is the only path that
// touches NVML, the CUDA runtime or cuBLAS.

#define NVML_NO_UNVERSIONED_FUNC_DEFS

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <fcntl.h>
#include <intrin.h>
#include <io.h>
#pragma comment(lib, "advapi32.lib")
#pragma comment(lib, "kernel32.lib")
#else
#include <dlfcn.h>
#include <signal.h>
#include <unistd.h>
#endif

#include <cublas_v2.h>
#include <cuda_bf16.h>
#include <cuda_runtime.h>
#include <nvml.h>  // types and constants only; NVML is loaded at run time

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#ifndef KAIRO_BENCH_BUILD
#define KAIRO_BENCH_BUILD "unknown"
#endif

#define KB_STR2(x) #x
#define KB_STR(x) KB_STR2(x)

namespace {

const char* const kSchema = "kairo.bench/v1";
const char* const kProgram = "kairo-bench-gpu";
const char* const kBuild = KAIRO_BENCH_BUILD;

const uint64_t KiB = 1024ull;
const uint64_t MiB = 1024ull * KiB;
const uint64_t GiB = 1024ull * MiB;
const uint64_t TiB = 1024ull * GiB;

const uint64_t kWorkspaceBytes = 32 * MiB;   // cublasSetWorkspace buffer (ours)
const uint64_t kCublasAllowance = 64 * MiB;  // cuBLAS handle internals
const uint64_t kCtxOverhead = 400 * MiB;     // context estimate (NVML pre-check)
const uint64_t kPcieBandwidthBytes = 64 * MiB;
const uint64_t kPcieLatencyBytes = 4 * KiB;
const double kBatchTargetMs = 20.0;
const double kCalibMinMs = 10.0;
const uint64_t kMaxBatchOps = 65536;
const uint64_t kLatencyWarmupOps = 100;
const int64_t kPollNs = 1000000;  // 1 ms event poll in batched mode

using Clock = std::chrono::steady_clock;

// ===========================================================================
// Parameters and argument parsing (pure C++, no CUDA/NVML/cuBLAS)
// ===========================================================================

enum class Workload { Gemm, Copy, Gemv, Pcie };

const char* workload_name(Workload w) {
  switch (w) {
    case Workload::Gemm: return "gemm";
    case Workload::Copy: return "copy";
    case Workload::Gemv: return "gemv";
    case Workload::Pcie: return "pcie";
  }
  return "?";
}

// Workload definition versions. Bump when what one unit of work means, the
// kernel, the data or the measurement method of a workload changes.
int workload_version(Workload w) {
  switch (w) {
    case Workload::Gemm: return 1;
    case Workload::Copy: return 1;
    case Workload::Gemv: return 1;
    case Workload::Pcie: return 1;
  }
  return 0;
}

struct Params {
  Workload wl = Workload::Gemm;
  std::string device;          // normalized "GPU-xxxxxxxx-..." (lowercase hex)
  int64_t seconds_ms = 60000;  // 0 = until signalled
  int64_t interval_ms = 1000;
  int64_t sample_ms = 100;
  uint64_t reserve_mib = 512;
  uint64_t n = 8192;           // gemm, gemv
  uint64_t bytes = 2 * GiB;    // copy (src+dst footprint)
  uint64_t batch = 1;          // gemv
  bool d2h = false;            // pcie
  bool latency = false;        // pcie
  // derived
  uint64_t transfer_bytes = 0;  // pcie
  uint64_t bytes_per_op = 0;
  uint64_t flop_per_op = 0;
  bool has_bytes = false;
  bool has_flop = false;
};

void derive(Params& p) {
  p.has_bytes = p.has_flop = false;
  p.bytes_per_op = p.flop_per_op = 0;
  switch (p.wl) {
    case Workload::Gemm:
      p.has_flop = true;
      p.flop_per_op = 2ull * p.n * p.n * p.n;
      break;
    case Workload::Copy:
      p.has_bytes = true;
      p.bytes_per_op = 2ull * (p.bytes / 2);  // read + write (STREAM "copy")
      break;
    case Workload::Gemv:
      p.has_bytes = p.has_flop = true;
      p.bytes_per_op = 2ull * p.n * p.n + 2ull * p.n * p.batch + 2ull * p.n * p.batch;
      p.flop_per_op = 2ull * p.n * p.n * p.batch;
      break;
    case Workload::Pcie:
      p.has_bytes = true;
      p.transfer_bytes = p.latency ? kPcieLatencyBytes : kPcieBandwidthBytes;
      p.bytes_per_op = p.transfer_bytes;
      break;
  }
}

// Device memory our buffers need (without cuBLAS allowance / reserve).
uint64_t buffer_bytes(const Params& p) {
  switch (p.wl) {
    case Workload::Gemm: return 3ull * p.n * p.n * 2ull + kWorkspaceBytes;
    case Workload::Copy: return p.bytes;
    case Workload::Gemv: return (p.n * p.n + 2ull * p.n * p.batch) * 2ull + kWorkspaceBytes;
    case Workload::Pcie: return p.transfer_bytes;
  }
  return 0;
}

bool uses_cublas(const Params& p) { return p.wl == Workload::Gemm || p.wl == Workload::Gemv; }

uint64_t need_bytes(const Params& p) {
  return buffer_bytes(p) + (uses_cublas(p) ? kCublasAllowance : 0);
}

bool all_digits(const std::string& s) {
  if (s.empty()) return false;
  for (char c : s)
    if (c < '0' || c > '9') return false;
  return true;
}

bool parse_uint(const std::string& s, uint64_t& out) {
  if (!all_digits(s) || s.size() > 19) return false;
  out = std::strtoull(s.c_str(), nullptr, 10);
  return true;
}

// Plain decimal: digits with at most one '.', at least one digit. No sign,
// exponent, hex, inf or nan.
bool parse_dec(const std::string& s, double& out) {
  int digits = 0, dots = 0;
  for (char c : s) {
    if (c >= '0' && c <= '9') ++digits;
    else if (c == '.') ++dots;
    else return false;
  }
  if (digits == 0 || dots > 1 || s.size() > 32) return false;
  out = std::strtod(s.c_str(), nullptr);
  return std::isfinite(out);
}

// Integer with an optional binary suffix: Ki, Mi, Gi, Ti (optionally followed
// by B), or a bare B. Decimal suffixes (K, M, G, KB, ...) are rejected as
// ambiguous.
bool parse_bytes(const std::string& s, uint64_t& out) {
  size_t i = 0;
  while (i < s.size() && s[i] >= '0' && s[i] <= '9') ++i;
  std::string num = s.substr(0, i), suf = s.substr(i);
  uint64_t v = 0;
  if (!parse_uint(num, v)) return false;
  uint64_t mul = 1;
  if (suf.empty() || suf == "B") mul = 1;
  else if (suf == "Ki" || suf == "KiB") mul = KiB;
  else if (suf == "Mi" || suf == "MiB") mul = MiB;
  else if (suf == "Gi" || suf == "GiB") mul = GiB;
  else if (suf == "Ti" || suf == "TiB") mul = TiB;
  else return false;
  if (v != 0 && mul > UINT64_MAX / v) return false;
  out = v * mul;
  return true;
}

bool is_hex(char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

// GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (prefix case-insensitive); the
// normalized form uses "GPU-" and lowercase hex, as nvidia-smi prints it.
bool normalize_uuid(const std::string& s, std::string& norm) {
  static const int groups[5] = {8, 4, 4, 4, 12};
  if (s.size() != 4 + 36) return false;
  if (!((s[0] == 'G' || s[0] == 'g') && (s[1] == 'P' || s[1] == 'p') &&
        (s[2] == 'U' || s[2] == 'u') && s[3] == '-'))
    return false;
  size_t pos = 4;
  norm = "GPU-";
  for (int g = 0; g < 5; ++g) {
    for (int k = 0; k < groups[g]; ++k, ++pos) {
      char c = s[pos];
      if (!is_hex(c)) return false;
      norm += (c >= 'A' && c <= 'F') ? char(c - 'A' + 'a') : c;
    }
    if (g < 4) {
      if (s[pos] != '-') return false;
      norm += '-';
      ++pos;
    }
  }
  return pos == s.size();
}

enum class Action { Run, Help, Version, Validate };

struct Cli {
  Action action = Action::Run;
  Params p;
};

const char* const kValuedFlags[] = {"--device", "--seconds", "--interval", "--sample-ms",
                                    "--n",      "--bytes",   "--batch",    "--dir",
                                    "--mode",   "--reserve-mib"};

bool is_valued_flag(const std::string& f) {
  for (const char* v : kValuedFlags)
    if (f == v) return true;
  return false;
}

bool flag_applies(const std::string& f, Workload w) {
  if (f == "--n") return w == Workload::Gemm || w == Workload::Gemv;
  if (f == "--bytes") return w == Workload::Copy;
  if (f == "--batch") return w == Workload::Gemv;
  if (f == "--dir" || f == "--mode") return w == Workload::Pcie;
  return true;
}

// Returns true on success; on usage error fills err.
bool parse_args(int argc, char** argv, Cli& cli, std::string& err) {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--help" || a == "-h") {
      cli.action = Action::Help;
      return true;
    }
  }
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--version") {
      cli.action = Action::Version;
      return true;
    }
  }

  std::map<std::string, std::string> vals;
  std::string wl;
  bool validate = false;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--validate-only") {
      if (validate) {
        err = "--validate-only given more than once";
        return false;
      }
      validate = true;
      continue;
    }
    if (a.size() >= 2 && a[0] == '-') {
      std::string name = a, val;
      bool has_val = false;
      size_t eq = a.find('=');
      if (eq != std::string::npos) {
        name = a.substr(0, eq);
        val = a.substr(eq + 1);
        has_val = true;
      }
      if (!is_valued_flag(name)) {
        err = "unknown flag " + name;
        return false;
      }
      if (!has_val) {
        if (i + 1 >= argc) {
          err = name + " requires a value";
          return false;
        }
        val = argv[++i];
      }
      if (vals.count(name)) {
        err = name + " given more than once";
        return false;
      }
      vals[name] = val;
      continue;
    }
    if (!wl.empty()) {
      err = "unexpected argument '" + a + "' (WORKLOAD already given as '" + wl + "')";
      return false;
    }
    wl = a;
  }

  Params& p = cli.p;
  if (wl.empty()) {
    err = "missing WORKLOAD (gemm, copy, gemv or pcie)";
    return false;
  }
  if (wl == "gemm") p.wl = Workload::Gemm;
  else if (wl == "copy") p.wl = Workload::Copy;
  else if (wl == "gemv") p.wl = Workload::Gemv;
  else if (wl == "pcie") p.wl = Workload::Pcie;
  else {
    err = "unknown WORKLOAD '" + wl + "' (want gemm, copy, gemv or pcie)";
    return false;
  }
  for (const auto& kv : vals) {
    if (!flag_applies(kv.first, p.wl)) {
      err = kv.first + " does not apply to workload " + wl;
      return false;
    }
  }

  auto it = vals.find("--device");
  if (it == vals.end()) {
    err = "--device GPU-UUID is required";
    return false;
  }
  if (!normalize_uuid(it->second, p.device)) {
    err = "--device '" + it->second +
          "' is not a GPU UUID of the form GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx";
    return false;
  }

  if ((it = vals.find("--seconds")) != vals.end()) {
    double s = 0;
    if (!parse_dec(it->second, s) || s > 86400.0) {
      err = "--seconds must be a number of seconds in 0..86400 (0 = until signalled)";
      return false;
    }
    p.seconds_ms = (int64_t)std::llround(s * 1000.0);
    if (s > 0 && p.seconds_ms == 0) {
      err = "--seconds must be 0 or at least 0.001";
      return false;
    }
  }
  if ((it = vals.find("--interval")) != vals.end()) {
    double s = 0;
    if (!parse_dec(it->second, s) || s > 86400.0) {
      err = "--interval must be a number of seconds in 0.05..86400";
      return false;
    }
    p.interval_ms = (int64_t)std::llround(s * 1000.0);
  }
  if (p.interval_ms < 50) {
    err = "--interval must be at least 0.05 seconds";
    return false;
  }
  if (p.seconds_ms > 0 && p.interval_ms > p.seconds_ms) {
    err = "--interval must not exceed --seconds";
    return false;
  }
  if ((it = vals.find("--sample-ms")) != vals.end()) {
    uint64_t v = 0;
    if (!parse_uint(it->second, v) || v < 10 || v > 10000) {
      err = "--sample-ms must be an integer in 10..10000";
      return false;
    }
    p.sample_ms = (int64_t)v;
  }
  if ((it = vals.find("--reserve-mib")) != vals.end()) {
    uint64_t v = 0;
    if (!parse_uint(it->second, v) || v > 1048576) {
      err = "--reserve-mib must be an integer in 0..1048576";
      return false;
    }
    p.reserve_mib = v;
  }
  if ((it = vals.find("--n")) != vals.end()) {
    uint64_t v = 0;
    if (!parse_uint(it->second, v) || v < 256 || v > 32768 || v % 64 != 0) {
      err = "--n must be a multiple of 64 in 256..32768";
      return false;
    }
    p.n = v;
  }
  if ((it = vals.find("--bytes")) != vals.end()) {
    uint64_t v = 0;
    if (!parse_bytes(it->second, v)) {
      err = "--bytes must be an integer byte count, optionally with a binary suffix "
            "(Ki, Mi, Gi, Ti, optionally followed by B), e.g. 2Gi";
      return false;
    }
    if (v < 2 * MiB || v % (2 * MiB) != 0 || v > 1024 * GiB) {
      err = "--bytes must be a multiple of 2 MiB, at least 2 MiB and at most 1 TiB";
      return false;
    }
    p.bytes = v;
  }
  if ((it = vals.find("--batch")) != vals.end()) {
    if (it->second == "1") p.batch = 1;
    else if (it->second == "8") p.batch = 8;
    else {
      err = "--batch must be 1 or 8";
      return false;
    }
  }
  if ((it = vals.find("--dir")) != vals.end()) {
    if (it->second == "h2d") p.d2h = false;
    else if (it->second == "d2h") p.d2h = true;
    else {
      err = "--dir must be h2d or d2h";
      return false;
    }
  }
  if ((it = vals.find("--mode")) != vals.end()) {
    if (it->second == "bandwidth") p.latency = false;
    else if (it->second == "latency") p.latency = true;
    else {
      err = "--mode must be bandwidth or latency";
      return false;
    }
  }
  derive(p);
  cli.action = validate ? Action::Validate : Action::Run;
  return true;
}

// ===========================================================================
// JSON writer
// ===========================================================================

std::string fmt3(double v) {
  char b[64];
  std::snprintf(b, sizeof b, "%.3f", v);
  std::string s = b;
  if (s.find('.') != std::string::npos) {
    while (!s.empty() && s.back() == '0') s.pop_back();
    if (!s.empty() && s.back() == '.') s.pop_back();
  }
  if (s == "-0") s = "0";
  return s;
}

class JW {
 public:
  JW& o() { pre(); s_ += '{'; st_.push_back(0); return *this; }
  JW& oe() { s_ += '}'; st_.pop_back(); return *this; }
  JW& a() { pre(); s_ += '['; st_.push_back(0); return *this; }
  JW& ae() { s_ += ']'; st_.pop_back(); return *this; }
  JW& k(const char* key) { pre(); esc(key); s_ += ':'; after_key_ = true; return *this; }
  JW& str(const std::string& v) { pre(); esc(v); return *this; }
  JW& u(uint64_t v) {
    pre();
    char b[32];
    std::snprintf(b, sizeof b, "%llu", (unsigned long long)v);
    s_ += b;
    return *this;
  }
  JW& i(int64_t v) {
    pre();
    char b[32];
    std::snprintf(b, sizeof b, "%lld", (long long)v);
    s_ += b;
    return *this;
  }
  JW& b(bool v) { pre(); s_ += v ? "true" : "false"; return *this; }
  JW& null() { pre(); s_ += "null"; return *this; }
  JW& num3(double v) {
    pre();
    s_ += std::isfinite(v) ? fmt3(v) : std::string("null");
    return *this;
  }
  template <class T>
  JW& ou(const std::optional<T>& v) {
    if (v) return u((uint64_t)*v);
    return null();
  }
  template <class T>
  JW& oi(const std::optional<T>& v) {
    if (v) return i((int64_t)*v);
    return null();
  }
  JW& ostr(const std::optional<std::string>& v) {
    if (v) return str(*v);
    return null();
  }
  const std::string& s() const { return s_; }

 private:
  void pre() {
    if (after_key_) {
      after_key_ = false;
      return;
    }
    if (!st_.empty()) {
      if (st_.back()) s_ += ',';
      st_.back() = 1;
    }
  }
  void esc(const std::string& v) {
    s_ += '"';
    for (unsigned char c : v) {
      switch (c) {
        case '"': s_ += "\\\""; break;
        case '\\': s_ += "\\\\"; break;
        case '\n': s_ += "\\n"; break;
        case '\r': s_ += "\\r"; break;
        case '\t': s_ += "\\t"; break;
        case '\b': s_ += "\\b"; break;
        case '\f': s_ += "\\f"; break;
        default:
          if (c < 0x20 || c == 0x7f) {
            char b[8];
            std::snprintf(b, sizeof b, "\\u%04x", c);
            s_ += b;
          } else {
            s_ += (char)c;
          }
      }
    }
    s_ += '"';
  }
  std::string s_;
  std::vector<char> st_;
  bool after_key_ = false;
};

std::atomic<int> g_output_failed{0};

void emit_line(const std::string& line) {
  bool ok = std::fwrite(line.data(), 1, line.size(), stdout) == line.size();
  ok = ok && std::fputc('\n', stdout) != EOF;
  ok = ok && std::fflush(stdout) == 0;
  if (!ok) g_output_failed.store(1);
}

void write_params(JW& j, const Params& p) {
  j.o();
  j.k("device").str(p.device);
  j.k("seconds").num3((double)p.seconds_ms / 1000.0);
  j.k("interval_ms").i(p.interval_ms);
  j.k("sample_ms").i(p.sample_ms);
  j.k("reserve_mib").u(p.reserve_mib);
  switch (p.wl) {
    case Workload::Gemm: j.k("n").u(p.n); break;
    case Workload::Copy: j.k("bytes").u(p.bytes); break;
    case Workload::Gemv: j.k("n").u(p.n).k("batch").u(p.batch); break;
    case Workload::Pcie:
      j.k("dir").str(p.d2h ? "d2h" : "h2d");
      j.k("mode").str(p.latency ? "latency" : "bandwidth");
      j.k("transfer_bytes").u(p.transfer_bytes);
      break;
  }
  if (p.has_bytes) j.k("bytes_per_op").u(p.bytes_per_op);
  if (p.has_flop) j.k("flop_per_op").u(p.flop_per_op);
  j.oe();
}

void write_units(JW& j, const Params& p) {
  j.a();
  if (p.has_bytes) j.str("byte");
  if (p.has_flop) j.str("flop");
  j.ae();
}

void write_work(JW& j, const Params& p, uint64_t ops) {
  j.o();
  if (p.has_bytes) j.k("byte").u(ops * p.bytes_per_op);
  if (p.has_flop) j.k("flop").u(ops * p.flop_per_op);
  j.oe();
}

// ===========================================================================
// Help / version / validate-only output (no CUDA)
// ===========================================================================

void print_help() {
  std::fputs(
      "kairo-bench-gpu - fixed, deterministic GPU workloads for Kairo benchmarks\n"
      "\n"
      "Usage:\n"
      "  kairo-bench-gpu WORKLOAD --device GPU-UUID [flags]\n"
      "  kairo-bench-gpu --help | --version\n"
      "\n"
      "Runs one workload for a fixed duration on the GPU named by its UUID (as\n"
      "printed by nvidia-smi -L) and streams JSON lines to stdout: one 'start'\n"
      "record, one 'interval' record per --interval, and a final 'end' record.\n"
      "Errors are also described in one line on stderr.\n"
      "\n"
      "Workloads (all version 1; inputs are filled on the device with a fixed\n"
      "hash-based pseudo-random pattern in [-1,1), never zeros):\n"
      "  gemm   C = A*B with cublasGemmEx, A, B, C bf16, m=n=k=N, column-major,\n"
      "         no transpose, fp32 compute, alpha=1 beta=0, CUBLAS_GEMM_DEFAULT.\n"
      "         Units [flop]; flop per op = 2*N^3.\n"
      "  copy   cudaMemcpyAsync device-to-device of BYTES/2 from src to dst.\n"
      "         Units [byte]; bytes per op = 2*(BYTES/2): read + write are both\n"
      "         counted (STREAM 'copy' convention).\n"
      "  gemv   y = A*X with cublasGemmEx, A N x N, X and y N x BATCH, bf16 in/out,\n"
      "         fp32 compute (memory-bound, like LLM decode). Units [byte, flop];\n"
      "         bytes per op = 2*N*N + 2*N*BATCH + 2*N*BATCH,\n"
      "         flop per op = 2*N*N*BATCH.\n"
      "  pcie   cudaMemcpyAsync between a pinned host buffer and a device buffer in\n"
      "         --dir. bandwidth: 64 MiB transfers; latency: 4 KiB transfers, each\n"
      "         synchronized on its own (spin-wait), with per-interval latency\n"
      "         percentiles. Units [byte]; bytes per op = transfer size.\n"
      "\n",
      stdout);
  std::fputs(
      "Flags:\n"
      "  --device GPU-UUID   required; GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\n"
      "  --seconds S         run duration, 0..86400 (default 60; 0 = until signalled;\n"
      "                      rounded to whole milliseconds)\n"
      "  --interval S        interval length in seconds, >= 0.05 and <= --seconds\n"
      "                      (default 1; rounded to whole milliseconds)\n"
      "  --sample-ms N       NVML sampling period, 10..10000 (default 100)\n"
      "  --reserve-mib N     device memory that must stay free after our\n"
      "                      allocations (default 512)\n"
      "  gemm:  --n N        matrix side, multiple of 64 in 256..32768 (default 8192)\n"
      "  copy:  --bytes B    total device footprint src+dst, multiple of 2 MiB,\n"
      "                      2 MiB..1 TiB (default 2Gi); suffixes Ki Mi Gi Ti\n"
      "                      (optionally followed by B)\n"
      "  gemv:  --n N        matrix side, multiple of 64 in 256..32768 (default 8192)\n"
      "         --batch 1|8  columns of X (default 1)\n"
      "  pcie:  --dir h2d|d2h            (default h2d)\n"
      "         --mode bandwidth|latency (default bandwidth)\n"
      "  --validate-only     parse and validate, print the effective parameters as\n"
      "                      one JSON line (type \"params\") and exit; touches no GPU\n"
      "  --version           print program, build and workload versions as JSON\n"
      "  --help              this text\n"
      "Flags accept '--flag value' or '--flag=value'. A flag that does not apply to\n"
      "the chosen workload is a usage error.\n"
      "\n",
      stdout);
  std::fputs(
      "Measurement:\n"
      "  Setup (device selection, memory checks, allocation, input fill, one\n"
      "  warmup op and a timed calibration) is untimed. Work is then issued in\n"
      "  batches of ops sized to ~20 ms of device time, bracketed by CUDA events,\n"
      "  two batches in flight. A batch counts in the interval in which the host\n"
      "  observes its end event; device_busy_ns sums the batches' event times.\n"
      "  Batches still in flight when the run stops are drained, not counted.\n"
      "  Interval boundaries are at origin + k*interval on the monotonic clock.\n"
      "  NVML (loaded dynamically; optional) is sampled every --sample-ms on a\n"
      "  separate thread; total energy is read at every interval boundary.\n"
      "\n"
      "Memory safety (never allocate-and-fail):\n"
      "  before creating a context: NVML free >= need + reserve + 400 MiB;\n"
      "  after creating it: cudaMemGetInfo free >= need + reserve, where need =\n"
      "  buffers (+ 32 MiB cuBLAS workspace + 64 MiB cuBLAS allowance for gemm and\n"
      "  gemv). Otherwise the run is refused.\n"
      "\n"
      "Stopping:\n"
      "  SIGINT/SIGTERM (Linux) or CTRL_C/CTRL_BREAK/CTRL_CLOSE (Windows) stop\n"
      "  issuing work, drain the device, emit the final partial interval and an\n"
      "  'end' record with reason \"signal\", free everything and exit 0.\n"
      "\n"
      "Exit status:\n"
      "  0  run completed (duration reached) or stopped by a signal\n"
      "  1  error or refusal at run time (the 'end' record carries it)\n"
      "  2  usage error (message on stderr, no JSON)\n"
      "\n"
      "Output: JSON lines, schema \"kairo.bench/v1\"; every record carries schema,\n"
      "type (start|interval|end), run_id, program, workload, wall (RFC 3339 UTC,\n"
      "nanoseconds) and mono_ns (monotonic ns since the start record).\n",
      stdout);
  std::fflush(stdout);
}

void print_version() {
  JW j;
  j.o();
  j.k("program").str(kProgram);
  j.k("build").str(kBuild);
  j.k("schema").str(kSchema);
  j.k("workloads").o();
  for (Workload w : {Workload::Gemm, Workload::Copy, Workload::Gemv, Workload::Pcie})
    j.k(workload_name(w)).i(workload_version(w));
  j.oe();
  j.k("cuda_runtime_build").i(CUDART_VERSION);
  j.k("cublas_build").i(CUBLAS_VER_MAJOR * 10000 + CUBLAS_VER_MINOR * 100 + CUBLAS_VER_PATCH);
  j.oe();
  emit_line(j.s());
}

void print_validated(const Params& p) {
  JW j;
  j.o();
  j.k("schema").str(kSchema);
  j.k("type").str("params");
  j.k("program").str(kProgram);
  j.k("workload").str(workload_name(p.wl));
  j.k("version").i(workload_version(p.wl));
  j.k("build").str(kBuild);
  j.k("params");
  write_params(j, p);
  j.k("units");
  write_units(j, p);
  j.oe();
  emit_line(j.s());
}

// ===========================================================================
// Stop signal handling
// ===========================================================================

static_assert(std::atomic<int>::is_always_lock_free, "lock-free atomic needed in handler");
std::atomic<int> g_stop{0};

#ifdef _WIN32
HANDLE g_done_event = nullptr;

BOOL WINAPI ctrl_handler(DWORD type) {
  switch (type) {
    case CTRL_C_EVENT:
    case CTRL_BREAK_EVENT:
      g_stop.store(1);
      return TRUE;
    case CTRL_CLOSE_EVENT:
      // The process is terminated when this handler returns: give the main
      // thread time to drain, emit 'end' and free resources.
      g_stop.store(1);
      if (g_done_event) WaitForSingleObject(g_done_event, 4500);
      return TRUE;
    default:
      return FALSE;
  }
}

void install_stop_handlers() {
  g_done_event = CreateEventW(nullptr, TRUE, FALSE, nullptr);
  // A child started with CREATE_NEW_PROCESS_GROUP inherits "ignore CTRL_C";
  // undo that so CTRL_C reaches the handler too (CTRL_BREAK always does).
  SetConsoleCtrlHandler(nullptr, FALSE);
  SetConsoleCtrlHandler(ctrl_handler, TRUE);
}

void signal_done() {
  if (g_done_event) SetEvent(g_done_event);
}
#else
void on_stop_signal(int) { g_stop.store(1); }

void install_stop_handlers() {
  struct sigaction sa;
  std::memset(&sa, 0, sizeof sa);
  sa.sa_handler = on_stop_signal;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGINT, &sa, nullptr);
  sigaction(SIGTERM, &sa, nullptr);
  signal(SIGPIPE, SIG_IGN);  // a closed stdout becomes a write error, not a kill
}

void signal_done() {}
#endif

// ===========================================================================
// Time helpers
// ===========================================================================

std::string wall_now() {
  struct timespec ts;
  if (timespec_get(&ts, TIME_UTC) != TIME_UTC) {
    ts.tv_sec = std::time(nullptr);
    ts.tv_nsec = 0;
  }
  struct tm tmv;
#ifdef _WIN32
  gmtime_s(&tmv, &ts.tv_sec);
#else
  gmtime_r(&ts.tv_sec, &tmv);
#endif
  char b[64];
  std::snprintf(b, sizeof b, "%04d-%02d-%02dT%02d:%02d:%02d.%09ldZ", tmv.tm_year + 1900,
                tmv.tm_mon + 1, tmv.tm_mday, tmv.tm_hour, tmv.tm_min, tmv.tm_sec,
                (long)ts.tv_nsec);
  return b;
}

#ifdef _WIN32
#ifndef CREATE_WAITABLE_TIMER_HIGH_RESOLUTION
#define CREATE_WAITABLE_TIMER_HIGH_RESOLUTION 0x00000002
#endif
struct WinTimer {
  HANDLE h = nullptr;
  WinTimer() {
    h = CreateWaitableTimerExW(nullptr, nullptr, CREATE_WAITABLE_TIMER_HIGH_RESOLUTION,
                               TIMER_ALL_ACCESS);
  }
  ~WinTimer() {
    if (h) CloseHandle(h);
  }
};
#endif

// Sleeps until tp with ~sub-millisecond precision (high-resolution waitable
// timer on Windows, where the default sleep granularity is ~15.6 ms).
void sleep_until(Clock::time_point tp) {
  Clock::time_point now = Clock::now();
  if (tp <= now) return;
#ifdef _WIN32
  thread_local WinTimer timer;
  int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(tp - now).count();
  if (timer.h) {
    LARGE_INTEGER due;
    due.QuadPart = -std::max<int64_t>(1, ns / 100);  // relative, 100 ns units
    if (SetWaitableTimer(timer.h, &due, 0, nullptr, nullptr, FALSE)) {
      WaitForSingleObject(timer.h, INFINITE);
      return;
    }
  }
  Sleep((DWORD)std::max<int64_t>(1, ns / 1000000));
#else
  std::this_thread::sleep_until(tp);
#endif
}

// ===========================================================================
// Latency histogram (log-linear; must match the Go implementation)
// ===========================================================================

int msb64(uint64_t v) {  // v != 0
#ifdef _WIN32
  unsigned long idx;
  _BitScanReverse64(&idx, v);
  return (int)idx;
#else
  return 63 - __builtin_clzll(v);
#endif
}

struct Hist {
  static const int kBuckets = 128 + 57 * 128;  // msb 7..63
  std::vector<uint64_t> b;
  uint64_t count = 0, sum = 0, mn = UINT64_MAX, mx = 0;
  Hist() : b(kBuckets, 0) {}
  static int index(uint64_t v) {
    if (v < 128) return (int)v;
    int msb = msb64(v);
    int sh = msb - 7;
    uint64_t sub = (v >> sh) - 128;
    return 128 + sh * 128 + (int)sub;
  }
  static void range(int idx, uint64_t& low, uint64_t& width) {
    if (idx < 128) {
      low = (uint64_t)idx;
      width = 1;
      return;
    }
    int sh = (idx - 128) / 128;
    uint64_t sub = (uint64_t)((idx - 128) % 128);
    low = (128 + sub) << sh;
    width = 1ull << sh;
  }
  void add(uint64_t v) {
    ++b[index(v)];
    ++count;
    sum += v;
    mn = std::min(mn, v);
    mx = std::max(mx, v);
  }
  void reset() {
    std::fill(b.begin(), b.end(), 0);
    count = sum = mx = 0;
    mn = UINT64_MAX;
  }
  uint64_t quantile(double q) const {
    double r = std::ceil(q * (double)count);
    uint64_t rank = r < 1.0 ? 1 : (uint64_t)r;
    if (rank > count) rank = count;
    uint64_t cum = 0;
    for (int i = 0; i < kBuckets; ++i) {
      cum += b[i];
      if (cum >= rank) {
        uint64_t low, width;
        range(i, low, width);
        uint64_t v = low + (width - 1) / 2;
        return std::min(std::max(v, mn), mx);
      }
    }
    return mx;
  }
};

void write_latency(JW& j, const Hist& h) {
  if (h.count == 0) {
    j.null();
    return;
  }
  j.o();
  j.k("count").u(h.count);
  j.k("min_ns").u(h.mn);
  j.k("mean_ns").u((uint64_t)std::llround((double)h.sum / (double)h.count));
  j.k("p50_ns").u(h.quantile(0.5));
  j.k("p99_ns").u(h.quantile(0.99));
  j.k("p999_ns").u(h.quantile(0.999));
  j.k("max_ns").u(h.mx);
  j.oe();
}

// ===========================================================================
// Host information
// ===========================================================================

struct HostInfo {
  std::string hostname;
  std::optional<std::string> cpu_model;
  uint64_t logical_cpus = 0;
};

std::string trim(const std::string& s) {
  size_t a = s.find_first_not_of(" \t\r\n");
  if (a == std::string::npos) return "";
  size_t e = s.find_last_not_of(" \t\r\n");
  return s.substr(a, e - a + 1);
}

#ifdef _WIN32
std::string utf8(const wchar_t* w) {
  int n = WideCharToMultiByte(CP_UTF8, 0, w, -1, nullptr, 0, nullptr, nullptr);
  if (n <= 1) return "";
  std::string s((size_t)n, '\0');
  WideCharToMultiByte(CP_UTF8, 0, w, -1, &s[0], n, nullptr, nullptr);
  s.resize((size_t)n - 1);
  return s;
}
#endif

HostInfo host_info() {
  HostInfo h;
#ifdef _WIN32
  wchar_t name[256];
  DWORD len = 256;
  if (GetComputerNameExW(ComputerNameDnsHostname, name, &len)) h.hostname = utf8(name);
  wchar_t cpu[256];
  DWORD cb = sizeof cpu;
  if (RegGetValueW(HKEY_LOCAL_MACHINE, L"HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0",
                   L"ProcessorNameString", RRF_RT_REG_SZ, nullptr, cpu, &cb) == ERROR_SUCCESS) {
    std::string s = trim(utf8(cpu));
    if (!s.empty()) h.cpu_model = s;
  }
  h.logical_cpus = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
#else
  char name[256] = {0};
  if (gethostname(name, sizeof name - 1) == 0) h.hostname = name;
  if (FILE* f = std::fopen("/proc/cpuinfo", "r")) {
    char line[1024];
    while (std::fgets(line, sizeof line, f)) {
      if (std::strncmp(line, "model name", 10) == 0) {
        const char* c = std::strchr(line, ':');
        if (c) {
          std::string s = trim(c + 1);
          if (!s.empty()) h.cpu_model = s;
        }
        break;
      }
    }
    std::fclose(f);
  }
  long n = sysconf(_SC_NPROCESSORS_ONLN);
  h.logical_cpus = n > 0 ? (uint64_t)n : 0;
#endif
  if (h.logical_cpus == 0) h.logical_cpus = std::thread::hardware_concurrency();
  return h;
}

const char* host_os() {
#ifdef _WIN32
  return "windows";
#else
  return "linux";
#endif
}

const char* host_arch() {
#if defined(_M_X64) || defined(__x86_64__)
  return "amd64";
#elif defined(_M_ARM64) || defined(__aarch64__)
  return "arm64";
#else
  return "unknown";
#endif
}

void write_host(JW& j, const HostInfo& h) {
  j.o();
  j.k("hostname").str(h.hostname);
  j.k("os").str(host_os());
  j.k("arch").str(host_arch());
  j.k("cpu_model").ostr(h.cpu_model);
  j.k("logical_cpus").u(h.logical_cpus);
  j.oe();
}

std::string random_run_id() {
  uint64_t v = 0;
  try {
    std::random_device rd;
    v = ((uint64_t)rd() << 32) ^ (uint64_t)rd();
  } catch (...) {
  }
  // Mix in time so a degenerate random_device still yields distinct ids.
  uint64_t z = v ^ (uint64_t)Clock::now().time_since_epoch().count() ^
               ((uint64_t)std::time(nullptr) << 20);
  z += 0x9E3779B97F4A7C15ull;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
  z ^= z >> 31;
  char b[24];
  std::snprintf(b, sizeof b, "%016llx", (unsigned long long)z);
  return b;
}

// ===========================================================================
// NVML (dynamically loaded)
// ===========================================================================

typedef nvmlReturn_t (*PFN_nvmlInit_v2)(void);
typedef nvmlReturn_t (*PFN_nvmlShutdown)(void);
typedef const char* (*PFN_nvmlErrorString)(nvmlReturn_t);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetHandleByUUID)(const char*, nvmlDevice_t*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetMemoryInfo)(nvmlDevice_t, nvmlMemory_t*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetPowerUsage)(nvmlDevice_t, unsigned int*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetClockInfo)(nvmlDevice_t, nvmlClockType_t, unsigned int*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetTemperature)(nvmlDevice_t, nvmlTemperatureSensors_t,
                                                     unsigned int*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetPerformanceState)(nvmlDevice_t, nvmlPstates_t*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetClocksReasons)(nvmlDevice_t, unsigned long long*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetUtilizationRates)(nvmlDevice_t, nvmlUtilization_t*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetTotalEnergyConsumption)(nvmlDevice_t, unsigned long long*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetEnforcedPowerLimit)(nvmlDevice_t, unsigned int*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetApplicationsClock)(nvmlDevice_t, nvmlClockType_t,
                                                           unsigned int*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetMaxClockInfo)(nvmlDevice_t, nvmlClockType_t,
                                                      unsigned int*);
typedef nvmlReturn_t (*PFN_nvmlDeviceGetVbiosVersion)(nvmlDevice_t, char*, unsigned int);
typedef nvmlReturn_t (*PFN_nvmlSystemGetDriverVersion)(char*, unsigned int);

struct Nvml {
#ifdef _WIN32
  HMODULE lib = nullptr;
#else
  void* lib = nullptr;
#endif
  bool inited = false;
  PFN_nvmlInit_v2 init = nullptr;
  PFN_nvmlShutdown shutdown = nullptr;
  PFN_nvmlErrorString error_string = nullptr;
  PFN_nvmlDeviceGetHandleByUUID handle_by_uuid = nullptr;
  PFN_nvmlDeviceGetMemoryInfo memory_info = nullptr;
  PFN_nvmlDeviceGetPowerUsage power_usage = nullptr;
  PFN_nvmlDeviceGetClockInfo clock_info = nullptr;
  PFN_nvmlDeviceGetTemperature temperature = nullptr;
  PFN_nvmlDeviceGetPerformanceState pstate = nullptr;
  PFN_nvmlDeviceGetClocksReasons clocks_reasons = nullptr;
  PFN_nvmlDeviceGetUtilizationRates utilization = nullptr;
  PFN_nvmlDeviceGetTotalEnergyConsumption energy = nullptr;
  PFN_nvmlDeviceGetEnforcedPowerLimit power_limit = nullptr;
  PFN_nvmlDeviceGetApplicationsClock app_clock = nullptr;
  PFN_nvmlDeviceGetMaxClockInfo max_clock = nullptr;
  PFN_nvmlDeviceGetVbiosVersion vbios = nullptr;
  PFN_nvmlSystemGetDriverVersion driver_version = nullptr;

  void* raw(const char* name) {
#ifdef _WIN32
    return reinterpret_cast<void*>(GetProcAddress(lib, name));
#else
    return dlsym(lib, name);
#endif
  }
  template <class F>
  void sym(F& f, const char* name) {
    f = reinterpret_cast<F>(raw(name));
  }

  // Loads the library and initializes NVML. On failure returns false with a
  // reason; NVML is then simply unavailable.
  bool load(std::string& why) {
#ifdef _WIN32
    lib = LoadLibraryExW(L"nvml.dll", nullptr, LOAD_LIBRARY_SEARCH_SYSTEM32);
    if (!lib) {
      why = "nvml.dll not found in System32";
      return false;
    }
#else
    lib = dlopen("libnvidia-ml.so.1", RTLD_NOW | RTLD_LOCAL);
    if (!lib) {
      why = "libnvidia-ml.so.1 not found";
      return false;
    }
#endif
    sym(init, "nvmlInit_v2");
    sym(shutdown, "nvmlShutdown");
    sym(error_string, "nvmlErrorString");
    sym(handle_by_uuid, "nvmlDeviceGetHandleByUUID");
    sym(memory_info, "nvmlDeviceGetMemoryInfo");
    sym(power_usage, "nvmlDeviceGetPowerUsage");
    sym(clock_info, "nvmlDeviceGetClockInfo");
    sym(temperature, "nvmlDeviceGetTemperature");
    sym(pstate, "nvmlDeviceGetPerformanceState");
    sym(clocks_reasons, "nvmlDeviceGetCurrentClocksEventReasons");
    if (!clocks_reasons) sym(clocks_reasons, "nvmlDeviceGetCurrentClocksThrottleReasons");
    sym(utilization, "nvmlDeviceGetUtilizationRates");
    sym(energy, "nvmlDeviceGetTotalEnergyConsumption");
    sym(power_limit, "nvmlDeviceGetEnforcedPowerLimit");
    sym(app_clock, "nvmlDeviceGetApplicationsClock");
    sym(max_clock, "nvmlDeviceGetMaxClockInfo");
    sym(vbios, "nvmlDeviceGetVbiosVersion");
    sym(driver_version, "nvmlSystemGetDriverVersion");
    if (!init || !shutdown || !handle_by_uuid) {
      why = "NVML library lacks nvmlInit_v2/nvmlShutdown/nvmlDeviceGetHandleByUUID";
      unload();
      return false;
    }
    nvmlReturn_t r = init();
    if (r != NVML_SUCCESS) {
      why = std::string("nvmlInit_v2 failed: ") + (error_string ? error_string(r) : "?");
      unload();
      return false;
    }
    inited = true;
    return true;
  }

  void unload() {
    if (inited && shutdown) shutdown();
    inited = false;
    if (lib) {
#ifdef _WIN32
      FreeLibrary(lib);
#else
      dlclose(lib);
#endif
    }
    lib = nullptr;
  }
};

struct FieldStat {
  uint64_t n = 0, mn = UINT64_MAX, mx = 0;
  double sum = 0;
  void add(uint64_t v) {
    ++n;
    sum += (double)v;
    mn = std::min(mn, v);
    mx = std::max(mx, v);
  }
};

struct NvmlAcc {
  uint64_t samples = 0;
  FieldStat power, sm, mem, temp, util_gpu, util_mem, pstate;
  bool reasons_valid = false;
  uint64_t reasons = 0;
};

void write_stat(JW& j, const FieldStat& s) {
  if (s.n == 0) {
    j.null();
    return;
  }
  j.o().k("min").u(s.mn).k("mean").num3(s.sum / (double)s.n).k("max").u(s.mx).oe();
}

class Sampler {
 public:
  void start(Nvml* nv, nvmlDevice_t dev, int64_t period_ms, Clock::time_point origin) {
    nv_ = nv;
    dev_ = dev;
    period_ = std::chrono::milliseconds(period_ms);
    origin_ = origin;
    stop_.store(false);
    th_ = std::thread([this] { loop(); });
    running_ = true;
  }
  NvmlAcc take() {
    std::lock_guard<std::mutex> g(mu_);
    NvmlAcc out = acc_;
    acc_ = NvmlAcc();
    return out;
  }
  void finish() {
    if (!running_) return;
    stop_.store(true);
    th_.join();
    running_ = false;
  }
  bool running() const { return running_; }

 private:
  void sample() {
    NvmlAcc s;
    unsigned int v = 0;
    if (nv_->power_usage && nv_->power_usage(dev_, &v) == NVML_SUCCESS) s.power.add(v);
    if (nv_->clock_info && nv_->clock_info(dev_, NVML_CLOCK_SM, &v) == NVML_SUCCESS) s.sm.add(v);
    if (nv_->clock_info && nv_->clock_info(dev_, NVML_CLOCK_MEM, &v) == NVML_SUCCESS)
      s.mem.add(v);
    if (nv_->temperature && nv_->temperature(dev_, NVML_TEMPERATURE_GPU, &v) == NVML_SUCCESS)
      s.temp.add(v);
    nvmlPstates_t ps;
    if (nv_->pstate && nv_->pstate(dev_, &ps) == NVML_SUCCESS && ps != NVML_PSTATE_UNKNOWN)
      s.pstate.add((uint64_t)ps);
    unsigned long long r = 0;
    if (nv_->clocks_reasons && nv_->clocks_reasons(dev_, &r) == NVML_SUCCESS) {
      s.reasons_valid = true;
      s.reasons = r;
    }
    nvmlUtilization_t u;
    if (nv_->utilization && nv_->utilization(dev_, &u) == NVML_SUCCESS) {
      s.util_gpu.add(u.gpu);
      s.util_mem.add(u.memory);
    }
    std::lock_guard<std::mutex> g(mu_);
    ++acc_.samples;
    merge(acc_.power, s.power);
    merge(acc_.sm, s.sm);
    merge(acc_.mem, s.mem);
    merge(acc_.temp, s.temp);
    merge(acc_.pstate, s.pstate);
    merge(acc_.util_gpu, s.util_gpu);
    merge(acc_.util_mem, s.util_mem);
    if (s.reasons_valid) {
      acc_.reasons_valid = true;
      acc_.reasons |= s.reasons;
    }
  }
  static void merge(FieldStat& a, const FieldStat& b) {
    if (b.n == 0) return;
    a.n += b.n;
    a.sum += b.sum;
    a.mn = std::min(a.mn, b.mn);
    a.mx = std::max(a.mx, b.mx);
  }
  void loop() {
    Clock::time_point next = origin_;
    while (!stop_.load()) {
      sample();
      Clock::time_point now = Clock::now();
      next += period_;
      while (next <= now) next += period_;  // skip missed ticks, stay on grid
      while (!stop_.load()) {
        now = Clock::now();
        if (now >= next) break;
        sleep_until(std::min(next, now + std::chrono::milliseconds(50)));
      }
    }
  }
  Nvml* nv_ = nullptr;
  nvmlDevice_t dev_ = nullptr;
  Clock::duration period_{};
  Clock::time_point origin_{};
  std::thread th_;
  std::mutex mu_;
  NvmlAcc acc_;
  std::atomic<bool> stop_{false};
  bool running_ = false;
};

}  // namespace

// ===========================================================================
// Device code (named namespace: kernels in an anonymous namespace make the
// nvcc-generated host stubs trip MSVC C4211)
// ===========================================================================

namespace kbdev {

__host__ __device__ inline uint32_t kb_hash(uint64_t seed, uint64_t i) {
  uint64_t z = seed * 0x9E3779B97F4A7C15ull + i + 0x632BE59BD9B4E019ull;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
  z ^= z >> 31;
  return (uint32_t)(z >> 32);
}

// Values in [-1, 1): 24 high bits of the hash scaled.
__global__ void fill_bf16_kernel(__nv_bfloat16* p, uint64_t count, uint64_t seed) {
  uint64_t stride = (uint64_t)gridDim.x * blockDim.x;
  for (uint64_t i = (uint64_t)blockIdx.x * blockDim.x + threadIdx.x; i < count; i += stride) {
    float v = (float)(kb_hash(seed, i) >> 8) * (2.0f / 16777216.0f) - 1.0f;
    p[i] = __float2bfloat16_rn(v);
  }
}

__global__ void fill_u32_kernel(uint32_t* p, uint64_t count, uint64_t seed) {
  uint64_t stride = (uint64_t)gridDim.x * blockDim.x;
  for (uint64_t i = (uint64_t)blockIdx.x * blockDim.x + threadIdx.x; i < count; i += stride)
    p[i] = kb_hash(seed, i);
}

}  // namespace kbdev

namespace {

using kbdev::fill_bf16_kernel;
using kbdev::fill_u32_kernel;
using kbdev::kb_hash;

unsigned grid_for(uint64_t count) {
  uint64_t blocks = (count + 255) / 256;
  return (unsigned)std::max<uint64_t>(1, std::min<uint64_t>(blocks, 8192));
}

// ===========================================================================
// The benchmark run (all NVML / CUDA / cuBLAS use is below this line)
// ===========================================================================

struct DeviceInfo {
  bool known = false;
  std::string uuid, name, pci_bus_id;
  int cc_major = 0, cc_minor = 0, sm_count = 0;
  uint64_t total = 0;
  std::optional<uint64_t> free_bytes;
  std::optional<std::string> driver_version, vbios;
  std::optional<int> cuda_driver, cuda_runtime, cublas;
  bool nvml_available = false;
  std::optional<unsigned> power_limit_mw, app_sm, app_mem, max_sm, max_mem;
};

void write_device(JW& j, const DeviceInfo& d) {
  j.o();
  j.k("kind").str("gpu");
  j.k("uuid").str(d.uuid);
  j.k("name").str(d.name);
  j.k("pci_bus_id").str(d.pci_bus_id);
  j.k("compute_capability").str(std::to_string(d.cc_major) + "." + std::to_string(d.cc_minor));
  j.k("sm_count").i(d.sm_count);
  j.k("memory_total_bytes").u(d.total);
  j.k("memory_free_bytes").ou(d.free_bytes);
  j.k("driver_version").ostr(d.driver_version);
  j.k("cuda_driver_version").oi(d.cuda_driver);
  j.k("cuda_runtime_version").oi(d.cuda_runtime);
  j.k("cublas_version").oi(d.cublas);
  j.k("nvml_available").b(d.nvml_available);
  j.k("power_limit_mw").ou(d.power_limit_mw);
  j.k("applications_clock_mhz").o().k("sm").ou(d.app_sm).k("mem").ou(d.app_mem).oe();
  j.k("max_clock_mhz").o().k("sm").ou(d.max_sm).k("mem").ou(d.max_mem).oe();
  j.k("vbios_version").ostr(d.vbios);
  j.oe();
}

std::string format_uuid(const cudaUUID_t& u) {
  const unsigned char* b = reinterpret_cast<const unsigned char*>(u.bytes);
  char s[64];
  std::snprintf(s, sizeof s,
                "GPU-%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", b[0],
                b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
                b[14], b[15]);
  return s;
}

std::string mib_str(uint64_t bytes) {
  return std::to_string((bytes + MiB - 1) / MiB) + " MiB";
}

class Bench {
 public:
  explicit Bench(const Params& p) : p_(p) {}
  ~Bench() { release(); }
  int run();

 private:
  enum class Reason { Duration, Signal, Error, Refused };

  // setup
  bool setup();
  bool select_device();
  bool nvml_setup();
  bool create_context();
  bool allocate_and_fill();
  bool calibrate();
  // timed
  bool loop_batched();
  bool loop_latency();
  bool issue_op();
  bool issue_batch(int slot);
  void close_to(int64_t t);
  void final_interval(int64_t t);
  void emit_interval(int64_t begin, int64_t end);
  void emit_start();
  void emit_end();
  void release();

  bool fail(Reason r, const std::string& msg) {
    reason_ = r;
    error_ = msg;
    return false;
  }
  bool cuda_fail(cudaError_t e, const char* what) {
    return fail(Reason::Error, std::string(what) + ": " + cudaGetErrorName(e) + " (" +
                                   cudaGetErrorString(e) + ")");
  }
  bool blas_fail(cublasStatus_t s, const char* what) {
    return fail(Reason::Error, std::string(what) + ": " + cublasGetStatusName(s) + " (" +
                                   cublasGetStatusString(s) + ")");
  }
  bool stop_requested() {
    if (g_stop.load()) {
      reason_ = Reason::Signal;
      return true;
    }
    return false;
  }
  bool dalloc(void** ptr, uint64_t bytes, const char* what) {
    cudaError_t e = cudaMalloc(ptr, (size_t)bytes);
    if (e != cudaSuccess) return cuda_fail(e, what);
    allocated_ += bytes;
    return true;
  }
  int64_t mono(Clock::time_point t) const {
    if (!started_) return 0;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(t - origin_).count();
  }
  Clock::time_point at(int64_t ns) const { return origin_ + std::chrono::nanoseconds(ns); }
  std::optional<uint64_t> read_energy() {
    unsigned long long e = 0;
    if (nvml_dev_ && nvml_.energy && nvml_.energy(nvml_dev_, &e) == NVML_SUCCESS)
      return (uint64_t)e;
    return std::nullopt;
  }
  // mono_ns is taken from `now` (the start record passes the origin itself).
  void header(JW& j, const char* type, Clock::time_point now = Clock::now()) {
    j.k("schema").str(kSchema);
    j.k("type").str(type);
    j.k("run_id").str(run_id_);
    j.k("program").str(kProgram);
    j.k("workload").str(workload_name(p_.wl));
    j.k("wall").str(wall_now());
    j.k("mono_ns").i(mono(now));
  }

  const Params& p_;
  std::string run_id_;
  HostInfo host_;
  DeviceInfo dev_;
  Reason reason_ = Reason::Duration;
  std::string error_;

  Nvml nvml_;
  nvmlDevice_t nvml_dev_ = nullptr;
  Sampler sampler_;

  int cuda_dev_ = -1;
  bool ctx_ = false;
  bool spin_ = false;
  cudaStream_t stream_ = nullptr;
  cublasHandle_t blas_ = nullptr;
  void* buf_a_ = nullptr;
  void* buf_b_ = nullptr;
  void* buf_c_ = nullptr;
  void* ws_ = nullptr;
  void* host_buf_ = nullptr;
  cudaEvent_t ev_start_[2] = {nullptr, nullptr};
  cudaEvent_t ev_end_[2] = {nullptr, nullptr};
  uint64_t slot_ops_[2] = {0, 0};
  bool inflight_[2] = {false, false};
  uint64_t allocated_ = 0;
  uint64_t batch_ops_ = 1;
  uint64_t warmup_ops_ = 0;

  // timed run state
  bool started_ = false;
  Clock::time_point origin_{};
  int64_t interval_ns_ = 0;
  int64_t deadline_ns_ = -1;
  int64_t prev_boundary_ = 0;
  int64_t next_boundary_ = 0;
  int64_t boundary_k_ = 0;
  int64_t window_end_ = 0;
  uint64_t intervals_ = 0;
  uint64_t cur_ops_ = 0, cur_busy_ = 0;
  uint64_t tot_ops_ = 0, tot_busy_ = 0;
  Hist cur_hist_, run_hist_;
  std::optional<uint64_t> energy_start_, energy_last_;
};

int Bench::run() {
  run_id_ = random_run_id();
  host_ = host_info();
  bool ok = setup();
  if (ok && !stop_requested()) {
    emit_start();
    ok = p_.wl == Workload::Pcie && p_.latency ? loop_latency() : loop_batched();
  } else if (ok) {
    reason_ = Reason::Signal;  // stopped during setup
  }
  if (ok && g_output_failed.load()) ok = fail(Reason::Error, "writing to stdout failed");
  sampler_.finish();
  // Drain whatever is still in flight (bounded: at most two short batches).
  if (stream_) {
    cudaError_t e = cudaStreamSynchronize(stream_);
    if (e != cudaSuccess && reason_ != Reason::Error && reason_ != Reason::Refused)
      cuda_fail(e, "cudaStreamSynchronize (drain)");
  }
  emit_end();
  release();
  switch (reason_) {
    case Reason::Duration:
    case Reason::Signal: return 0;
    default:
      std::fprintf(stderr, "%s: %s: %s\n", kProgram,
                   reason_ == Reason::Refused ? "refused" : "error", error_.c_str());
      std::fflush(stderr);
      return 1;
  }
}

bool Bench::setup() {
  if (stop_requested()) return true;
  if (!select_device()) return false;
  if (stop_requested()) return true;
  if (!nvml_setup()) return false;
  if (stop_requested()) return true;
  if (!create_context()) return false;
  if (stop_requested()) return true;
  if (!allocate_and_fill()) return false;
  if (stop_requested()) return true;
  return calibrate();
}

// Enumerates CUDA devices and matches --device by UUID. This initializes the
// driver but creates no context. FIRST CUDA RUNTIME CALL OF THE PROGRAM.
bool Bench::select_device() {
  int rt = 0;
  if (cudaRuntimeGetVersion(&rt) == cudaSuccess) dev_.cuda_runtime = rt;
  int count = 0;
  cudaError_t e = cudaGetDeviceCount(&count);
  if (e == cudaErrorNoDevice || (e == cudaSuccess && count <= 0))
    return fail(Reason::Refused, "no CUDA devices visible; wanted " + p_.device);
  if (e != cudaSuccess) return cuda_fail(e, "cudaGetDeviceCount");
  int dv = 0;
  if (cudaDriverGetVersion(&dv) == cudaSuccess) dev_.cuda_driver = dv;
  std::string visible;
  for (int i = 0; i < count; ++i) {
    cudaDeviceProp prop;
    e = cudaGetDeviceProperties(&prop, i);
    if (e != cudaSuccess) return cuda_fail(e, "cudaGetDeviceProperties");
    std::string uuid = format_uuid(prop.uuid);
    if (!visible.empty()) visible += ", ";
    visible += uuid;
    if (uuid != p_.device) continue;
    cuda_dev_ = i;
    dev_.known = true;
    dev_.uuid = uuid;
    dev_.name = prop.name;
    dev_.cc_major = prop.major;
    dev_.cc_minor = prop.minor;
    dev_.sm_count = prop.multiProcessorCount;
    dev_.total = (uint64_t)prop.totalGlobalMem;
    char bus[64] = {0};
    if (cudaDeviceGetPCIBusId(bus, (int)sizeof bus, i) == cudaSuccess) dev_.pci_bus_id = bus;
  }
  if (cuda_dev_ < 0)
    return fail(Reason::Refused,
                "device " + p_.device + " not found; visible CUDA devices: " + visible);
  return true;
}

// Loads NVML, reads static device facts and checks free memory before any
// context exists.
bool Bench::nvml_setup() {
  std::string why;
  if (!nvml_.load(why)) return true;  // NVML optional
  nvmlDevice_t d = nullptr;
  if (nvml_.handle_by_uuid(p_.device.c_str(), &d) != NVML_SUCCESS) {
    nvml_.unload();
    return true;
  }
  nvml_dev_ = d;
  dev_.nvml_available = true;
  char buf[256];
  if (nvml_.driver_version &&
      nvml_.driver_version(buf, (unsigned)sizeof buf) == NVML_SUCCESS)
    dev_.driver_version = std::string(buf);
  if (nvml_.vbios && nvml_.vbios(d, buf, (unsigned)sizeof buf) == NVML_SUCCESS)
    dev_.vbios = std::string(buf);
  unsigned v = 0;
  if (nvml_.power_limit && nvml_.power_limit(d, &v) == NVML_SUCCESS) dev_.power_limit_mw = v;
  if (nvml_.app_clock && nvml_.app_clock(d, NVML_CLOCK_SM, &v) == NVML_SUCCESS) dev_.app_sm = v;
  if (nvml_.app_clock && nvml_.app_clock(d, NVML_CLOCK_MEM, &v) == NVML_SUCCESS) dev_.app_mem = v;
  if (nvml_.max_clock && nvml_.max_clock(d, NVML_CLOCK_SM, &v) == NVML_SUCCESS) dev_.max_sm = v;
  if (nvml_.max_clock && nvml_.max_clock(d, NVML_CLOCK_MEM, &v) == NVML_SUCCESS) dev_.max_mem = v;

  nvmlMemory_t mem;
  if (nvml_.memory_info && nvml_.memory_info(d, &mem) == NVML_SUCCESS) {
    uint64_t need = need_bytes(p_);
    uint64_t reserve = p_.reserve_mib * MiB;
    if (need + reserve + kCtxOverhead > mem.free)
      return fail(Reason::Refused, "insufficient free device memory (NVML, before context): need " +
                                       mib_str(need) + " + reserve " + mib_str(reserve) +
                                       " + context estimate " + mib_str(kCtxOverhead) +
                                       " > free " + std::to_string(mem.free / MiB) + " MiB");
  }
  return true;
}

bool Bench::create_context() {
  spin_ = p_.wl == Workload::Pcie && p_.latency;
  unsigned flags = spin_ ? cudaDeviceScheduleSpin : cudaDeviceScheduleBlockingSync;
  // cudaInitDevice sets the flags of the target device's primary context
  // without touching any other device (cudaSetDeviceFlags would act on the
  // current device, which defaults to device 0).
  cudaError_t e = cudaInitDevice(cuda_dev_, flags, 0);
  if (e != cudaSuccess) return cuda_fail(e, "cudaInitDevice");
  ctx_ = true;
  e = cudaSetDevice(cuda_dev_);
  if (e != cudaSuccess) return cuda_fail(e, "cudaSetDevice");
  unsigned got = 0;
  if (cudaGetDeviceFlags(&got) == cudaSuccess) spin_ = (got & cudaDeviceScheduleMask) == cudaDeviceScheduleSpin;

  size_t free_b = 0, total_b = 0;
  e = cudaMemGetInfo(&free_b, &total_b);
  if (e != cudaSuccess) return cuda_fail(e, "cudaMemGetInfo");
  dev_.free_bytes = (uint64_t)free_b;
  uint64_t need = need_bytes(p_);
  uint64_t reserve = p_.reserve_mib * MiB;
  if (need + reserve > (uint64_t)free_b)
    return fail(Reason::Refused, "insufficient free device memory (cudaMemGetInfo): need " +
                                     mib_str(need) + " + reserve " + mib_str(reserve) +
                                     " > free " + std::to_string((uint64_t)free_b / MiB) + " MiB");
  e = cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking);
  if (e != cudaSuccess) return cuda_fail(e, "cudaStreamCreateWithFlags");
  for (int s = 0; s < 2; ++s) {
    if ((e = cudaEventCreate(&ev_start_[s])) != cudaSuccess) return cuda_fail(e, "cudaEventCreate");
    if ((e = cudaEventCreate(&ev_end_[s])) != cudaSuccess) return cuda_fail(e, "cudaEventCreate");
  }
  return true;
}

bool Bench::allocate_and_fill() {
  cudaError_t e;
  const uint64_t seed_a = 0x4b4149524f2d41ull, seed_b = 0x4b4149524f2d42ull,
                 seed_c = 0x4b4149524f2d43ull;
  auto fill_bf16 = [&](void* p, uint64_t elems, uint64_t seed) -> bool {
    fill_bf16_kernel<<<grid_for(elems), 256, 0, stream_>>>(static_cast<__nv_bfloat16*>(p),
                                                          elems, seed);
    cudaError_t le = cudaGetLastError();
    if (le != cudaSuccess) return cuda_fail(le, "fill kernel launch");
    return true;
  };
  auto fill_u32 = [&](void* p, uint64_t bytes, uint64_t seed) -> bool {
    uint64_t words = bytes / 4;
    fill_u32_kernel<<<grid_for(words), 256, 0, stream_>>>(static_cast<uint32_t*>(p), words,
                                                         seed);
    cudaError_t le = cudaGetLastError();
    if (le != cudaSuccess) return cuda_fail(le, "fill kernel launch");
    return true;
  };

  if (uses_cublas(p_)) {
#ifdef _WIN32
    // cuBLAS is delay-loaded: load it explicitly so a missing DLL is a clean
    // error instead of a delay-load exception.
    const char* dll = "cublas64_" KB_STR(CUBLAS_VER_MAJOR) ".dll";
    if (!LoadLibraryA(dll))
      return fail(Reason::Error, std::string("cannot load ") + dll +
                                     " (needs it and cublasLt64_" KB_STR(CUBLAS_VER_MAJOR)
                                     ".dll on PATH or next to the executable)");
#endif
    cublasStatus_t s = cublasCreate(&blas_);
    if (s != CUBLAS_STATUS_SUCCESS) return blas_fail(s, "cublasCreate");
    int ver = 0;
    if (cublasGetVersion(blas_, &ver) == CUBLAS_STATUS_SUCCESS) dev_.cublas = ver;
    if ((s = cublasSetStream(blas_, stream_)) != CUBLAS_STATUS_SUCCESS)
      return blas_fail(s, "cublasSetStream");
    if (!dalloc(&ws_, kWorkspaceBytes, "cudaMalloc workspace")) return false;
    if ((s = cublasSetWorkspace(blas_, ws_, (size_t)kWorkspaceBytes)) != CUBLAS_STATUS_SUCCESS)
      return blas_fail(s, "cublasSetWorkspace");
    if ((s = cublasSetPointerMode(blas_, CUBLAS_POINTER_MODE_HOST)) != CUBLAS_STATUS_SUCCESS)
      return blas_fail(s, "cublasSetPointerMode");
  }

  switch (p_.wl) {
    case Workload::Gemm: {
      uint64_t elems = p_.n * p_.n;
      if (!dalloc(&buf_a_, elems * 2, "cudaMalloc A") || !dalloc(&buf_b_, elems * 2, "cudaMalloc B") ||
          !dalloc(&buf_c_, elems * 2, "cudaMalloc C"))
        return false;
      if (!fill_bf16(buf_a_, elems, seed_a) || !fill_bf16(buf_b_, elems, seed_b) ||
          !fill_bf16(buf_c_, elems, seed_c))
        return false;
      break;
    }
    case Workload::Gemv: {
      uint64_t a = p_.n * p_.n, x = p_.n * p_.batch;
      if (!dalloc(&buf_a_, a * 2, "cudaMalloc A") || !dalloc(&buf_b_, x * 2, "cudaMalloc X") ||
          !dalloc(&buf_c_, x * 2, "cudaMalloc Y"))
        return false;
      if (!fill_bf16(buf_a_, a, seed_a) || !fill_bf16(buf_b_, x, seed_b) ||
          !fill_bf16(buf_c_, x, seed_c))
        return false;
      break;
    }
    case Workload::Copy: {
      uint64_t half = p_.bytes / 2;
      if (!dalloc(&buf_a_, half, "cudaMalloc src") || !dalloc(&buf_b_, half, "cudaMalloc dst"))
        return false;
      if (!fill_u32(buf_a_, half, seed_a) || !fill_u32(buf_b_, half, seed_b)) return false;
      break;
    }
    case Workload::Pcie: {
      uint64_t t = p_.transfer_bytes;
      if (!dalloc(&buf_a_, t, "cudaMalloc device buffer")) return false;
      if (!fill_u32(buf_a_, t, seed_b)) return false;
      e = cudaHostAlloc(&host_buf_, (size_t)t, cudaHostAllocDefault);
      if (e != cudaSuccess) return cuda_fail(e, "cudaHostAlloc");
      uint32_t* h = static_cast<uint32_t*>(host_buf_);
      for (uint64_t i = 0; i < t / 4; ++i) h[i] = kb_hash(seed_a, i);
      break;
    }
  }
  if ((e = cudaStreamSynchronize(stream_)) != cudaSuccess)
    return cuda_fail(e, "cudaStreamSynchronize (fill)");

  // Final guard: the reserve must still be free after our allocations.
  size_t free_b = 0, total_b = 0;
  if ((e = cudaMemGetInfo(&free_b, &total_b)) != cudaSuccess)
    return cuda_fail(e, "cudaMemGetInfo");
  if ((uint64_t)free_b < p_.reserve_mib * MiB)
    return fail(Reason::Refused, "free device memory after allocation " +
                                     std::to_string((uint64_t)free_b / MiB) +
                                     " MiB is below the reserve " +
                                     std::to_string(p_.reserve_mib) + " MiB");
  return true;
}

bool Bench::issue_op() {
  switch (p_.wl) {
    case Workload::Gemm:
    case Workload::Gemv: {
      const float alpha = 1.0f, beta = 0.0f;
      int m = (int)p_.n, k = (int)p_.n;
      int n = p_.wl == Workload::Gemm ? (int)p_.n : (int)p_.batch;
      cublasStatus_t s =
          cublasGemmEx(blas_, CUBLAS_OP_N, CUBLAS_OP_N, m, n, k, &alpha, buf_a_, CUDA_R_16BF, m,
                       buf_b_, CUDA_R_16BF, k, &beta, buf_c_, CUDA_R_16BF, m, CUBLAS_COMPUTE_32F,
                       CUBLAS_GEMM_DEFAULT);
      if (s != CUBLAS_STATUS_SUCCESS) return blas_fail(s, "cublasGemmEx");
      return true;
    }
    case Workload::Copy: {
      cudaError_t e = cudaMemcpyAsync(buf_b_, buf_a_, (size_t)(p_.bytes / 2),
                                      cudaMemcpyDeviceToDevice, stream_);
      if (e != cudaSuccess) return cuda_fail(e, "cudaMemcpyAsync D2D");
      return true;
    }
    case Workload::Pcie: {
      cudaError_t e =
          p_.d2h ? cudaMemcpyAsync(host_buf_, buf_a_, (size_t)p_.transfer_bytes,
                                   cudaMemcpyDeviceToHost, stream_)
                 : cudaMemcpyAsync(buf_a_, host_buf_, (size_t)p_.transfer_bytes,
                                   cudaMemcpyHostToDevice, stream_);
      if (e != cudaSuccess) return cuda_fail(e, "cudaMemcpyAsync");
      return true;
    }
  }
  return false;
}

// One untimed warmup op, then timed calibration runs (also untimed for the
// benchmark) until one measurement spans >= 10 ms; batch_ops targets 20 ms.
bool Bench::calibrate() {
  cudaError_t e;
  if (p_.wl == Workload::Pcie && p_.latency) {
    for (uint64_t i = 0; i < kLatencyWarmupOps; ++i) {
      if (!issue_op()) return false;
      if ((e = cudaStreamSynchronize(stream_)) != cudaSuccess)
        return cuda_fail(e, "cudaStreamSynchronize (warmup)");
      if (stop_requested()) return true;
    }
    warmup_ops_ = kLatencyWarmupOps;
    batch_ops_ = 1;
    return true;
  }
  if (!issue_op()) return false;
  if ((e = cudaStreamSynchronize(stream_)) != cudaSuccess)
    return cuda_fail(e, "cudaStreamSynchronize (warmup)");
  warmup_ops_ = 1;
  uint64_t count = 1;
  for (;;) {
    if (stop_requested()) return true;
    if ((e = cudaEventRecord(ev_start_[0], stream_)) != cudaSuccess)
      return cuda_fail(e, "cudaEventRecord");
    for (uint64_t i = 0; i < count; ++i)
      if (!issue_op()) return false;
    if ((e = cudaEventRecord(ev_end_[0], stream_)) != cudaSuccess)
      return cuda_fail(e, "cudaEventRecord");
    if ((e = cudaEventSynchronize(ev_end_[0])) != cudaSuccess)
      return cuda_fail(e, "cudaEventSynchronize (calibration)");
    float ms = 0;
    if ((e = cudaEventElapsedTime(&ms, ev_start_[0], ev_end_[0])) != cudaSuccess)
      return cuda_fail(e, "cudaEventElapsedTime");
    warmup_ops_ += count;
    if (ms >= kCalibMinMs || count >= kMaxBatchOps) {
      double per_op = ms > 0 ? (double)ms / (double)count : 1e-6;
      double b = std::floor(kBatchTargetMs / per_op + 0.5);
      batch_ops_ = (uint64_t)std::min<double>(std::max<double>(b, 1.0), (double)kMaxBatchOps);
      return true;
    }
    uint64_t next = ms > 0 ? (uint64_t)std::ceil((double)count * kBatchTargetMs / (double)ms)
                           : count * 16;
    next = std::max(next, count * 2);
    next = std::min(next, count * 16);
    count = std::min(next, kMaxBatchOps);
  }
}

void Bench::emit_start() {
  origin_ = Clock::now();
  started_ = true;
  interval_ns_ = p_.interval_ms * 1000000;
  deadline_ns_ = p_.seconds_ms > 0 ? p_.seconds_ms * 1000000 : -1;
  prev_boundary_ = 0;
  boundary_k_ = 0;
  next_boundary_ = interval_ns_;

  JW j;
  j.o();
  header(j, "start", origin_);
  j.k("version").i(workload_version(p_.wl));
  j.k("build").str(kBuild);
  j.k("params");
  write_params(j, p_);
  j.k("units");
  write_units(j, p_);
  j.k("host");
  write_host(j, host_);
  j.k("device");
  write_device(j, dev_);
  j.k("setup").o();
  j.k("sync").str(spin_ ? "spin" : "blocking");
  j.k("batch_ops").u(batch_ops_);
  j.k("warmup_ops").u(warmup_ops_);
  j.k("allocated_bytes").u(allocated_);
  j.oe();
  j.oe();
  emit_line(j.s());

  energy_start_ = read_energy();
  energy_last_ = energy_start_;
  if (nvml_dev_) sampler_.start(&nvml_, nvml_dev_, p_.sample_ms, origin_);
}

void Bench::emit_interval(int64_t begin, int64_t end) {
  NvmlAcc acc;
  std::optional<uint64_t> energy_delta;
  if (nvml_dev_) {
    if (sampler_.running()) acc = sampler_.take();
    std::optional<uint64_t> e = read_energy();
    if (e && energy_last_ && *e >= *energy_last_) energy_delta = *e - *energy_last_;
    energy_last_ = e;
  }
  JW j;
  j.o();
  header(j, "interval");
  j.k("index").u(intervals_);
  j.k("begin_ns").i(begin);
  j.k("end_ns").i(end);
  j.k("work");
  write_work(j, p_, cur_ops_);
  j.k("ops").u(cur_ops_);
  j.k("device_busy_ns").u(cur_busy_);
  j.k("latency");
  if (p_.wl == Workload::Pcie && p_.latency) write_latency(j, cur_hist_);
  else j.null();
  j.k("nvml");
  if (!nvml_dev_) {
    j.null();
  } else {
    j.o();
    j.k("samples").u(acc.samples);
    j.k("energy_mj").ou(energy_delta);
    j.k("power_mw");
    write_stat(j, acc.power);
    j.k("sm_clock_mhz");
    write_stat(j, acc.sm);
    j.k("mem_clock_mhz");
    write_stat(j, acc.mem);
    j.k("temperature_c");
    write_stat(j, acc.temp);
    j.k("util_gpu_pct");
    write_stat(j, acc.util_gpu);
    j.k("util_mem_pct");
    write_stat(j, acc.util_mem);
    j.k("pstate");
    if (acc.pstate.n == 0) j.null();
    else j.o().k("min").u(acc.pstate.mn).k("max").u(acc.pstate.mx).oe();
    j.k("clocks_event_reasons");
    if (acc.reasons_valid) j.u(acc.reasons);
    else j.null();
    j.oe();
  }
  j.oe();
  emit_line(j.s());

  ++intervals_;
  tot_ops_ += cur_ops_;
  tot_busy_ += cur_busy_;
  cur_ops_ = cur_busy_ = 0;
  if (p_.wl == Workload::Pcie && p_.latency) cur_hist_.reset();
  window_end_ = end;
}

// Emits every regular interval whose boundary is <= t (and before the
// deadline, which is handled by final_interval).
void Bench::close_to(int64_t t) {
  while (next_boundary_ <= t && (deadline_ns_ < 0 || next_boundary_ < deadline_ns_)) {
    emit_interval(prev_boundary_, next_boundary_);
    prev_boundary_ = next_boundary_;
    ++boundary_k_;
    next_boundary_ = (boundary_k_ + 1) * interval_ns_;
  }
}

void Bench::final_interval(int64_t t) {
  close_to(t);
  if (t > prev_boundary_) {
    emit_interval(prev_boundary_, t);
    prev_boundary_ = t;
  }
  window_end_ = prev_boundary_;
}

bool Bench::issue_batch(int s) {
  cudaError_t e = cudaEventRecord(ev_start_[s], stream_);
  if (e != cudaSuccess) return cuda_fail(e, "cudaEventRecord");
  for (uint64_t i = 0; i < batch_ops_; ++i)
    if (!issue_op()) return false;
  if ((e = cudaEventRecord(ev_end_[s], stream_)) != cudaSuccess)
    return cuda_fail(e, "cudaEventRecord");
  slot_ops_[s] = batch_ops_;
  inflight_[s] = true;
  (void)cudaStreamQuery(stream_);  // submit now (WDDM batches submissions)
  return true;
}

bool Bench::loop_batched() {
  if (!issue_batch(0) || !issue_batch(1)) return false;
  int oldest = 0;
  for (;;) {
    int64_t t = mono(Clock::now());
    if (g_output_failed.load()) return fail(Reason::Error, "writing to stdout failed");
    if (deadline_ns_ >= 0 && t >= deadline_ns_) {
      final_interval(deadline_ns_);
      reason_ = Reason::Duration;
      return true;
    }
    if (g_stop.load()) {
      final_interval(t);
      reason_ = Reason::Signal;
      return true;
    }
    close_to(t);
    cudaError_t e = cudaEventQuery(ev_end_[oldest]);
    if (e == cudaSuccess) {
      float ms = 0;
      if ((e = cudaEventElapsedTime(&ms, ev_start_[oldest], ev_end_[oldest])) != cudaSuccess)
        return cuda_fail(e, "cudaEventElapsedTime");
      cur_ops_ += slot_ops_[oldest];
      cur_busy_ += (uint64_t)std::llround((double)ms * 1e6);
      inflight_[oldest] = false;
      if (!issue_batch(oldest)) return false;
      oldest ^= 1;
      continue;
    }
    if (e != cudaErrorNotReady) return cuda_fail(e, "cudaEventQuery");
    int64_t wake = std::min(t + kPollNs, next_boundary_);
    if (deadline_ns_ >= 0) wake = std::min(wake, deadline_ns_);
    sleep_until(at(wake));
  }
}

bool Bench::loop_latency() {
  for (;;) {
    int64_t t = mono(Clock::now());
    if (g_output_failed.load()) return fail(Reason::Error, "writing to stdout failed");
    if (deadline_ns_ >= 0 && t >= deadline_ns_) {
      final_interval(deadline_ns_);
      reason_ = Reason::Duration;
      return true;
    }
    if (g_stop.load()) {
      final_interval(t);
      reason_ = Reason::Signal;
      return true;
    }
    close_to(t);
    Clock::time_point t0 = Clock::now();
    if (!issue_op()) return false;
    cudaError_t e = cudaStreamSynchronize(stream_);
    Clock::time_point t1 = Clock::now();
    if (e != cudaSuccess) return cuda_fail(e, "cudaStreamSynchronize");
    int64_t tm = mono(t1);
    if (deadline_ns_ >= 0 && tm >= deadline_ns_) continue;  // after the window
    close_to(tm);
    uint64_t lat = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    cur_hist_.add(lat);
    run_hist_.add(lat);
    ++cur_ops_;
    cur_busy_ += lat;
  }
}

void Bench::emit_end() {
  const char* reason = "error";
  switch (reason_) {
    case Reason::Duration: reason = "duration"; break;
    case Reason::Signal: reason = "signal"; break;
    case Reason::Error: reason = "error"; break;
    case Reason::Refused: reason = "refused"; break;
  }
  JW j;
  j.o();
  header(j, "end");
  j.k("reason").str(reason);
  j.k("error");
  if (reason_ == Reason::Error || reason_ == Reason::Refused) j.str(error_);
  else j.null();
  j.k("elapsed_ns").i(started_ ? window_end_ : 0);
  j.k("intervals").u(intervals_);
  j.k("work");
  write_work(j, p_, tot_ops_);
  j.k("ops").u(tot_ops_);
  j.k("device_busy_ns").u(tot_busy_);
  j.k("latency");
  if (started_ && p_.wl == Workload::Pcie && p_.latency) write_latency(j, run_hist_);
  else j.null();
  j.k("energy_mj");
  // energy_last_ is the reading taken when the last interval was closed, i.e.
  // at the end of the measured window.
  if (started_ && energy_start_ && energy_last_ && *energy_last_ >= *energy_start_)
    j.u(*energy_last_ - *energy_start_);
  else j.null();
  if (!started_) {
    j.k("params");
    write_params(j, p_);
    if (dev_.known) {
      j.k("device");
      write_device(j, dev_);
    }
  }
  j.oe();
  emit_line(j.s());
}

void Bench::release() {
  sampler_.finish();
  if (ctx_) {
    if (stream_) cudaStreamSynchronize(stream_);
    for (int s = 0; s < 2; ++s) {
      if (ev_start_[s]) cudaEventDestroy(ev_start_[s]);
      if (ev_end_[s]) cudaEventDestroy(ev_end_[s]);
      ev_start_[s] = ev_end_[s] = nullptr;
    }
    if (blas_) cublasDestroy(blas_);
    blas_ = nullptr;
    for (void** p : {&buf_a_, &buf_b_, &buf_c_, &ws_}) {
      if (*p) cudaFree(*p);
      *p = nullptr;
    }
    if (host_buf_) cudaFreeHost(host_buf_);
    host_buf_ = nullptr;
    if (stream_) cudaStreamDestroy(stream_);
    stream_ = nullptr;
    ctx_ = false;
  }
  nvml_dev_ = nullptr;
  nvml_.unload();
}

// The only entry point into NVML / CUDA / cuBLAS.
int run_benchmark(const Params& p) {
  int rc;
  {
    Bench b(p);
    rc = b.run();
  }
  signal_done();
  return rc;
}

}  // namespace

int main(int argc, char** argv) {
#ifdef _WIN32
  _setmode(_fileno(stdout), _O_BINARY);  // JSON lines end in "\n", not "\r\n"
#endif
  // ---- Pure C++: parse and validate. No NVML/CUDA/cuBLAS before this block
  // ---- has returned or fallen through to run_benchmark().
  Cli cli;
  std::string err;
  if (!parse_args(argc, argv, cli, err)) {
    std::fprintf(stderr, "%s: %s\nRun '%s --help' for usage.\n", kProgram, err.c_str(), kProgram);
    return 2;
  }
  switch (cli.action) {
    case Action::Help: print_help(); return 0;
    case Action::Version: print_version(); return 0;
    case Action::Validate: print_validated(cli.p); return 0;
    case Action::Run: break;
  }
  // ---- Run time: everything that touches the GPU starts here.
  install_stop_handlers();
  return run_benchmark(cli.p);
}
