package hwbench

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	gpubench "kairo/bench/gpu"
)

// gpuNVCCFlags are bench/gpu/Makefile's (CUBLAS_STATIC=1): sm_80 SASS for
// the A100, sm_86 SASS for Ampere plus compute_86 PTX for newer GPUs;
// cudart and cuBLAS static, NVML opened at run time (the driver's, which
// the container is handed).
var gpuNVCCFlags = []string{"-O3", "-std=c++17",
	"-gencode", "arch=compute_80,code=sm_80", "-gencode", "arch=compute_86,code=sm_86", "-gencode", "arch=compute_86,code=compute_86",
	"-Xcompiler", "-Wall,-Wextra"}

// BuildGPU builds kairo-bench-gpu from the source Kairo carries with the
// container's CUDA toolkit (nvcc; the GPU bench runs in
// store.BenchGPUImage), once per source: the program is kept in dir (the
// project's cache directory on the node), named after the source's digest,
// for the benches after this one. Its build string is that digest.
func BuildGPU(ctx context.Context, dir string, log io.Writer) (string, error) {
	if dir == "" {
		return "", errors.New("no cache directory to keep kairo-bench-gpu in (KAIRO_CACHE_DIR)")
	}
	sum := sha256.Sum256(gpubench.Source)
	digest := hex.EncodeToString(sum[:])[:16]
	dir = filepath.Join(dir, "kairo-bench-gpu")
	program := filepath.Join(dir, "kairo-bench-gpu-"+digest)
	if info, err := os.Stat(program); err == nil && info.Mode().IsRegular() {
		return program, nil
	}
	cuda := os.Getenv("CUDA_HOME")
	if cuda == "" {
		cuda = "/usr/local/cuda"
	}
	nvcc, err := exec.LookPath("nvcc")
	if err != nil {
		nvcc = filepath.Join(cuda, "bin", "nvcc")
		if _, statErr := os.Stat(nvcc); statErr != nil {
			return "", fmt.Errorf("no nvcc (a GPU bench runs in an image with the CUDA toolkit): %w", err)
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	build, err := os.MkdirTemp(dir, "build-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(build)
	source := filepath.Join(build, "kairo-bench-gpu.cu")
	if err := os.WriteFile(source, gpubench.Source, 0o644); err != nil {
		return "", err
	}
	out := filepath.Join(build, "kairo-bench-gpu")
	args := append(append([]string{}, gpuNVCCFlags...), `-DKAIRO_BENCH_BUILD="src-`+digest+`"`, "-o", out, source,
		"-L"+filepath.Join(cuda, "lib64"), "-lcublas_static", "-lcublasLt_static", "-lculibos", "-ldl", "-lpthread", "-lrt")
	cmd := exec.CommandContext(ctx, nvcc, args...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build kairo-bench-gpu: %w", err)
	}
	// in place at once: another bench of the node may build it too
	if err := os.Rename(out, program); err != nil {
		return "", err
	}
	return program, nil
}
