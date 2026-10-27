package devicecontrol

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"kairo/internal/store"
)

func writeFile(path, body string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(body), 0o644)
}

const supportedClocks = `
==============NVSMI LOG==============

Timestamp                                              : Tue Oct 27 17:48:01 2026
Driver Version                                         : 610.88

Attached GPUs                                          : 1
GPU 00000000:01:00.0
    Supported Clocks
        Memory                                         : 9501 MHz
            Graphics                                   : 2130 MHz
            Graphics                                   : 2115 MHz
            Graphics                                   : 210 MHz
        Memory                                         : 405 MHz
            Graphics                                   : 645 MHz
            Graphics                                   : 210 MHz
`

// exitError is a failed command's error with its exit code.
type exitError int

func (e exitError) Error() string { return "exit status " + strconv.Itoa(int(e)) }
func (e exitError) ExitCode() int { return int(e) }

// fakeSMI is an nvidia-smi: queries answer from the test, changes are
// recorded; fail maps a flag (-lmc) to an exit code.
type fakeSMI struct {
	calls []string
	fail  map[string]int
}

func (f *fakeSMI) run(_ context.Context, args ...string) ([]byte, error) {
	switch {
	case slices.Contains(args, "SUPPORTED_CLOCKS"):
		return []byte(supportedClocks), nil
	case strings.HasPrefix(args[len(args)-2], "--query-gpu=power"):
		return []byte("100.00, 350.00, 320.00\n"), nil
	}
	f.calls = append(f.calls, strings.Join(args, " "))
	if code, ok := f.fail[args[2]]; ok {
		return nil, fmt.Errorf("nvidia-smi: %w", exitError(code))
	}
	return nil, nil
}

func TestNVIDIACapabilitiesAreReadFromTheHardware(t *testing.T) {
	smi := &fakeSMI{}
	n := &NVIDIA{Command: smi.run}
	caps, err := n.Capabilities(context.Background(), "GPU-1")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(caps.GraphicsClocksMHz, []int{210, 645, 2115, 2130}) || !slices.Equal(caps.MemoryClocksMHz, []int{405, 9501}) {
		t.Fatalf("clocks: %+v", caps)
	}
	if *caps.PowerLimitMinW != 100 || *caps.PowerLimitMaxW != 350 || *caps.PowerLimitDefaultW != 320 || len(caps.Settings) != 3 {
		t.Fatalf("power: %+v", caps)
	}
}

func TestNVIDIAAppliesAndResetsEachSetting(t *testing.T) {
	smi := &fakeSMI{}
	n := &NVIDIA{Command: smi.run}
	ctx := context.Background()
	clock, limit := 1800, 250
	if err := n.Apply(ctx, "GPU-1", store.DeviceSettings{GraphicsClockMaxMHz: &clock, PowerLimitW: &limit}); err != nil {
		t.Fatal(err)
	}
	want := []string{"-i GPU-1 -lgc 210,1800", "-i GPU-1 -rmc", "-i GPU-1 -pl 250"}
	if !slices.Equal(smi.calls, want) {
		t.Fatalf("apply: %v", smi.calls)
	}
	smi.calls = nil
	// a reset the card does not offer (a GeForce's -rmc) is already at its default
	smi.fail = map[string]int{"-rmc": nvidiaNotSupported}
	if err := n.Apply(ctx, "GPU-1", store.DeviceSettings{}); err != nil {
		t.Fatal(err)
	}
	if want = []string{"-i GPU-1 -rgc", "-i GPU-1 -rmc", "-i GPU-1 -pl 320"}; !slices.Equal(smi.calls, want) {
		t.Fatalf("reset: %v", smi.calls)
	}
	// a change it does not offer fails, and does not keep the others from being made
	smi.calls = nil
	smi.fail = map[string]int{"-lmc": nvidiaNotSupported}
	memory := 9501
	err := n.Apply(ctx, "GPU-1", store.DeviceSettings{MemoryClockMHz: &memory, PowerLimitW: &limit})
	if err == nil || !strings.Contains(err.Error(), "-lmc 9501,9501") || len(smi.calls) != 3 {
		t.Fatalf("unsupported lock: %v %v", err, smi.calls)
	}
}

func TestSysfsUsesIntelPstateWhenPresent(t *testing.T) {
	root := t.TempDir()
	for path, body := range map[string]string{"intel_pstate/max_perf_pct": "100\n", "intel_pstate/min_perf_pct": "24\n"} {
		if err := writeFile(filepath.Join(root, path), body); err != nil {
			t.Fatal(err)
		}
	}
	s := &Sysfs{Root: root}
	caps, err := s.Capabilities(context.Background(), "cpu")
	if err != nil || !slices.Equal(caps.Settings, []string{store.SettingMaxPerf}) || *caps.MaxPerfPctMin != 24 {
		t.Fatalf("capabilities: %+v %v", caps, err)
	}
	pct := 80
	if err = s.Apply(context.Background(), "cpu", store.DeviceSettings{MaxPerfPct: &pct}); err != nil {
		t.Fatal(err)
	}
	if v, _ := readInt(filepath.Join(root, "intel_pstate/max_perf_pct")); v != 80 {
		t.Fatalf("max_perf_pct %d", v)
	}
	if err = s.Apply(context.Background(), "cpu", store.DeviceSettings{}); err != nil {
		t.Fatal(err)
	}
	if v, _ := readInt(filepath.Join(root, "intel_pstate/max_perf_pct")); v != 100 {
		t.Fatalf("default max_perf_pct %d", v)
	}
}

func TestSysfsCpufreqCapsEveryCPU(t *testing.T) {
	root := t.TempDir()
	for cpu, freqs := range map[string][2]int{"cpu0": {400000, 5700000}, "cpu1": {400000, 5200000}} {
		for name, v := range map[string]int{"cpuinfo_min_freq": freqs[0], "cpuinfo_max_freq": freqs[1], "scaling_max_freq": freqs[1]} {
			if err := writeFile(filepath.Join(root, cpu, "cpufreq", name), strconv.Itoa(v)+"\n"); err != nil {
				t.Fatal(err)
			}
		}
	}
	s := &Sysfs{Root: root}
	caps, err := s.Capabilities(context.Background(), "cpu")
	if err != nil || !slices.Equal(caps.Settings, []string{store.SettingMaxFreq}) || *caps.MaxFreqMinMHz != 400 || *caps.MaxFreqMaxMHz != 5700 {
		t.Fatalf("capabilities: %+v %v", caps, err)
	}
	mhz := 5500
	if err = s.Apply(context.Background(), "cpu", store.DeviceSettings{MaxFreqMHz: &mhz}); err != nil {
		t.Fatal(err)
	}
	read := func(cpu string) int {
		v, err := readInt(filepath.Join(root, cpu, "cpufreq", "scaling_max_freq"))
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	// within each CPU's own range
	if read("cpu0") != 5500000 || read("cpu1") != 5200000 {
		t.Fatalf("capped: %d %d", read("cpu0"), read("cpu1"))
	}
	if err = s.Apply(context.Background(), "cpu", store.DeviceSettings{}); err != nil {
		t.Fatal(err)
	}
	if read("cpu0") != 5700000 || read("cpu1") != 5200000 {
		t.Fatalf("defaults: %d %d", read("cpu0"), read("cpu1"))
	}
	if err = s.Apply(context.Background(), "cpu", store.DeviceSettings{MaxPerfPct: &mhz}); err == nil {
		t.Fatal("cpufreq took max_perf_pct")
	}
}

// fakePowercfg is a powercfg holding power schemes: setting -> AC value.
type fakePowercfg struct {
	active   string
	schemes  map[string]map[string]uint32
	calls    []string
	activate int
}

var setValue = regexp.MustCompile(`^/setacvalueindex (\S+) SUB_PROCESSOR (\S+) (\d+)$`)

func (f *fakePowercfg) run(_ context.Context, args ...string) ([]byte, error) {
	line := strings.Join(args, " ")
	switch args[0] {
	case "/getactivescheme":
		return []byte("電源設定の GUID: " + f.active + "  (高パフォーマンス)\r\n"), nil
	case "/qh":
		v, ok := f.schemes[args[1]][args[3]]
		if !ok {
			return nil, exitError(1)
		}
		return fmt.Appendf(nil, "    電源設定の GUID: 75b0ae3f-bce0-45a7-8c89-c9611c25e100  (プロセッサの最大周波数)\r\n      利用可能な設定の最小値: 0x00000000\r\n      利用可能な設定の最大値: 0xffffffff\r\n    現在の AC 電源設定のインデックス: 0x%08x\r\n    現在の DC 電源設定のインデックス: 0x00000000\r\n", v), nil
	case "/setacvalueindex":
		m := setValue.FindStringSubmatch(line)
		v, _ := strconv.ParseUint(m[3], 10, 32)
		f.schemes[m[1]][m[2]] = uint32(v)
	case "/setactive":
		f.activate++
	}
	f.calls = append(f.calls, line)
	return nil, nil
}

const (
	balanced = "381b4222-f694-41f0-9685-ff5bb260df2e"
	highPerf = "8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c"
)

func TestPowercfgKeepsTheUsersValuesAndRestoresThem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kairo-controller.state.json")
	state, err := OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &fakePowercfg{active: highPerf, schemes: map[string]map[string]uint32{
		highPerf: {"PROCFREQMAX": 4800, "PROCFREQMAX1": 0},
		balanced: {"PROCFREQMAX": 0},
	}}
	p := &Powercfg{State: state, Command: cfg.run}
	ctx := context.Background()
	if caps, err := p.Capabilities(ctx, "cpu"); err != nil || !slices.Equal(caps.Settings, []string{store.SettingMaxFreq}) {
		t.Fatalf("capabilities: %+v %v", caps, err)
	}
	mhz := 3000
	if err = p.Apply(ctx, "cpu", store.DeviceSettings{MaxFreqMHz: &mhz}); err != nil {
		t.Fatal(err)
	}
	if cfg.schemes[highPerf]["PROCFREQMAX"] != 3000 || cfg.schemes[highPerf]["PROCFREQMAX1"] != 3000 || cfg.activate != 1 {
		t.Fatalf("capped: %+v", cfg.schemes)
	}
	// a second change keeps the values found before the first
	mhz = 2500
	if err = p.Apply(ctx, "cpu", store.DeviceSettings{MaxFreqMHz: &mhz}); err != nil {
		t.Fatal(err)
	}
	// after a crash the next controller finds them in the state file
	if state, err = OpenState(path); err != nil {
		t.Fatal(err)
	}
	if got := state.ProcessorScheme(highPerf); got["PROCFREQMAX"] != 4800 || got["PROCFREQMAX1"] != 0 {
		t.Fatalf("recorded: %+v", got)
	}
	p = &Powercfg{State: state, Command: cfg.run}
	// the user switched schemes meanwhile: the new one is capped, the old one restored
	cfg.active = balanced
	if err = p.Apply(ctx, "cpu", store.DeviceSettings{MaxFreqMHz: &mhz}); err != nil {
		t.Fatal(err)
	}
	if cfg.schemes[highPerf]["PROCFREQMAX"] != 4800 || cfg.schemes[balanced]["PROCFREQMAX"] != 2500 {
		t.Fatalf("scheme switch: %+v", cfg.schemes)
	}
	if err = p.Apply(ctx, "cpu", store.DeviceSettings{}); err != nil {
		t.Fatal(err)
	}
	if cfg.schemes[balanced]["PROCFREQMAX"] != 0 || len(state.ProcessorSchemes()) != 0 {
		t.Fatalf("reset: %+v, recorded %v", cfg.schemes, state.ProcessorSchemes())
	}
	// a reset with nothing recorded changes nothing
	cfg.calls = nil
	if err = p.Apply(ctx, "cpu", store.DeviceSettings{}); err != nil || len(cfg.calls) != 0 {
		t.Fatalf("idle reset: %v %v", cfg.calls, err)
	}
}
