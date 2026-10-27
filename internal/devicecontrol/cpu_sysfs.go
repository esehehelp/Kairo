package devicecontrol

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"kairo/internal/store"
)

// Sysfs changes a Linux host's CPUs through /sys/devices/system/cpu:
// intel_pstate's max_perf_pct when the driver offers it (default 100), else
// cpufreq's scaling_max_freq of every CPU (default its cpuinfo_max_freq).
// Every default can be read from the hardware, so it keeps no state.
type Sysfs struct {
	// Root is /sys/devices/system/cpu (a directory of the same layout in tests).
	Root string
}

func (s *Sysfs) root() string {
	if s.Root != "" {
		return s.Root
	}
	return "/sys/devices/system/cpu"
}

func (s *Sysfs) pstate() string { return filepath.Join(s.root(), "intel_pstate", "max_perf_pct") }

// policies are the cpufreq directories of the online CPUs.
func (s *Sysfs) policies() ([]string, error) {
	dirs, err := filepath.Glob(filepath.Join(s.root(), "cpu[0-9]*", "cpufreq"))
	if err != nil {
		return nil, err
	}
	if len(dirs) == 0 {
		return nil, fmt.Errorf("%s has neither intel_pstate nor cpufreq: the CPU frequency cannot be changed here", s.root())
	}
	return dirs, nil
}

func readInt(path string) (int, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(body)))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", path, err)
	}
	return v, nil
}

func writeInt(path string, v int) error {
	return os.WriteFile(path, []byte(strconv.Itoa(v)+"\n"), 0o644)
}

func (s *Sysfs) Capabilities(_ context.Context, _ string) (store.DeviceCapabilities, error) {
	if _, err := os.Stat(s.pstate()); err == nil {
		low, err := readInt(filepath.Join(s.root(), "intel_pstate", "min_perf_pct"))
		if err != nil {
			return store.DeviceCapabilities{}, err
		}
		return store.DeviceCapabilities{Settings: []string{store.SettingMaxPerf}, MaxPerfPctMin: &low}, nil
	}
	dirs, err := s.policies()
	if err != nil {
		return store.DeviceCapabilities{}, err
	}
	lowest, highest := 0, 0
	for i, dir := range dirs {
		low, err := readInt(filepath.Join(dir, "cpuinfo_min_freq"))
		if err != nil {
			return store.DeviceCapabilities{}, err
		}
		high, err := readInt(filepath.Join(dir, "cpuinfo_max_freq"))
		if err != nil {
			return store.DeviceCapabilities{}, err
		}
		if i == 0 || low < lowest {
			lowest = low
		}
		highest = max(highest, high)
	}
	// sysfs counts kHz
	lowest, highest = (lowest+999)/1000, highest/1000
	return store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}, MaxFreqMinMHz: &lowest, MaxFreqMaxMHz: &highest}, nil
}

func (s *Sysfs) Apply(_ context.Context, _ string, settings store.DeviceSettings) error {
	if _, err := os.Stat(s.pstate()); err == nil {
		if settings.MaxFreqMHz != nil {
			return errors.New("this host's intel_pstate takes max_perf_pct, not max_freq_mhz")
		}
		pct := 100
		if settings.MaxPerfPct != nil {
			pct = *settings.MaxPerfPct
		}
		return writeInt(s.pstate(), pct)
	}
	if settings.MaxPerfPct != nil {
		return errors.New("this host's cpufreq takes max_freq_mhz, not max_perf_pct")
	}
	dirs, err := s.policies()
	if err != nil {
		return err
	}
	var errs []error
	for _, dir := range dirs {
		high, err := readInt(filepath.Join(dir, "cpuinfo_max_freq"))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		target := high
		if settings.MaxFreqMHz != nil {
			low, err := readInt(filepath.Join(dir, "cpuinfo_min_freq"))
			if err != nil {
				errs = append(errs, err)
				continue
			}
			target = min(max(*settings.MaxFreqMHz*1000, low), high)
		}
		if err = writeInt(filepath.Join(dir, "scaling_max_freq"), target); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
