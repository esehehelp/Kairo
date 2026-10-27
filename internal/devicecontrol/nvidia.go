package devicecontrol

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"kairo/internal/extcmd"
	"kairo/internal/store"
)

// nvidiaSMITimeout bounds each nvidia-smi run.
const nvidiaSMITimeout = 20 * time.Second

// nvidiaNotSupported is nvidia-smi's exit code for an operation the device
// does not offer (a GeForce memory clock lock, say).
const nvidiaNotSupported = 3

// NVIDIA changes a GPU, named by its UUID, with nvidia-smi: the graphics
// clock maximum (-lgc / -rgc), a locked memory clock (-lmc / -rmc) and the
// power limit (-pl, back to power.default_limit). Every default can be read
// from the hardware, so it keeps no state.
type NVIDIA struct {
	// Command runs nvidia-smi (nil: the nvidia-smi on PATH).
	Command func(ctx context.Context, args ...string) ([]byte, error)
	Timeout time.Duration

	mu   sync.Mutex
	caps map[string]store.DeviceCapabilities
}

func (n *NVIDIA) run(ctx context.Context, args ...string) ([]byte, error) {
	timeout := n.Timeout
	if timeout == 0 {
		timeout = nvidiaSMITimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if n.Command != nil {
		return n.Command(ctx, args...)
	}
	return extcmd.Output(ctx, "nvidia-smi", args...)
}

func (n *NVIDIA) Capabilities(ctx context.Context, uuid string) (store.DeviceCapabilities, error) {
	out, err := n.run(ctx, "-i", uuid, "-q", "-d", "SUPPORTED_CLOCKS")
	if err != nil {
		return store.DeviceCapabilities{}, fmt.Errorf("nvidia-smi supported clocks of %s: %w", uuid, err)
	}
	caps := parseSupportedClocks(out)
	out, err = n.run(ctx, "-i", uuid, "--query-gpu=power.min_limit,power.max_limit,power.default_limit", "--format=csv,noheader,nounits")
	if err != nil {
		return store.DeviceCapabilities{}, fmt.Errorf("nvidia-smi power limits of %s: %w", uuid, err)
	}
	limits := strings.Split(strings.TrimSpace(string(out)), ",")
	if len(limits) == 3 {
		caps.PowerLimitMinW, caps.PowerLimitMaxW, caps.PowerLimitDefaultW = watts(limits[0]), watts(limits[1]), watts(limits[2])
	}
	caps.Settings = []string{}
	if len(caps.GraphicsClocksMHz) != 0 {
		caps.Settings = append(caps.Settings, store.SettingGraphicsClockMax)
	}
	if len(caps.MemoryClocksMHz) != 0 {
		caps.Settings = append(caps.Settings, store.SettingMemoryClock)
	}
	if caps.PowerLimitMinW != nil && caps.PowerLimitMaxW != nil && caps.PowerLimitDefaultW != nil {
		caps.Settings = append(caps.Settings, store.SettingPowerLimit)
	}
	n.mu.Lock()
	if n.caps == nil {
		n.caps = map[string]store.DeviceCapabilities{}
	}
	n.caps[uuid] = caps
	n.mu.Unlock()
	return caps, nil
}

// parseSupportedClocks reads `nvidia-smi -q -d SUPPORTED_CLOCKS`: "Memory :
// 9501 MHz" lines, each followed by its "Graphics : 2130 MHz" lines.
func parseSupportedClocks(out []byte) store.DeviceCapabilities {
	var caps store.DeviceCapabilities
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), ":")
		if !found {
			continue
		}
		mhz, err := strconv.Atoi(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(value), "MHz")))
		if err != nil {
			continue
		}
		switch strings.TrimSpace(key) {
		case "Memory":
			caps.MemoryClocksMHz = append(caps.MemoryClocksMHz, mhz)
		case "Graphics":
			caps.GraphicsClocksMHz = append(caps.GraphicsClocksMHz, mhz)
		}
	}
	for _, list := range []*[]int{&caps.MemoryClocksMHz, &caps.GraphicsClocksMHz} {
		slices.Sort(*list)
		*list = slices.Compact(*list)
	}
	return caps
}

func watts(field string) *float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(field), 64)
	if err != nil {
		return nil // [N/A]
	}
	return &v
}

// Apply sets or resets each of the three settings; one that fails does not
// keep the others from being made. A reset the device does not offer (exit
// code 3) is already at its default.
func (n *NVIDIA) Apply(ctx context.Context, uuid string, s store.DeviceSettings) error {
	n.mu.Lock()
	caps, known := n.caps[uuid]
	n.mu.Unlock()
	var errs []error
	if !known {
		var err error
		if caps, err = n.Capabilities(ctx, uuid); err != nil {
			if !s.IsDefault() {
				return err
			}
			// A reset needs no capabilities but the default power limit:
			// the clocks are reset all the same.
			errs = append(errs, fmt.Errorf("power limit not reset: %w", err))
		}
	}
	step := func(reset bool, args ...string) {
		_, err := n.run(ctx, append([]string{"-i", uuid}, args...)...)
		if err != nil && !(reset && exitCode(err) == nvidiaNotSupported) {
			errs = append(errs, fmt.Errorf("nvidia-smi %s: %w", strings.Join(args, " "), err))
		}
	}
	if v := s.GraphicsClockMaxMHz; v != nil {
		// the range runs from the lowest supported graphics clock to the maximum
		low := *v
		if len(caps.GraphicsClocksMHz) != 0 {
			low = min(caps.GraphicsClocksMHz[0], *v)
		}
		step(false, "-lgc", fmt.Sprintf("%d,%d", low, *v))
	} else {
		step(true, "-rgc")
	}
	if v := s.MemoryClockMHz; v != nil {
		step(false, "-lmc", fmt.Sprintf("%d,%d", *v, *v))
	} else {
		step(true, "-rmc")
	}
	switch {
	case s.PowerLimitW != nil:
		step(false, "-pl", strconv.Itoa(*s.PowerLimitW))
	case caps.PowerLimitDefaultW != nil:
		step(true, "-pl", strconv.FormatFloat(*caps.PowerLimitDefaultW, 'f', -1, 64))
	}
	// (a default limit nvidia-smi reports as [N/A]: the card has no power
	// limit to change, so it cannot have been changed)
	return errors.Join(errs...)
}

// exitCode is the exit code of a command that ran and failed, else -1.
func exitCode(err error) int {
	var exit interface{ ExitCode() int }
	if errors.As(err, &exit) {
		return exit.ExitCode()
	}
	return -1
}
