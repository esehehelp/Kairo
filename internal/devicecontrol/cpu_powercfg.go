package devicecontrol

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"kairo/internal/extcmd"
	"kairo/internal/store"
)

// powercfgTimeout bounds each powercfg run.
const powercfgTimeout = 20 * time.Second

// processorFrequencySettings are the power settings capping the processor
// frequency (MHz, 0: no cap): PROCFREQMAX, and PROCFREQMAX1 for the second
// efficiency class of a hybrid CPU where Windows has it.
var processorFrequencySettings = []string{"PROCFREQMAX", "PROCFREQMAX1"}

var (
	schemeGUID = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	hexValue   = regexp.MustCompile(`0x[0-9a-fA-F]+`)
)

// Powercfg changes a Windows host's maximum processor frequency: the AC
// value of PROCFREQMAX (and PROCFREQMAX1) of the active power scheme, which
// is then re-activated. What the user had there cannot be read back once it
// was changed, so the values found are kept in State before the first change
// and written back on reset.
type Powercfg struct {
	State *State
	// Command runs powercfg (nil: powercfg.exe).
	Command func(ctx context.Context, args ...string) ([]byte, error)
}

func (p *Powercfg) run(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, powercfgTimeout)
	defer cancel()
	if p.Command != nil {
		return p.Command(ctx, args...)
	}
	return extcmd.Output(ctx, "powercfg", args...)
}

func (p *Powercfg) activeScheme(ctx context.Context) (string, error) {
	out, err := p.run(ctx, "/getactivescheme")
	if err != nil {
		return "", err
	}
	guid := schemeGUID.Find(out)
	if guid == nil {
		return "", fmt.Errorf("powercfg /getactivescheme printed no scheme: %q", out)
	}
	return string(guid), nil
}

// query is a setting's AC value. The output is localized; its last two hex
// values are the current AC and DC indexes, in that order.
func (p *Powercfg) query(ctx context.Context, scheme, setting string) (uint32, error) {
	out, err := p.run(ctx, "/qh", scheme, "SUB_PROCESSOR", setting)
	if err != nil {
		return 0, err
	}
	values := hexValue.FindAll(out, -1)
	if len(values) < 2 {
		return 0, fmt.Errorf("powercfg /qh %s SUB_PROCESSOR %s printed no AC value", scheme, setting)
	}
	v, err := strconv.ParseUint(string(values[len(values)-2]), 0, 32)
	return uint32(v), err
}

// current is the scheme's value of each processor frequency setting Windows
// has (PROCFREQMAX at least).
func (p *Powercfg) current(ctx context.Context, scheme string) (map[string]uint32, error) {
	values := map[string]uint32{}
	for i, setting := range processorFrequencySettings {
		v, err := p.query(ctx, scheme, setting)
		if err != nil {
			if i == 0 {
				return nil, fmt.Errorf("read the maximum processor frequency: %w", err)
			}
			continue
		}
		values[setting] = v
	}
	return values, nil
}

func (p *Powercfg) Capabilities(ctx context.Context, _ string) (store.DeviceCapabilities, error) {
	scheme, err := p.activeScheme(ctx)
	if err != nil {
		return store.DeviceCapabilities{}, err
	}
	if _, err = p.current(ctx, scheme); err != nil {
		return store.DeviceCapabilities{}, err
	}
	return store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}}, nil
}

// Apply caps the active scheme's frequency at s.MaxFreqMHz, recording the
// values it had first; every other scheme the controller changed (all of
// them on a reset) gets its recorded values back.
func (p *Powercfg) Apply(ctx context.Context, _ string, s store.DeviceSettings) error {
	if s.MaxPerfPct != nil {
		return errors.New("a Windows host takes max_freq_mhz, not max_perf_pct")
	}
	if p.State == nil {
		return errors.New("no state file to keep the processor settings in")
	}
	active, err := p.activeScheme(ctx)
	if err != nil {
		return err
	}
	if s.MaxFreqMHz != nil {
		if p.State.ProcessorScheme(active) == nil {
			original, err := p.current(ctx, active)
			if err != nil {
				return err
			}
			// persisted before the first change: after that it cannot be read
			if err = p.State.RecordProcessorScheme(active, original); err != nil {
				return err
			}
		}
		for setting := range p.State.ProcessorScheme(active) {
			if _, err = p.run(ctx, "/setacvalueindex", active, "SUB_PROCESSOR", setting, strconv.Itoa(*s.MaxFreqMHz)); err != nil {
				return err
			}
		}
		if _, err = p.run(ctx, "/setactive", active); err != nil {
			return err
		}
	}
	var errs []error
	for _, scheme := range p.State.ProcessorSchemes() {
		if s.MaxFreqMHz != nil && scheme == active {
			continue
		}
		if err := p.restore(ctx, scheme, scheme == active); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// restore writes a scheme's recorded values back, then forgets them.
func (p *Powercfg) restore(ctx context.Context, scheme string, active bool) error {
	for setting, v := range p.State.ProcessorScheme(scheme) {
		if _, err := p.run(ctx, "/setacvalueindex", scheme, "SUB_PROCESSOR", setting, strconv.FormatUint(uint64(v), 10)); err != nil {
			return fmt.Errorf("restore %s of power scheme %s: %w", setting, scheme, err)
		}
	}
	if active {
		if _, err := p.run(ctx, "/setactive", scheme); err != nil {
			return err
		}
	}
	return p.State.ForgetProcessorScheme(scheme)
}
