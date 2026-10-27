package provider

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"kairo/internal/store"
)

// inventory is a GPU's inventory fields from a CSV row "uuid, index,
// memory.total, memory.free, utilization.gpu, temperature.gpu", plus extra
// field=value pairs.
func inventory(row string, extra ...string) map[string]string {
	gpu := map[string]string{}
	for i, v := range strings.Split(row, ",") {
		gpu[nvidiaInventoryFields[i]] = strings.TrimSpace(v)
	}
	for _, pair := range extra {
		field, value, _ := strings.Cut(pair, "=")
		gpu[field] = value
	}
	return gpu
}

// queryAnswer answers a --query-gpu run as nvidia-smi does: a CSV row per
// GPU with the fields asked, "[N/A]" for those the GPU lacks.
func queryAnswer(args []string, gpus ...map[string]string) []byte {
	fields := strings.Split(strings.TrimPrefix(args[0], "--query-gpu="), ",")
	var out strings.Builder
	for _, gpu := range gpus {
		values := make([]string, len(fields))
		for i, field := range fields {
			v, ok := gpu[field]
			if !ok {
				v = "[N/A]"
			}
			values[i] = v
		}
		out.WriteString(strings.Join(values, ", ") + "\n")
	}
	return []byte(out.String())
}

// exitError is a command's exit status, as extcmd wraps an *exec.ExitError.
type exitError int

func (e exitError) Error() string { return fmt.Sprintf("exit status %d", int(e)) }
func (e exitError) ExitCode() int { return int(e) }

func f(v float64) *float64 { return &v }
func i(v int64) *int64     { return &v }

// An RTX 3080 as driver 610 reports it (the values of a real query), next to
// a GPU that lacks much of it: a missing field is absent, never an error.
func TestNVIDIAReadsPowerClocksAndStatePerGPU(t *testing.T) {
	var asked []string
	p := &NVIDIA{ProviderID: "p", NodeID: "n", Command: func(_ context.Context, args ...string) ([]byte, error) {
		if args[0] == "pmon" {
			return []byte("# gpu pid type fb command\n"), nil
		}
		asked = append(asked, args[0])
		return queryAnswer(args,
			inventory("GPU-e710f4db-fe00-ca74-7acc-e8ef0092d37c, 0, 10240, 9000, 97, 66",
				"power.draw.average=110.93", "power.draw=110.99", "enforced.power.limit=320.00", "power.default_limit=320.00", "power.min_limit=100.00", "power.max_limit=350.00",
				"clocks.sm=1800", "clocks.mem=9251", "clocks.max.sm=2130", "clocks.max.mem=9501", "pstate=P2", "clocks_event_reasons.active=0x0000000000000004",
				"pcie.link.gen.gpucurrent=4", "pcie.link.gen.max=4", "pcie.link.width.current=16", "pcie.link.width.max=16", "utilization.memory=31", "fan.speed=45"),
			inventory("GPU-b, 1, 81920, 81000, [N/A], [N/A]",
				"power.draw.average=[Not Supported]", "power.draw=61.5", "enforced.power.limit=[N/A]", "clocks.sm=210", "pstate=N/A",
				"clocks_event_reasons.active=[Requested functionality has been deprecated]", "fan.speed=[N/A]", "pcie.link.gen.gpucurrent=garbage"),
		), nil
	}}
	snap, err := p.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(asked) != 1 || asked[0] != "--query-gpu="+strings.Join(nvidiaQueries[0], ",") {
		t.Fatalf("queries %v", asked)
	}
	if len(snap.Observations) != 2 {
		t.Fatalf("observations %+v", snap.Observations)
	}
	a, b := snap.Observations[0], snap.Observations[1]
	pstate := "P2"
	want := store.DeviceSample{PowerW: f(110.93), PowerLimitW: f(320), SMClockMHz: i(1800), MemClockMHz: i(9251), PState: &pstate, EventReasons: i(4),
		MemUtilization: f(31), FanPercent: f(45), PCIeGen: i(4), PCIeWidth: i(16),
		DeviceProperties: store.DeviceProperties{DefaultPowerLimitW: f(320), MinPowerLimitW: f(100), MaxPowerLimitW: f(350), MaxSMClockMHz: i(2130), MaxMemClockMHz: i(9501), PCIeGenMax: i(4), PCIeWidthMax: i(16)}}
	if a.Device == nil || !sameSample(*a.Device, want) {
		t.Fatalf("RTX 3080 sample %s, want %s", sampleText(a.Device), sampleText(&want))
	}
	if *a.Utilization != 97 || *a.TemperatureC != 66 || *a.TotalBytes != 10240<<20 {
		t.Fatalf("inventory of the first GPU: %+v", a)
	}
	// power.draw stands in for the average; whatever is not reported is nil.
	want = store.DeviceSample{PowerW: f(61.5), SMClockMHz: i(210)}
	if b.Device == nil || !sameSample(*b.Device, want) {
		t.Fatalf("sparse GPU sample %s, want %s", sampleText(b.Device), sampleText(&want))
	}
	if b.Utilization != nil || b.TemperatureC != nil || *b.FreeBytes != 81000<<20 {
		t.Fatalf("a GPU without utilization or temperature: %+v", b)
	}
}

func sampleText(s *store.DeviceSample) string {
	if s == nil {
		return "<nil>"
	}
	var parts []string
	add := func(name string, v any) { parts = append(parts, name+"="+v.(string)) }
	for name, v := range map[string]*float64{"power": s.PowerW, "limit": s.PowerLimitW, "memutil": s.MemUtilization, "fan": s.FanPercent, "default": s.DefaultPowerLimitW, "min": s.MinPowerLimitW, "max": s.MaxPowerLimitW} {
		if v != nil {
			add(name, fmt.Sprint(*v))
		}
	}
	for name, v := range map[string]*int64{"sm": s.SMClockMHz, "mem": s.MemClockMHz, "reasons": s.EventReasons, "gen": s.PCIeGen, "width": s.PCIeWidth, "maxsm": s.MaxSMClockMHz, "maxmem": s.MaxMemClockMHz, "genmax": s.PCIeGenMax, "widthmax": s.PCIeWidthMax} {
		if v != nil {
			add(name, fmt.Sprint(*v))
		}
	}
	if s.PState != nil {
		add("pstate", *s.PState)
	}
	slices.Sort(parts)
	return strings.Join(parts, " ")
}

func sameSample(a, b store.DeviceSample) bool { return sampleText(&a) == sampleText(&b) }

// A driver that does not know a field rejects the whole query (exit status
// 2): the provider falls back to the older field names, then to the
// inventory alone, and keeps asking for what worked.
func TestNVIDIAFallsBackToFieldsTheDriverKnows(t *testing.T) {
	for _, tc := range []struct {
		name     string
		knows    func(field string) bool
		wantUsed int
	}{
		{"older aliases", func(field string) bool {
			return field != "power.draw.average" && field != "clocks_event_reasons.active" && field != "pcie.link.gen.gpucurrent"
		}, 1},
		{"inventory only", func(field string) bool { return slices.Contains(nvidiaInventoryFields, field) }, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var asked []int
			p := &NVIDIA{ProviderID: "p", NodeID: "n", Command: func(_ context.Context, args ...string) ([]byte, error) {
				if args[0] == "pmon" {
					return []byte("# gpu pid type fb command\n"), nil
				}
				fields := strings.Split(strings.TrimPrefix(args[0], "--query-gpu="), ",")
				asked = append(asked, slices.IndexFunc(nvidiaQueries, func(q []string) bool { return slices.Equal(q, fields) }))
				for _, field := range fields {
					if !tc.knows(field) {
						return nil, fmt.Errorf("nvidia-smi: %w", exitError(2))
					}
				}
				return queryAnswer(args, inventory("GPU-a, 0, 8192, 7600, 2, 40", "power.draw=55.5", "clocks_throttle_reasons.active=0x1", "pcie.link.gen.current=3")), nil
			}}
			for range 2 {
				snap, err := p.Observe(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				device := snap.Observations[0].Device
				if tc.wantUsed == 2 {
					if device != nil {
						t.Fatalf("the inventory alone has no device sample: %s", sampleText(device))
					}
				} else if device == nil || *device.PowerW != 55.5 || *device.EventReasons != 1 || *device.PCIeGen != 3 {
					t.Fatalf("older aliases: %s", sampleText(device))
				}
			}
			want := []int{}
			for q := 0; q <= tc.wantUsed; q++ {
				want = append(want, q)
			}
			want = append(want, tc.wantUsed)
			if !slices.Equal(asked, want) {
				t.Fatalf("queries asked %v, want %v", asked, want)
			}
		})
	}
	// Any other failure fails the observation without trying other fields.
	runs := 0
	p := &NVIDIA{ProviderID: "p", NodeID: "n", Command: func(context.Context, ...string) ([]byte, error) {
		runs++
		return nil, fmt.Errorf("nvidia-smi: %w", exitError(9))
	}}
	if _, err := p.Observe(context.Background()); err == nil || runs != 1 || !errors.As(err, new(exitError)) {
		t.Fatalf("driver failure: error %v after %d runs", err, runs)
	}
}
