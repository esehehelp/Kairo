package devicecontrol

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"kairo/internal/store"
)

func TestADeviceLeavingControlAfterFailuresIsStillReset(t *testing.T) {
	daemon := &fakeDaemon{devices: []store.ControlledDevice{gpu(1, power(250))}}
	gpus := &fakeActuator{fail: errors.New("nvidia-smi -lmc: exit status 3")}
	clk := &clock{time.Now()}
	c := newTestController(daemon, gpus, &fakeActuator{}, nil, clk)
	ctx := context.Background()
	for range abandonAfter + 1 {
		c.Step(ctx)
		clk.advance(retryMax)
	}
	gpus.take()
	gpus.fail = nil
	daemon.devices = nil
	c.Step(ctx) // within the retry delay of the last failure, yet tried at once
	if got := gpus.take(); !slices.Equal(got, []string{"GPU-1 defaults"}) {
		t.Fatalf("left control: %v", got)
	}
}

func TestTheDaemonLossResetDoesNotWaitForARetry(t *testing.T) {
	daemon := &fakeDaemon{devices: []store.ControlledDevice{gpu(1, power(250))}}
	gpus := &fakeActuator{fail: errors.New("partly applied")}
	clk := &clock{time.Now()}
	c := newTestController(daemon, gpus, &fakeActuator{}, nil, clk)
	ctx := context.Background()
	for range 6 {
		c.Step(ctx)
		clk.advance(5 * time.Second)
	}
	gpus.take()
	gpus.fail = nil
	daemon.down = true
	c.Step(ctx)
	clk.advance(2 * time.Minute)
	c.Step(ctx)
	if got := gpus.take(); !slices.Equal(got, []string{"GPU-1 defaults"}) {
		t.Fatalf("loss reset: %v", got)
	}
}

func TestASettingInPlaceIsCarriedOutAgain(t *testing.T) {
	daemon := &fakeDaemon{devices: []store.ControlledDevice{gpu(1, power(250)), cpu()}}
	gpus, cpus := &fakeActuator{}, &fakeActuator{}
	clk := &clock{time.Now()}
	c := newTestController(daemon, gpus, cpus, nil, clk)
	ctx := context.Background()
	c.Step(ctx)
	gpus.take()
	cpus.take()
	reports := len(daemon.reports)
	clk.advance(reapplyEvery)
	c.Step(ctx)
	if got := gpus.take(); !slices.Equal(got, []string{"GPU-1 power_limit_w=250W"}) {
		t.Fatalf("re-apply: %v", got)
	}
	if len(cpus.calls) != 0 {
		t.Fatalf("defaults are not reset again: %v", cpus.calls)
	}
	for _, r := range daemon.reports[reports:] {
		if r.Applied != nil {
			t.Fatalf("a re-apply is not a new fact: %+v", r)
		}
	}
}

func TestNVIDIAResetsClocksWithoutCapabilities(t *testing.T) {
	var calls []string
	n := &NVIDIA{Command: func(_ context.Context, args ...string) ([]byte, error) {
		if slices.Contains(args, "SUPPORTED_CLOCKS") {
			return nil, exitError(15) // fallen off the bus, say
		}
		calls = append(calls, strings.Join(args, " "))
		return nil, nil
	}}
	err := n.Apply(context.Background(), "GPU-1", store.DeviceSettings{})
	if err == nil || !strings.Contains(err.Error(), "power limit not reset") || !slices.Equal(calls, []string{"-i GPU-1 -rgc", "-i GPU-1 -rmc"}) {
		t.Fatalf("reset: %v %v", calls, err)
	}
	limit := 250
	if err = n.Apply(context.Background(), "GPU-1", store.DeviceSettings{PowerLimitW: &limit}); err == nil || len(calls) != 2 {
		t.Fatalf("a change without capabilities: %v %v", calls, err)
	}
}
