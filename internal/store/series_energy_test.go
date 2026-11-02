package store

import (
	"context"
	"math"
	"testing"
	"time"
)

// An energy window reads the samples up to the first one at or after its end
// (which closes its last span), and none after: a window ending between two
// samples still counts that last span.
func TestSeriesEnergyEndsAtTheSampleThatClosesTheWindow(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	sample := func(at time.Duration, watts float64) ControllerSample {
		return ControllerSample{DeviceID: "cpu", ObservedAt: t0.Add(at).Format(time.RFC3339Nano), ValidUntil: t0.Add(at + 20*time.Second).Format(time.RFC3339Nano), PowerW: &watts}
	}
	var samples []ControllerSample
	for i := 0; i <= 30; i++ {
		samples = append(samples, sample(time.Duration(i)*10*time.Second, float64(40+i)))
	}
	if skipped, err := store.RecordControllerSamples(ctx, "ctl", samples); err != nil || len(skipped) != 0 {
		t.Fatalf("record: %v %v", skipped, err)
	}
	// [5 s, 15 s]: 5 s of the 10 s sample's span (41 W), 5 s of the 20 s one's (42 W).
	e, err := store.HostCPUEnergy(ctx, "h", t0.Add(5*time.Second), t0.Add(15*time.Second))
	if err != nil || math.Abs(e.Joules-(5*41+5*42)) > 1e-6 || e.SecondsCovered != 10 {
		t.Fatalf("energy: %+v %v", e, err)
	}
}
