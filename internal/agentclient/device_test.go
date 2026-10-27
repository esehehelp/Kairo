package agentclient

import (
	"context"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// An agent's observations carry their device samples to the daemon, which
// keeps the latest for resource status.
func TestAgentObservationsCarryDeviceSamples(t *testing.T) {
	st, ts, token, _ := newDaemon(t)
	ctx := context.Background()
	c := New(ts.URL, token, nil)
	registerRemote(t, c)
	if _, err := st.ApplyMachines(ctx, machine.Spec{Nodes: []machine.Node{{ID: "remote", GPUs: []string{"GPU-remote-0"}}}}, false); err != nil {
		t.Fatal(err)
	}
	snap, _ := (&fakeGPU{node: "remote"}).Observe(ctx)
	power, limit, maxLimit, reasons, pstate := 111.5, 320.0, 350.0, int64(4), "P2"
	snap.Observations[0].Device = &store.DeviceSample{PowerW: &power, PowerLimitW: &limit, PState: &pstate, EventReasons: &reasons,
		DeviceProperties: store.DeviceProperties{MaxPowerLimitW: &maxLimit}}
	if err := c.ApplyObservationBatch(ctx, "remote-gpu", snap.Resources, snap.Observations, snap.Claims); err != nil {
		t.Fatal(err)
	}
	usage, err := st.ListResourceUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d := usage["remote-gpu0"].Device
	if d == nil || *d.PowerW != power || *d.PowerLimitW != limit || *d.PState != pstate || *d.EventReasons != reasons || *d.MaxPowerLimitW != maxLimit {
		t.Fatalf("device reading %+v", d)
	}
}
