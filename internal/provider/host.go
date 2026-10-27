//go:build linux || windows

package provider

import (
	"context"
	"encoding/json"
	"runtime"
	"sync"
	"time"

	"kairo/internal/cpupower"
	"kairo/internal/store"
)

// Host reports a node's logical CPUs and physical memory (on Linux, including
// a WSL2 distro or a container, what /proc/meminfo shows: the visible limit),
// and its CPU package power where the machine has a readable meter.
type Host struct {
	ProviderID string
	NodeID     string
	TTL        time.Duration
	// Meter reads the CPU package power; nil means cpupower.New().
	Meter cpupower.Meter

	meterOnce sync.Once
}

func (p *Host) ID() string { return p.ProviderID }

// snapshot is the node's cpu and ram as observed now, the cpu with the CPU
// package power when the meter has a reading.
func (p *Host) snapshot(ramTotal, ramFree int64) Snapshot {
	t := time.Now().UTC()
	ttl := p.TTL
	if ttl == 0 {
		ttl = 15 * time.Second
	}
	stamp, valid := t.Format(time.RFC3339Nano), t.Add(ttl).Format(time.RFC3339Nano)
	cpuTotal := int64(runtime.NumCPU() * 1000)
	empty := json.RawMessage(`{}`)
	resources := []store.ResourceInstance{{ID: p.NodeID + "-cpu", NodeID: p.NodeID, ProviderID: p.ProviderID, Kind: "cpu", StableIdentity: "logical-cpu", Binding: empty, Attributes: empty, AdminState: "enabled"}, {ID: p.NodeID + "-ram", NodeID: p.NodeID, ProviderID: p.ProviderID, Kind: "ram", StableIdentity: "physical-memory", Binding: empty, Attributes: empty, AdminState: "enabled"}}
	observations := []store.Observation{{ResourceID: resources[0].ID, ObservedAt: stamp, ValidUntil: valid, TotalBytes: &cpuTotal, FreeBytes: &cpuTotal, Evidence: empty}, {ResourceID: resources[1].ID, ObservedAt: stamp, ValidUntil: valid, TotalBytes: &ramTotal, FreeBytes: &ramFree, Evidence: empty}}
	p.meterOnce.Do(func() {
		if p.Meter == nil {
			p.Meter = cpupower.New()
		}
	})
	if reading, ok := p.Meter.Read(); ok {
		watts := reading.Watts
		observations[0].Device = &store.DeviceSample{PowerW: &watts}
	}
	return Snapshot{Resources: resources, Observations: observations}
}

func (p *Host) Prepare(ctx context.Context, r store.ResourceInstance) error {
	_, err := p.Observe(ctx)
	return err
}

func (p *Host) Release(context.Context, store.ResourceInstance) error { return nil }
