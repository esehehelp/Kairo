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
	// Owner tells its owner's use of the node's CPU and RAM (owneruse.go);
	// nil tells none.
	Owner *OwnerUse
	// CPUTimes reads the node's CPU time (nil: readCPUTimes).
	CPUTimes func() (cpuTimes, error)

	meterOnce sync.Once
	cpuMu     sync.Mutex
	cpuPrev   cpuTimes
	cpuHave   bool
}

func (p *Host) ID() string { return p.ProviderID }

// snapshot is the node's cpu and ram as observed now, the cpu with the CPU
// package power when the meter has a reading, the ram with the floor its
// owner keeps free now (evidence floor_bytes: admission leaves it free).
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
	ramEvidence := empty
	if p.Owner.enabled() && ramTotal > 0 {
		present, _ := p.Owner.present()
		ramEvidence, _ = json.Marshal(map[string]int64{"floor_bytes": p.Owner.ramFloor(ramTotal, present)})
	}
	observations := []store.Observation{{ResourceID: resources[0].ID, ObservedAt: stamp, ValidUntil: valid, TotalBytes: &cpuTotal, FreeBytes: &cpuTotal, Evidence: empty}, {ResourceID: resources[1].ID, ObservedAt: stamp, ValidUntil: valid, TotalBytes: &ramTotal, FreeBytes: &ramFree, Evidence: ramEvidence}}
	p.meterOnce.Do(func() {
		if p.Meter == nil {
			p.Meter = cpupower.New()
		}
	})
	if reading, ok := p.Meter.Read(); ok {
		watts := reading.Watts
		observations[0].Device = &store.DeviceSample{PowerW: &watts}
	}
	return Snapshot{Resources: resources, Observations: observations, Claims: p.ownerClaims(resources[0].ID, resources[1].ID, ramTotal, ramFree)}
}

// ownerClaims are the owner_use claims of the node's CPU and RAM
// (owneruse.go): the CPU's from this reading of its CPU time and the last.
func (p *Host) ownerClaims(cpuID, ramID string, ramTotal, ramFree int64) []store.ExternalClaim {
	if !p.Owner.enabled() {
		return nil
	}
	present, _ := p.Owner.present()
	var claims []store.ExternalClaim
	read := p.CPUTimes
	if read == nil {
		read = readCPUTimes
	}
	if cur, err := read(); err == nil {
		p.cpuMu.Lock()
		prev, have := p.cpuPrev, p.cpuHave
		p.cpuPrev, p.cpuHave = cur, true
		p.cpuMu.Unlock()
		if busy, kairo, owner, ok := cpuOwnerUse(prev, cur); have && ok {
			threshold := p.Owner.cpuThreshold(present)
			if busy >= ownerCPUBusy && owner*100 >= threshold {
				claims = append(claims, ownerClaim(cpuID, map[string]any{"source": "observed", "busy_percent": busy * 100, "kairo_percent": kairo * 100,
					"owner_percent": owner * 100, "threshold_percent": threshold, "owner_present": present}))
			}
		}
	}
	if floor := p.Owner.ramFloor(ramTotal, present); ramTotal > 0 && ramFree < floor {
		claims = append(claims, ownerClaim(ramID, map[string]any{"source": "observed", "available_bytes": ramFree, "floor_bytes": floor, "owner_present": present}))
	}
	return claims
}

func (p *Host) Prepare(ctx context.Context, r store.ResourceInstance) error {
	_, err := p.Observe(ctx)
	return err
}

func (p *Host) Release(context.Context, store.ResourceInstance) error { return nil }
