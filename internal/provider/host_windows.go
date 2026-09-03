//go:build windows

package provider

import (
	"context"
	"encoding/json"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	"kairo/internal/store"
)

type Host struct {
	ProviderID string
	NodeID     string
	TTL        time.Duration
}

func (p *Host) ID() string { return p.ProviderID }

type memoryStatusEx struct {
	Length, MemoryLoad                                                                                   uint32
	TotalPhys, AvailPhys, TotalPageFile, AvailPageFile, TotalVirtual, AvailVirtual, AvailExtendedVirtual uint64
}

func (p *Host) Observe(context.Context) (Snapshot, error) {
	m := memoryStatusEx{}
	m.Length = uint32(unsafe.Sizeof(m))
	proc := syscall.NewLazyDLL("kernel32.dll").NewProc("GlobalMemoryStatusEx")
	ok, _, err := proc.Call(uintptr(unsafe.Pointer(&m)))
	if ok == 0 {
		return Snapshot{}, err
	}
	t := time.Now().UTC()
	ttl := p.TTL
	if ttl == 0 {
		ttl = 15 * time.Second
	}
	stamp, valid := t.Format(time.RFC3339Nano), t.Add(ttl).Format(time.RFC3339Nano)
	cpuTotal := int64(runtime.NumCPU() * 1000)
	ramTotal, ramFree := int64(m.TotalPhys), int64(m.AvailPhys)
	empty := json.RawMessage(`{}`)
	resources := []store.ResourceInstance{{ID: p.NodeID + "-cpu", NodeID: p.NodeID, ProviderID: p.ProviderID, Kind: "cpu", StableIdentity: "logical-cpu", Binding: empty, Attributes: empty, AdminState: "enabled"}, {ID: p.NodeID + "-ram", NodeID: p.NodeID, ProviderID: p.ProviderID, Kind: "ram", StableIdentity: "physical-memory", Binding: empty, Attributes: empty, AdminState: "enabled"}}
	observations := []store.Observation{{ResourceID: resources[0].ID, ObservedAt: stamp, ValidUntil: valid, TotalBytes: &cpuTotal, FreeBytes: &cpuTotal, Evidence: empty}, {ResourceID: resources[1].ID, ObservedAt: stamp, ValidUntil: valid, TotalBytes: &ramTotal, FreeBytes: &ramFree, Evidence: empty}}
	return Snapshot{Resources: resources, Observations: observations}, nil
}
func (p *Host) Prepare(ctx context.Context, r store.ResourceInstance) error {
	_, err := p.Observe(ctx)
	return err
}
func (p *Host) Release(context.Context, store.ResourceInstance) error { return nil }
