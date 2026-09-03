//go:build windows

package provider

import (
	"context"
	"encoding/json"
	"time"

	"golang.org/x/sys/windows"
	"kairo/internal/store"
)

type Filesystem struct {
	ProviderID  string
	NodeID      string
	Filesystems []string
	TTL         time.Duration
}

func (p *Filesystem) ID() string { return p.ProviderID }
func (p *Filesystem) Observe(context.Context) (Snapshot, error) {
	snap := Snapshot{}
	t := time.Now().UTC()
	ttl := p.TTL
	if ttl == 0 {
		ttl = 15 * time.Second
	}
	for _, filesystem := range p.Filesystems {
		path, err := windows.UTF16PtrFromString(filesystem + `\`)
		if err != nil {
			return Snapshot{}, err
		}
		var available, total, free uint64
		if err = windows.GetDiskFreeSpaceEx(path, &available, &total, &free); err != nil {
			return Snapshot{}, err
		}
		resourceID := p.NodeID + "-disk-" + filesystem
		binding, _ := json.Marshal(map[string]string{"filesystem": filesystem})
		snap.Resources = append(snap.Resources, store.ResourceInstance{ID: resourceID, NodeID: p.NodeID, ProviderID: p.ProviderID, Kind: "disk", StableIdentity: filesystem, Binding: binding, Attributes: json.RawMessage(`{}`), AdminState: "enabled"})
		totalI, freeI := int64(total), int64(available)
		snap.Observations = append(snap.Observations, store.Observation{ResourceID: resourceID, ObservedAt: t.Format(time.RFC3339Nano), ValidUntil: t.Add(ttl).Format(time.RFC3339Nano), TotalBytes: &totalI, FreeBytes: &freeI, Evidence: json.RawMessage(`{}`)})
	}
	return snap, nil
}
func (p *Filesystem) Prepare(ctx context.Context, r store.ResourceInstance) error {
	_, err := p.Observe(ctx)
	return err
}
func (p *Filesystem) Release(context.Context, store.ResourceInstance) error { return nil }
