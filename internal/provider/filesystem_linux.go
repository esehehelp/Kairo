//go:build linux

package provider

import (
	"context"
	"encoding/json"
	"syscall"
	"time"

	"kairo/internal/store"
)

// Filesystem reports capacity of the configured mount points (for example
// "/" or "/srv") on a Linux node.
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
		var st syscall.Statfs_t
		if err := syscall.Statfs(filesystem, &st); err != nil {
			return Snapshot{}, err
		}
		resourceID := p.NodeID + "-disk-" + filesystem
		binding, _ := json.Marshal(map[string]string{"filesystem": filesystem})
		snap.Resources = append(snap.Resources, store.ResourceInstance{ID: resourceID, NodeID: p.NodeID, ProviderID: p.ProviderID, Kind: "disk", StableIdentity: filesystem, Binding: binding, Attributes: json.RawMessage(`{}`), AdminState: "enabled"})
		totalI := int64(st.Blocks) * int64(st.Bsize)
		freeI := int64(st.Bavail) * int64(st.Bsize)
		snap.Observations = append(snap.Observations, store.Observation{ResourceID: resourceID, ObservedAt: t.Format(time.RFC3339Nano), ValidUntil: t.Add(ttl).Format(time.RFC3339Nano), TotalBytes: &totalI, FreeBytes: &freeI, Evidence: json.RawMessage(`{}`)})
	}
	return snap, nil
}

func (p *Filesystem) Prepare(ctx context.Context, r store.ResourceInstance) error {
	_, err := p.Observe(ctx)
	return err
}

func (p *Filesystem) Release(context.Context, store.ResourceInstance) error { return nil }
