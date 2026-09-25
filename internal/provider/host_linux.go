//go:build linux

package provider

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"kairo/internal/store"
)

// Host reports logical CPUs and physical memory on a Linux node (including a
// WSL2 distro or a container, where /proc/meminfo reflects the visible limit).
type Host struct {
	ProviderID string
	NodeID     string
	TTL        time.Duration
}

func (p *Host) ID() string { return p.ProviderID }

func (p *Host) Observe(context.Context) (Snapshot, error) {
	total, available, err := readMeminfo("/proc/meminfo")
	if err != nil {
		return Snapshot{}, err
	}
	t := time.Now().UTC()
	ttl := p.TTL
	if ttl == 0 {
		ttl = 15 * time.Second
	}
	stamp, valid := t.Format(time.RFC3339Nano), t.Add(ttl).Format(time.RFC3339Nano)
	cpuTotal := int64(runtime.NumCPU() * 1000)
	empty := json.RawMessage(`{}`)
	resources := []store.ResourceInstance{{ID: p.NodeID + "-cpu", NodeID: p.NodeID, ProviderID: p.ProviderID, Kind: "cpu", StableIdentity: "logical-cpu", Binding: empty, Attributes: empty, AdminState: "enabled"}, {ID: p.NodeID + "-ram", NodeID: p.NodeID, ProviderID: p.ProviderID, Kind: "ram", StableIdentity: "physical-memory", Binding: empty, Attributes: empty, AdminState: "enabled"}}
	observations := []store.Observation{{ResourceID: resources[0].ID, ObservedAt: stamp, ValidUntil: valid, TotalBytes: &cpuTotal, FreeBytes: &cpuTotal, Evidence: empty}, {ResourceID: resources[1].ID, ObservedAt: stamp, ValidUntil: valid, TotalBytes: &total, FreeBytes: &available, Evidence: empty}}
	return Snapshot{Resources: resources, Observations: observations}, nil
}

func (p *Host) Prepare(ctx context.Context, r store.ResourceInstance) error {
	_, err := p.Observe(ctx)
	return err
}

func (p *Host) Release(context.Context, store.ResourceInstance) error { return nil }

// readMeminfo returns MemTotal and MemAvailable in bytes.
func readMeminfo(path string) (total, available int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	found := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		kb, perr := strconv.ParseInt(fields[1], 10, 64)
		if perr != nil {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			total, found = kb*1024, found+1
		case "MemAvailable:":
			available, found = kb*1024, found+1
		}
	}
	if err = sc.Err(); err != nil {
		return 0, 0, err
	}
	if found != 2 {
		return 0, 0, fmt.Errorf("%s lacks MemTotal/MemAvailable", path)
	}
	return total, available, nil
}
