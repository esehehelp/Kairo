package provider

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"kairo/internal/processidentity"
	"kairo/internal/store"
)

type NVIDIA struct {
	ProviderID      string
	NodeID          string
	TTL             time.Duration
	Command         func(context.Context, ...string) ([]byte, error)
	ProcessIdentity func(int) (string, error)
}

func (p *NVIDIA) ID() string { return p.ProviderID }
func (p *NVIDIA) run(ctx context.Context, args ...string) ([]byte, error) {
	if p.Command != nil {
		return p.Command(ctx, args...)
	}
	return exec.CommandContext(ctx, "nvidia-smi", args...).Output()
}
func (p *NVIDIA) Observe(ctx context.Context) (Snapshot, error) {
	body, err := p.run(ctx, "--query-gpu=uuid,index,memory.total,memory.free,utilization.gpu,temperature.gpu", "--format=csv,noheader,nounits")
	if err != nil {
		return Snapshot{}, fmt.Errorf("nvidia-smi inventory: %w", err)
	}
	records, err := csv.NewReader(strings.NewReader(string(body))).ReadAll()
	if err != nil {
		return Snapshot{}, err
	}
	t := time.Now().UTC()
	ttl := p.TTL
	if ttl == 0 {
		ttl = 15 * time.Second
	}
	snap := Snapshot{}
	byIndex := map[string]struct{ uuid, resource string }{}
	used := map[string]int64{}
	utils := map[string]float64{}
	for _, row := range records {
		if len(row) != 6 {
			return Snapshot{}, errors.New("unexpected nvidia-smi GPU row")
		}
		for i := range row {
			row[i] = strings.TrimSpace(row[i])
		}
		total, err := strconv.ParseInt(row[2], 10, 64)
		if err != nil {
			return Snapshot{}, err
		}
		free, err := strconv.ParseInt(row[3], 10, 64)
		if err != nil {
			return Snapshot{}, err
		}
		util, _ := strconv.ParseFloat(row[4], 64)
		temp, _ := strconv.ParseFloat(row[5], 64)
		total *= 1024 * 1024
		free *= 1024 * 1024
		resourceID := "gpu-" + strings.ToLower(strings.ReplaceAll(row[0], "GPU-", ""))
		byIndex[row[1]] = struct{ uuid, resource string }{row[0], resourceID}
		used[row[0]] = total - free
		utils[row[0]] = util
		binding, _ := json.Marshal(map[string]string{"cuda_index": row[1], "uuid": row[0]})
		attrs, _ := json.Marshal(map[string]any{"uuid": row[0]})
		snap.Resources = append(snap.Resources, store.ResourceInstance{ID: resourceID, NodeID: p.NodeID, ProviderID: p.ProviderID, Kind: "gpu", StableIdentity: row[0], Binding: binding, Attributes: attrs, AdminState: "enabled"})
		stamp, valid := t.Format(time.RFC3339Nano), t.Add(ttl).Format(time.RFC3339Nano)
		evidence, _ := json.Marshal(map[string]string{"source": "nvidia-smi"})
		snap.Observations = append(snap.Observations, store.Observation{ResourceID: resourceID, ObservedAt: stamp, ValidUntil: valid, TotalBytes: &total, FreeBytes: &free, Utilization: &util, TemperatureC: &temp, Evidence: evidence})
	}
	// pmon exposes the C/G process type. query-compute-apps on WDDM drivers may
	// include ordinary desktop graphics clients, which must not claim a GPU.
	processes, processErr := p.run(ctx, "pmon", "-c", "1", "-s", "m")
	seenProcess := map[string]bool{}
	if processErr == nil {
		for _, line := range strings.Split(string(processes), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 4 || strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			entry, ok := byIndex[fields[0]]
			pid, parseErr := strconv.Atoi(fields[1])
			processType := fields[2]
			name := fields[len(fields)-1]
			fb, _ := strconv.Atoi(fields[3])
			if !ok || parseErr != nil || pid <= 0 || !isComputeProcess(processType, name, fb) {
				continue
			}
			uuid, resourceID := entry.uuid, entry.resource
			if resourceID == "" {
				continue
			}
			seenProcess[uuid] = true
			pidText := strconv.Itoa(pid)
			identityFn := p.ProcessIdentity
			if identityFn == nil {
				identityFn = processidentity.ForPID
			}
			identity, identityErr := identityFn(pid)
			if identityErr != nil {
				identity = "pid:" + pidText + ":start:unknown"
			}
			evidence, _ := json.Marshal(map[string]string{"pid": pidText, "process_name": name, "gpu_uuid": uuid, "process_type": processType})
			identityHash := fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))[:16]
			snap.Claims = append(snap.Claims, store.ExternalClaim{ID: "claim-" + resourceID + "-process-" + identityHash, ResourceID: resourceID, ClaimKind: "external_process", ProcessIdentity: &identity, Evidence: evidence})
		}
	}
	for uuid, bytes := range used {
		if bytes > 1024*1024*1024 && utils[uuid] >= 10 && !seenProcess[uuid] {
			resourceID := ""
			for _, entry := range byIndex {
				if entry.uuid == uuid {
					resourceID = entry.resource
					break
				}
			}
			evidence, _ := json.Marshal(map[string]any{"used_memory_bytes": bytes, "gpu_uuid": uuid})
			snap.Claims = append(snap.Claims, store.ExternalClaim{ID: "claim-" + resourceID + "-unattributed", ResourceID: resourceID, ClaimKind: "unattributed_activity", Evidence: evidence})
		}
	}
	return snap, nil
}

func isComputeProcess(processType, name string, framebufferMiB int) bool {
	if processType == "C" {
		return true
	}
	if !strings.Contains(processType, "C") {
		return false
	}
	if framebufferMiB > 64 {
		return true
	}
	lower := strings.ToLower(name)
	for _, marker := range []string{"python", "torchrun", "llama", "vllm", "cuda"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}
func (p *NVIDIA) Prepare(ctx context.Context, r store.ResourceInstance) error {
	snap, err := p.Observe(ctx)
	if err != nil {
		return err
	}
	for i, item := range snap.Resources {
		if item.ID == r.ID {
			obs := snap.Observations[i]
			deadline, _ := time.Parse(time.RFC3339Nano, obs.ValidUntil)
			if deadline.After(time.Now().UTC()) {
				return nil
			}
		}
	}
	return errors.New("GPU disappeared during prepare")
}
func (p *NVIDIA) Release(context.Context, store.ResourceInstance) error { return nil }
