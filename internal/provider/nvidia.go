package provider

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"kairo/internal/extcmd"
	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// nvidiaSMITimeout bounds each nvidia-smi run. A run that never returns (a
// pmon whose driver stopped answering) fails the observation instead of
// holding the executor's tick.
const nvidiaSMITimeout = 20 * time.Second

type NVIDIA struct {
	ProviderID      string
	NodeID          string
	TTL             time.Duration
	Timeout         time.Duration
	Command         func(context.Context, ...string) ([]byte, error)
	ProcessIdentity func(int) (string, error)
	// ProcessLineage ties a GPU process (pid, identity) to the attempt that
	// started it; nil means processidentity.LineageOf.
	ProcessLineage func(int, string) processidentity.Lineage

	query atomic.Int32 // the index in nvidiaQueries of the query this driver accepts
}

func (p *NVIDIA) ID() string { return p.ProviderID }

// run runs nvidia-smi with args under the provider's timeout. A run that does
// not finish in time fails with an error wrapping context.DeadlineExceeded.
func (p *NVIDIA) run(ctx context.Context, args ...string) ([]byte, error) {
	timeout := p.Timeout
	if timeout == 0 {
		timeout = nvidiaSMITimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var body []byte
	var err error
	if p.Command != nil {
		body, err = p.Command(ctx, args...)
	} else {
		body, err = extcmd.Output(ctx, "nvidia-smi", args...)
	}
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return nil, fmt.Errorf("did not finish within %s: %w", timeout, ctx.Err())
	}
	return body, err
}

// nvidiaInventoryFields are what every GPU observation needs: without them
// the observation fails.
var nvidiaInventoryFields = []string{"uuid", "index", "memory.total", "memory.free", "utilization.gpu", "temperature.gpu"}

// nvidiaQueries are the fields Observe asks nvidia-smi for, richest first: the
// inventory with the GPU's power and operating point by the names current
// drivers know, then by their older aliases, then the inventory alone. A
// driver that knows a field of none of them rejects the query (exit status
// 2), and the next one is tried; the first that works is kept.
var nvidiaQueries = [][]string{
	append(slices.Clone(nvidiaInventoryFields), "power.draw.average", "power.draw", "enforced.power.limit", "power.default_limit", "power.min_limit", "power.max_limit",
		"clocks.sm", "clocks.mem", "clocks.max.sm", "clocks.max.mem", "pstate", "clocks_event_reasons.active",
		"pcie.link.gen.gpucurrent", "pcie.link.gen.max", "pcie.link.width.current", "pcie.link.width.max", "utilization.memory", "fan.speed"),
	append(slices.Clone(nvidiaInventoryFields), "power.draw", "enforced.power.limit", "power.default_limit", "power.min_limit", "power.max_limit",
		"clocks.sm", "clocks.mem", "clocks.max.sm", "clocks.max.mem", "pstate", "clocks_throttle_reasons.active",
		"pcie.link.gen.current", "pcie.link.gen.max", "pcie.link.width.current", "pcie.link.width.max", "utilization.memory", "fan.speed"),
	nvidiaInventoryFields,
}

// rejectedQuery reports whether nvidia-smi refused a query's fields (exit
// status 2, "Field ... is not a valid field to query") rather than failing to
// read the GPUs.
func rejectedQuery(err error) bool {
	var exit interface{ ExitCode() int }
	return errors.As(err, &exit) && exit.ExitCode() == 2
}

// queryGPUs runs the richest query of nvidiaQueries this driver accepts and
// returns each GPU's values by field name.
func (p *NVIDIA) queryGPUs(ctx context.Context) ([]map[string]string, error) {
	for i := int(p.query.Load()); ; i++ {
		fields := nvidiaQueries[i]
		body, err := p.run(ctx, "--query-gpu="+strings.Join(fields, ","), "--format=csv,noheader,nounits")
		if err != nil {
			if i+1 < len(nvidiaQueries) && rejectedQuery(err) {
				continue
			}
			return nil, fmt.Errorf("nvidia-smi inventory: %w", err)
		}
		p.query.Store(int32(i))
		records, err := csv.NewReader(strings.NewReader(string(body))).ReadAll()
		if err != nil {
			return nil, err
		}
		gpus := make([]map[string]string, 0, len(records))
		for _, row := range records {
			if len(row) != len(fields) {
				return nil, errors.New("unexpected nvidia-smi GPU row")
			}
			gpu := make(map[string]string, len(fields))
			for j, field := range fields {
				gpu[field] = strings.TrimSpace(row[j])
			}
			gpus = append(gpus, gpu)
		}
		return gpus, nil
	}
}

// nvidiaValue is a field's value, or false when the GPU does not report it:
// nvidia-smi prints "[N/A]", "[Not Supported]", "[Requested functionality has
// been deprecated]" and the like, bracketed, or "N/A".
func nvidiaValue(gpu map[string]string, field string) (string, bool) {
	v := gpu[field]
	if v == "" || strings.HasPrefix(v, "[") || strings.EqualFold(v, "N/A") {
		return "", false
	}
	return v, true
}

// nvidiaFloat reads the first of fields the GPU reports as a number.
func nvidiaFloat(gpu map[string]string, fields ...string) *float64 {
	for _, field := range fields {
		if v, ok := nvidiaValue(gpu, field); ok {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return &f
			}
		}
	}
	return nil
}

// nvidiaInt reads the first of fields the GPU reports as an integer, decimal
// or 0x-prefixed hexadecimal (a bitmask).
func nvidiaInt(gpu map[string]string, fields ...string) *int64 {
	for _, field := range fields {
		if v, ok := nvidiaValue(gpu, field); ok {
			if n, err := strconv.ParseUint(v, 0, 64); err == nil {
				i := int64(n)
				return &i
			}
		}
	}
	return nil
}

// deviceSample is a GPU's power and operating point; nil when the query had
// none of it (the inventory alone).
func deviceSample(gpu map[string]string) *store.DeviceSample {
	if _, queried := gpu["power.draw"]; !queried {
		return nil
	}
	s := &store.DeviceSample{
		PowerW:         nvidiaFloat(gpu, "power.draw.average", "power.draw"),
		PowerLimitW:    nvidiaFloat(gpu, "enforced.power.limit"),
		SMClockMHz:     nvidiaInt(gpu, "clocks.sm"),
		MemClockMHz:    nvidiaInt(gpu, "clocks.mem"),
		EventReasons:   nvidiaInt(gpu, "clocks_event_reasons.active", "clocks_throttle_reasons.active"),
		MemUtilization: nvidiaFloat(gpu, "utilization.memory"),
		FanPercent:     nvidiaFloat(gpu, "fan.speed"),
		PCIeGen:        nvidiaInt(gpu, "pcie.link.gen.gpucurrent", "pcie.link.gen.current"),
		PCIeWidth:      nvidiaInt(gpu, "pcie.link.width.current"),
		DeviceProperties: store.DeviceProperties{
			DefaultPowerLimitW: nvidiaFloat(gpu, "power.default_limit"),
			MinPowerLimitW:     nvidiaFloat(gpu, "power.min_limit"),
			MaxPowerLimitW:     nvidiaFloat(gpu, "power.max_limit"),
			MaxSMClockMHz:      nvidiaInt(gpu, "clocks.max.sm"),
			MaxMemClockMHz:     nvidiaInt(gpu, "clocks.max.mem"),
			PCIeGenMax:         nvidiaInt(gpu, "pcie.link.gen.max"),
			PCIeWidthMax:       nvidiaInt(gpu, "pcie.link.width.max"),
		},
	}
	if v, ok := nvidiaValue(gpu, "pstate"); ok {
		s.PState = &v
	}
	return s
}

func (p *NVIDIA) Observe(ctx context.Context) (Snapshot, error) {
	gpus, err := p.queryGPUs(ctx)
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
	for _, gpu := range gpus {
		uuid, index := gpu["uuid"], gpu["index"]
		total, err := strconv.ParseInt(gpu["memory.total"], 10, 64)
		if err != nil {
			return Snapshot{}, err
		}
		free, err := strconv.ParseInt(gpu["memory.free"], 10, 64)
		if err != nil {
			return Snapshot{}, err
		}
		total *= 1024 * 1024
		free *= 1024 * 1024
		resourceID := "gpu-" + strings.ToLower(strings.ReplaceAll(uuid, "GPU-", ""))
		byIndex[index] = struct{ uuid, resource string }{uuid, resourceID}
		used[uuid] = total - free
		binding, _ := json.Marshal(map[string]string{"cuda_index": index, "uuid": uuid})
		attrs, _ := json.Marshal(map[string]any{"uuid": uuid})
		snap.Resources = append(snap.Resources, store.ResourceInstance{ID: resourceID, NodeID: p.NodeID, ProviderID: p.ProviderID, Kind: "gpu", StableIdentity: uuid, Binding: binding, Attributes: attrs, AdminState: "enabled"})
		stamp, valid := t.Format(time.RFC3339Nano), t.Add(ttl).Format(time.RFC3339Nano)
		evidence, _ := json.Marshal(map[string]string{"source": "nvidia-smi"})
		snap.Observations = append(snap.Observations, store.Observation{ResourceID: resourceID, ObservedAt: stamp, ValidUntil: valid, TotalBytes: &total, FreeBytes: &free,
			Utilization: nvidiaFloat(gpu, "utilization.gpu"), TemperatureC: nvidiaFloat(gpu, "temperature.gpu"), Evidence: evidence, Device: deviceSample(gpu)})
	}
	// pmon exposes the C/G process type. query-compute-apps on WDDM drivers may
	// include ordinary desktop graphics clients, which must not claim a GPU.
	processes, processErr := p.run(ctx, "pmon", "-c", "1", "-s", "m")
	if errors.Is(processErr, context.DeadlineExceeded) {
		// Without the process list the GPUs' claims are unknown: fail the
		// observation rather than record them as unclaimed.
		return Snapshot{}, fmt.Errorf("nvidia-smi pmon: %w", processErr)
	}
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
			// The lineage lets the daemon tell the work of the attempt holding
			// this GPU (a descendant of its registered processes) from a foreign
			// process; a process without one is foreign.
			var lineage processidentity.Lineage
			if identityErr != nil {
				identity = "pid:" + pidText + ":start:unknown"
			} else {
				lineageFn := p.ProcessLineage
				if lineageFn == nil {
					lineageFn = processidentity.LineageOf
				}
				lineage = lineageFn(pid, identity)
			}
			detail := map[string]string{"pid": pidText, "process_name": name, "gpu_uuid": uuid, "process_type": processType}
			if lineage.AttemptID != "" {
				detail["attempt_id"] = lineage.AttemptID
			}
			evidence, _ := json.Marshal(detail)
			identityHash := fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))[:16]
			snap.Claims = append(snap.Claims, store.ExternalClaim{ID: "claim-" + resourceID + "-process-" + identityHash, ResourceID: resourceID, ClaimKind: "external_process", ProcessIdentity: &identity, Evidence: evidence, AttemptID: lineage.AttemptID, Ancestors: lineage.Ancestors})
		}
	}
	for uuid, bytes := range used {
		// Exclusive scheduling must fail closed even while a compute process is
		// between kernels (and therefore reports 0% utilization). Operators can
		// explicitly allow unattributed activity in the manifest when desired.
		if bytes > 1024*1024*1024 && !seenProcess[uuid] {
			resourceID := ""
			for _, entry := range byIndex {
				if entry.uuid == uuid {
					resourceID = entry.resource
					break
				}
			}
			evidence, _ := json.Marshal(map[string]any{"used_memory_bytes": bytes, "gpu_uuid": uuid, "reason": "significant_vram_without_attributed_process"})
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
		// The store qualifies a shared GPU's resource ID with @node to avoid
		// collisions. Observe still returns the provider-local ID; match the
		// physical UUID within this node/provider instead of that store key.
		if item.StableIdentity == r.StableIdentity && item.NodeID == r.NodeID &&
			item.ProviderID == r.ProviderID && item.Kind == r.Kind {
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
