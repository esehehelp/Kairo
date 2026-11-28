package provider

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
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
	// GraphicsExplainVRAM: memory on a GPU that shows graphics processes
	// (pmon type G or C+G) is theirs, not unattributed activity. nil means
	// true on Windows, where the desktop's own processes (the compositor, a
	// browser) hold that memory and WDDM reports no per-process figure, and
	// false elsewhere, where memory without a visible process is what another
	// container's work looks like.
	GraphicsExplainVRAM *bool
	// GraphicsVRAMAllowance bounds what GraphicsExplainVRAM explains: GPU
	// memory in use beyond it is unattributed activity although graphics
	// processes show (zero: defaultGraphicsVRAMAllowance, 3 GiB).
	GraphicsVRAMAllowance int64
	// Owner tells its owner's use of a GPU (owneruse.go); nil tells none.
	Owner *OwnerUse
	// KairoVRAM reads the GPU memory Kairo's work holds where nvidia-smi
	// shows no process for it, by the GPU's PCI address (pciAddress): on
	// Windows, the wslc VM where Kairo's containers run (GPU
	// paravirtualization: the host's nvidia-smi lists none of the VM's
	// processes, only the memory they hold). That memory is Kairo's, not
	// unattributed activity. nil means the wslc VM's on Windows, none
	// elsewhere.
	KairoVRAM func() (map[string]int64, error)

	query atomic.Int32 // the index in nvidiaQueries of the query this driver accepts
	vm    wslcVRAM
	// owned are the readings of each GPU its owner's use is told by
	// (owneruse.go)
	owned gpuOwnerReadings
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
		"pcie.link.gen.gpucurrent", "pcie.link.gen.max", "pcie.link.width.current", "pcie.link.width.max", "utilization.memory", "fan.speed", "pci.bus_id"),
	append(slices.Clone(nvidiaInventoryFields), "power.draw", "enforced.power.limit", "power.default_limit", "power.min_limit", "power.max_limit",
		"clocks.sm", "clocks.mem", "clocks.max.sm", "clocks.max.mem", "pstate", "clocks_throttle_reasons.active",
		"pcie.link.gen.current", "pcie.link.gen.max", "pcie.link.width.current", "pcie.link.width.max", "utilization.memory", "fan.speed", "pci.bus_id"),
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

// pciAddress is a GPU's PCI address as Kairo matches it across sources:
// "bus:device.function" in lower-case hexadecimal, without the domain.
func pciAddress(bus, device, function uint64) string {
	return fmt.Sprintf("%02x:%02x.%x", bus, device, function)
}

// nvidiaPCIAddress is pciAddress of nvidia-smi's pci.bus_id
// ("00000000:2B:00.0").
func nvidiaPCIAddress(gpu map[string]string) (string, bool) {
	v, ok := nvidiaValue(gpu, "pci.bus_id")
	if !ok {
		return "", false
	}
	parts := strings.Split(v, ":")
	if len(parts) < 2 {
		return "", false
	}
	device, function, ok := strings.Cut(parts[len(parts)-1], ".")
	if !ok {
		return "", false
	}
	b, err1 := strconv.ParseUint(parts[len(parts)-2], 16, 8)
	d, err2 := strconv.ParseUint(device, 16, 8)
	f, err3 := strconv.ParseUint(function, 16, 8)
	if err1 != nil || err2 != nil || err3 != nil {
		return "", false
	}
	return pciAddress(b, d, f), true
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
	// kairo: what Kairo's work holds of each GPU without a process nvidia-smi
	// shows (KairoVRAM); none when it cannot be read (the memory then stays
	// unattributed: fail closed)
	kairo := map[string]int64{}
	readKairo := p.KairoVRAM
	if readKairo == nil {
		readKairo = p.vm.read
	}
	kairoByAddress, _ := readKairo()
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
		if address, ok := nvidiaPCIAddress(gpu); ok && kairoByAddress[address] > 0 {
			kairo[uuid] = kairoByAddress[address]
		}
		binding, _ := json.Marshal(map[string]string{"cuda_index": index, "uuid": uuid})
		attrs, _ := json.Marshal(map[string]any{"uuid": uuid})
		snap.Resources = append(snap.Resources, store.ResourceInstance{ID: resourceID, NodeID: p.NodeID, ProviderID: p.ProviderID, Kind: "gpu", StableIdentity: uuid, Binding: binding, Attributes: attrs, AdminState: "enabled"})
		stamp, valid := t.Format(time.RFC3339Nano), t.Add(ttl).Format(time.RFC3339Nano)
		evidence, _ := json.Marshal(map[string]string{"source": "nvidia-smi"})
		if kairo[uuid] > 0 {
			evidence, _ = json.Marshal(map[string]any{"source": "nvidia-smi", "kairo_vm_bytes": kairo[uuid]})
		}
		snap.Observations = append(snap.Observations, store.Observation{ResourceID: resourceID, ObservedAt: stamp, ValidUntil: valid, TotalBytes: &total, FreeBytes: &free,
			Utilization: nvidiaFloat(gpu, "utilization.gpu"), TemperatureC: nvidiaFloat(gpu, "temperature.gpu"), Evidence: evidence, Device: deviceSample(gpu)})
	}
	// pmon exposes the C/G process type. query-compute-apps on WDDM drivers may
	// include ordinary desktop graphics clients, which must not claim a GPU.
	// -s um: each process's SM share (its owner's use, owneruse.go) and memory
	processes, processErr := p.run(ctx, "pmon", "-c", "1", "-s", "um")
	if errors.Is(processErr, context.DeadlineExceeded) {
		// Without the process list the GPUs' claims are unknown: fail the
		// observation rather than record them as unclaimed.
		return Snapshot{}, fmt.Errorf("nvidia-smi pmon: %w", processErr)
	}
	seenProcess := map[string]bool{}
	// graphics: GPUs on which a graphics process was seen (see GraphicsExplainVRAM).
	graphics := map[string]bool{}
	// owner: the graphics processes using each GPU's SMs (owneruse.go)
	owner := map[string][]ownerProcess{}
	if processErr == nil {
		cols := pmonColumns{gpu: 0, pid: 1, kind: 2, sm: -1, fb: 3, command: 4}
		header := false
		for _, line := range strings.Split(string(processes), "\n") {
			if trimmed := strings.TrimSpace(line); strings.HasPrefix(trimmed, "#") {
				if !header {
					cols, header = parsePmonHeader(trimmed, cols), true
				}
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 4 || len(fields) <= cols.fb || len(fields) <= cols.kind {
				continue
			}
			entry, ok := byIndex[fields[cols.gpu]]
			pid, parseErr := strconv.Atoi(fields[cols.pid])
			processType := fields[cols.kind]
			name := fields[len(fields)-1]
			if cols.command < len(fields) {
				name = strings.Join(fields[cols.command:], " ")
			}
			fb, _ := strconv.Atoi(fields[cols.fb])
			if ok && parseErr == nil && pid > 0 && strings.Contains(processType, "G") {
				graphics[entry.uuid] = true
				// not Kairo's: on Windows its work runs in the wslc VM (vmmem*)
				if sm, err := strconv.ParseFloat(fieldAt(fields, cols.sm), 64); err == nil && sm > 0 && !strings.HasPrefix(strings.ToLower(name), "vmmem") {
					owner[entry.uuid] = append(owner[entry.uuid], ownerProcess{PID: pid, Name: name, SM: sm})
				}
			}
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
		// What Kairo's work holds in the wslc VM is Kairo's (KairoVRAM).
		// Graphics processes explain a desktop's share of the rest, not more:
		// beyond the allowance the memory is someone's work (a game, a
		// D3D/Vulkan compute job) that pmon shows only as graphics.
		bytes -= min(bytes, kairo[uuid])
		explained := graphics[uuid] && p.graphicsExplainVRAM() && bytes <= p.graphicsVRAMAllowance()
		if bytes > 1024*1024*1024 && !seenProcess[uuid] && !explained {
			resourceID := ""
			for _, entry := range byIndex {
				if entry.uuid == uuid {
					resourceID = entry.resource
					break
				}
			}
			detail := map[string]any{"used_memory_bytes": bytes, "gpu_uuid": uuid, "reason": "significant_vram_without_attributed_process"}
			if kairo[uuid] > 0 {
				detail["kairo_vm_bytes"] = kairo[uuid]
			}
			evidence, _ := json.Marshal(detail)
			snap.Claims = append(snap.Claims, store.ExternalClaim{ID: "claim-" + resourceID + "-unattributed", ResourceID: resourceID, ClaimKind: "unattributed_activity", Evidence: evidence})
		}
	}
	if p.Owner.observed() && processErr == nil {
		// each GPU's reading, with or without graphics on it: the mean over
		// the window is the owner's use
		present, _ := p.Owner.present()
		threshold := p.Owner.gpuThreshold(present)
		now := p.Owner.now()
		for _, entry := range byIndex {
			sum, top, ok := p.owned.add(entry.uuid, now, owner[entry.uuid])
			if ok && sum >= threshold {
				snap.Claims = append(snap.Claims, ownerClaim(entry.resource, map[string]any{"source": "observed", "gpu_uuid": entry.uuid, "sm_percent": sum,
					"threshold_percent": threshold, "owner_present": present, "window_seconds": ownerWindow.Seconds(), "processes": top}))
			}
		}
	}
	return snap, nil
}

// pmonColumns are where pmon's columns are, by its header.
type pmonColumns struct{ gpu, pid, kind, sm, fb, command int }

// parsePmonHeader reads the columns from pmon's first header line ("# gpu
// pid type sm mem ... fb ... command"); a column it does not name keeps
// def's place.
func parsePmonHeader(line string, def pmonColumns) pmonColumns {
	c := def
	for i, name := range strings.Fields(strings.TrimPrefix(line, "#")) {
		switch name {
		case "gpu":
			c.gpu = i
		case "pid":
			c.pid = i
		case "type":
			c.kind = i
		case "sm":
			c.sm = i
		case "fb":
			c.fb = i
		case "command":
			c.command = i
		}
	}
	return c
}

// fieldAt is fields[i], or "" outside them.
func fieldAt(fields []string, i int) string {
	if i < 0 || i >= len(fields) {
		return ""
	}
	return fields[i]
}

// defaultGraphicsVRAMAllowance is the GPU memory graphics processes explain
// when GraphicsVRAMAllowance is zero: a Windows desktop with browsers holds
// 1-2 GiB.
const defaultGraphicsVRAMAllowance = int64(3) << 30

func (p *NVIDIA) graphicsVRAMAllowance() int64 {
	if p.GraphicsVRAMAllowance > 0 {
		return p.GraphicsVRAMAllowance
	}
	return defaultGraphicsVRAMAllowance
}

func (p *NVIDIA) graphicsExplainVRAM() bool {
	if p.GraphicsExplainVRAM != nil {
		return *p.GraphicsExplainVRAM
	}
	return runtime.GOOS == "windows"
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
