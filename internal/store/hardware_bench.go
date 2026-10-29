package store

// Hardware benches: `kairo hardware bench HOST DEVICE` measures how a device
// performs, and what energy it uses, at each setting of a grid, against
// references at its defaults. A run is an ordinary Task of the Kairo-owned
// project "kairo" (queue "hardware-bench", named after the run) at the lowest
// priority, preemptible and checkpointable, holding the device exclusively:
// it never displaces work, work never runs beside it, and work that needs
// the device suspends it at its next point. The task's continuation carries
// on the same run (benchTaskExecutionTx): the kinds it finished stay, the one
// it was measuring is measured again. Its argv is the bench driver (`kairo
// hardware bench-run`), which asks for its settings and reports its
// measurements through the worker API with its attempt token; it can reach
// only its own run (BenchOfAttempt) and set only its own lease's bench
// setting, which goes with the lease. What a run measures are facts about
// this device, compared only with its own references and earlier runs.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"kairo/internal/id"
	"kairo/internal/machine"
	"kairo/internal/orchestration"
)

// Bench tasks run in this project and queue, at BenchPriority.
const (
	BenchProject = "kairo"
	BenchQueue   = "hardware-bench"
	// BenchPriority is below anything an operator would submit: a bench waits
	// for every other execution, is never a reason to suspend one, and is
	// suspended for any of them that needs its device.
	BenchPriority = -1_000_000
)

// ErrBenchConflict: the device already has a live bench, the network to bench
// is in use, or the run is not in a state to take the request.
var ErrBenchConflict = errors.New("hardware bench conflict")

// Bench device classes.
const (
	BenchGPU     = "gpu"
	BenchCPU     = "cpu"
	BenchStorage = "storage"
	BenchNetwork = "network"
)

// Device ids of the classes that are not host devices: a filesystem of one
// of the host's nodes, and the network path to another node.
const (
	BenchDiskPrefix = "disk:"
	BenchNetPrefix  = "net:"
)

// Sweeps: what a bench varies for a kind.
const (
	// SweepClocks: the GPU's graphics clock cap, and its memory clock at two
	// of those caps.
	SweepClocks = "clocks"
	// SweepClocksPower: SweepClocks and the power limit.
	SweepClocksPower = "clocks+power"
	// SweepFrequency: the CPU's maximum frequency (or performance) cap.
	SweepFrequency = "frequency"
)

// BenchKind is one workload a bench of a class measures. A kind without a
// Sweep is measured at the device's defaults only.
type BenchKind struct {
	Name  string `json:"name"`
	Class string `json:"class"`
	Sweep string `json:"sweep,omitempty"`
}

var benchKinds = []BenchKind{
	{"gemm", BenchGPU, SweepClocksPower},
	{"copy", BenchGPU, SweepClocks},
	{"gemv-b1", BenchGPU, SweepClocks},
	{"gemv-b8", BenchGPU, SweepClocks},
	{"pcie-h2d", BenchGPU, ""},
	{"pcie-d2h", BenchGPU, ""},
	{"pcie-h2d-latency", BenchGPU, ""},
	{"pcie-d2h-latency", BenchGPU, ""},
	{"cpu-compute", BenchCPU, SweepFrequency},
	{"cpu-single", BenchCPU, SweepFrequency},
	{"cpu-stream", BenchCPU, SweepFrequency},
	{"disk-seq-read", BenchStorage, ""},
	{"disk-seq-write", BenchStorage, ""},
	{"disk-rand-read", BenchStorage, ""},
	{"disk-rand-read-qd1", BenchStorage, ""},
	{"disk-rand-write", BenchStorage, ""},
	{"disk-fsync", BenchStorage, ""},
	{"disk-sustained", BenchStorage, ""},
	{"net-latency", BenchNetwork, ""},
	{"net-bandwidth-send", BenchNetwork, ""},
	{"net-bandwidth-recv", BenchNetwork, ""},
}

// BenchKinds are the kinds of a class (every class when class is empty).
func BenchKinds(class string) []BenchKind {
	var out []BenchKind
	for _, k := range benchKinds {
		if class == "" || k.Class == class {
			out = append(out, k)
		}
	}
	return out
}

// FindBenchKind is the kind named name.
func FindBenchKind(name string) (BenchKind, bool) {
	for _, k := range benchKinds {
		if k.Name == name {
			return k, true
		}
	}
	return BenchKind{}, false
}

// selectBenchKinds resolves --kinds: each name is a kind of the class or a
// group of them by prefix ("pcie", "gemv"); none is every kind of the class.
func selectBenchKinds(class string, names []string) ([]string, error) {
	all := BenchKinds(class)
	if len(names) == 0 {
		out := make([]string, len(all))
		for i, k := range all {
			out[i] = k.Name
		}
		return out, nil
	}
	chosen := map[string]bool{}
	for _, name := range names {
		name = strings.TrimSpace(name)
		found := false
		for _, k := range all {
			if k.Name == name || strings.HasPrefix(k.Name, name+"-") {
				chosen[k.Name], found = true, true
			}
		}
		if !found {
			valid := make([]string, len(all))
			for i, k := range all {
				valid[i] = k.Name
			}
			return nil, fmt.Errorf("%q is not a %s bench kind (one of %s)", name, class, strings.Join(valid, ", "))
		}
	}
	var out []string
	for _, k := range all {
		if chosen[k.Name] {
			out = append(out, k.Name)
		}
	}
	return out, nil
}

// NodeBench is what a node's agent (or the daemon, for its own node) reports
// of the bench programs there, from the [bench] table of its configuration:
// node attributes under "bench".
type NodeBench struct {
	// Kairo is the kairo binary that runs the bench driver.
	Kairo string `json:"kairo,omitempty"`
	// GPU is kairo-bench-gpu; a node without it takes no GPU bench.
	GPU string `json:"gpu,omitempty"`
	// CWD is the bench's working directory (default: Kairo's directory).
	CWD string `json:"cwd,omitempty"`
	// ExecutorLabels pick the node's executor that runs benches.
	ExecutorLabels map[string]string `json:"executor_labels,omitempty"`
}

// BenchRequest asks for a bench of a host's device.
type BenchRequest struct {
	HostID   string `json:"-"`
	DeviceID string `json:"-"`
	// Kinds limits the kinds measured (default every kind of the class).
	Kinds []string `json:"kinds,omitempty"`
	// Node is the host's node to run on (default: the first that can).
	Node   string `json:"node,omitempty"`
	DryRun bool   `json:"dry_run,omitempty"`
	Actor  string `json:"actor,omitempty"`
}

// BenchVersion is what ran a kind: the program, its workload version and its
// build.
type BenchVersion struct {
	Program string `json:"program"`
	Version int    `json:"version"`
	Build   string `json:"build,omitempty"`
}

// Fingerprint is how busy a workload keeps a GPU: its mean utilization
// (NVML's utilization.gpu and utilization.memory, in percent).
type Fingerprint struct {
	UtilGPUPct float64 `json:"util_gpu_pct"`
	UtilMemPct float64 `json:"util_mem_pct"`
}

// BenchReference is a kind's measurements at the defaults: the start, middle
// and end references of a sweep (one for a kind measured once), their mean
// and their spread ((max-min)/mean). Valid false: the references disagree
// (spread over 1%) or one of them was invalid, so the kind's curve is not
// trusted.
type BenchReference struct {
	Unit          string       `json:"unit"`
	WorkPerSecond []float64    `json:"work_per_second"`
	Mean          float64      `json:"mean"`
	Spread        float64      `json:"spread"`
	PowerW        *float64     `json:"power_w,omitempty"`
	JoulesPerWork *float64     `json:"joules_per_work,omitempty"`
	Fingerprint   *Fingerprint `json:"fingerprint,omitempty"`
	Valid         bool         `json:"valid"`
	Reason        string       `json:"reason,omitempty"`
}

// BenchMetrics are what one measurement window showed. Work is in Unit, the
// kind's primary unit; RelPerf is WorkPerSecond over the kind's reference
// mean; the energy comes from the GPU's energy counter (EnergySource "nvml")
// or the host's CPU package samples ("cpu_package"). Clocks, temperature and
// utilization are means over the window, EventReasons the OR of every
// sample's clock event reasons.
type BenchMetrics struct {
	Unit            string   `json:"unit"`
	Begin           string   `json:"begin"`
	End             string   `json:"end"`
	Seconds         float64  `json:"seconds"`
	Work            float64  `json:"work"`
	WorkPerSecond   float64  `json:"work_per_second"`
	RelPerf         *float64 `json:"rel_perf,omitempty"`
	PowerW          *float64 `json:"power_w,omitempty"`
	Joules          *float64 `json:"joules,omitempty"`
	JoulesPerWork   *float64 `json:"joules_per_work,omitempty"`
	EnergySource    string   `json:"energy_source,omitempty"`
	SMClockMHz      *float64 `json:"sm_clock_mhz,omitempty"`
	MemClockMHz     *float64 `json:"mem_clock_mhz,omitempty"`
	TemperatureC    *float64 `json:"temperature_c,omitempty"`
	TemperatureMaxC *float64 `json:"temperature_max_c,omitempty"`
	EventReasons    *int64   `json:"event_reasons,omitempty"`
	UtilGPUPct      *float64 `json:"util_gpu_pct,omitempty"`
	UtilMemPct      *float64 `json:"util_mem_pct,omitempty"`
	LatencyP50NS    *int64   `json:"latency_p50_ns,omitempty"`
	LatencyP99NS    *int64   `json:"latency_p99_ns,omitempty"`
}

// BenchPoint is one measurement of a run: a kind at a setting (the zero
// setting: the defaults). Flags say why a point is invalid ("not applied",
// "thermal", "other process", ...) or note what was seen.
type BenchPoint struct {
	Seq        int            `json:"seq"`
	Kind       string         `json:"kind"`
	Phase      string         `json:"phase"`
	Settings   DeviceSettings `json:"settings"`
	Metrics    BenchMetrics   `json:"metrics"`
	Valid      bool           `json:"valid"`
	Flags      []string       `json:"flags"`
	MeasuredAt string         `json:"measured_at"`
}

// Point phases.
const (
	PhaseReference = "reference"
	PhaseSweep     = "sweep"
	PhaseMeasure   = "measure"
)

// Bench run states: waiting (for its first execution to start, or, once
// suspended, for its continuation), running, then finished (every kind
// valid), partial (some kinds valid: their curves are used, Reason gives the
// others'), invalid (no kind valid) or failed (the task ended without the
// driver finishing the run).
const (
	BenchWaiting  = "waiting"
	BenchRunning  = "running"
	BenchFinished = "finished"
	BenchPartial  = "partial"
	BenchInvalid  = "invalid"
	BenchFailed   = "failed"
)

// BenchRun is one bench of a device. ExecutionID is its latest execution;
// References are each kind's, recorded as the kind is done.
type BenchRun struct {
	ID          string   `json:"id"`
	HostID      string   `json:"host_id"`
	DeviceID    string   `json:"device_id"`
	Class       string   `json:"class"`
	Kinds       []string `json:"kinds"`
	NodeID      string   `json:"node_id"`
	PeerNodeID  *string  `json:"peer_node_id,omitempty"`
	Target      string   `json:"target,omitempty"`
	ExecutionID string   `json:"execution_id"`
	State       string   `json:"state"`
	RequestedBy string   `json:"requested_by"`
	CreatedAt   string   `json:"created_at"`
	StartedAt   *string  `json:"started_at,omitempty"`
	EndedAt     *string  `json:"ended_at,omitempty"`
	// DriverBuild is the bench driver's build; Versions what ran each kind;
	// DriverVersion the GPU driver's.
	DriverBuild      *string                   `json:"driver_build,omitempty"`
	Versions         map[string]BenchVersion   `json:"versions,omitempty"`
	DriverVersion    *string                   `json:"driver_version,omitempty"`
	Device           json.RawMessage           `json:"device,omitempty"`
	IdleTemperatureC *float64                  `json:"idle_temperature_c,omitempty"`
	References       map[string]BenchReference `json:"references,omitempty"`
	Valid            *bool                     `json:"valid,omitempty"`
	Reason           *string                   `json:"reason,omitempty"`
	Points           []BenchPoint              `json:"points,omitempty"`
}

// Live reports whether the run has not ended.
func (r BenchRun) Live() bool { return r.State == BenchWaiting || r.State == BenchRunning }

// Ended reports whether the run ended with results (finished, partial or
// invalid), not failed or live.
func (r BenchRun) Ended() bool {
	return r.State == BenchFinished || r.State == BenchPartial || r.State == BenchInvalid
}

// BenchPlan is a created (or, for a dry run, planned) bench: the run and the
// execution of its task that carries it out first.
type BenchPlan struct {
	Run       BenchRun      `json:"run"`
	Execution ExecutionSpec `json:"execution"`
	DryRun    bool          `json:"dry_run"`
}

const benchRunColumns = `id,host_id,device_id,class,kinds_json,node_id,peer_node_id,target,execution_id,state,requested_by,created_at,started_at,ended_at,
	driver_build,versions_json,driver_version,device_json,idle_temperature_c,references_json,valid,reason`

func scanBenchRun(row interface{ Scan(...any) error }) (BenchRun, error) {
	var r BenchRun
	var kinds string
	var versions, device, references sql.NullString
	var valid sql.NullBool
	if err := row.Scan(&r.ID, &r.HostID, &r.DeviceID, &r.Class, &kinds, &r.NodeID, &r.PeerNodeID, &r.Target, &r.ExecutionID, &r.State, &r.RequestedBy, &r.CreatedAt, &r.StartedAt, &r.EndedAt,
		&r.DriverBuild, &versions, &r.DriverVersion, &device, &r.IdleTemperatureC, &references, &valid, &r.Reason); err != nil {
		return r, err
	}
	if err := json.Unmarshal([]byte(kinds), &r.Kinds); err != nil {
		return r, err
	}
	if versions.Valid {
		if err := json.Unmarshal([]byte(versions.String), &r.Versions); err != nil {
			return r, err
		}
	}
	if device.Valid {
		r.Device = json.RawMessage(device.String)
	}
	if references.Valid {
		if err := json.Unmarshal([]byte(references.String), &r.References); err != nil {
			return r, err
		}
	}
	if valid.Valid {
		r.Valid = &valid.Bool
	}
	return r, nil
}

// CreateBenchRun creates a bench of a host's device and submits its
// execution (with DryRun, only plans it). A GPU or CPU bench sweeps settings,
// so the device must be controllable (control = true on a host with a
// controller that has reported what it supports) and have no manual setting;
// a storage or network bench measures the defaults and needs no controller.
// One live run per device.
//
// The run's task (project kairo, queue hardware-bench, named after the run)
// is declared as `project apply` declares one, and its first execution
// submitted at once, in the transaction that creates the run.
func (s *Store) CreateBenchRun(ctx context.Context, req BenchRequest) (BenchPlan, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return BenchPlan{}, err
	}
	defer tx.Rollback()
	plan, declaration, err := planBenchTx(ctx, tx, req)
	if err != nil || req.DryRun {
		return plan, err
	}
	run := &plan.Run
	if _, err = applyProjectTx(ctx, tx, declaration); err != nil {
		return BenchPlan{}, err
	}
	scopes, err := ensureScopePathTx(ctx, tx, ScopePath{Project: BenchProject, Queue: BenchQueue, Task: run.ID})
	if err != nil {
		return BenchPlan{}, err
	}
	template, err := json.Marshal(declaration.Manifest.Tasks[0].Execution)
	if err != nil {
		return BenchPlan{}, err
	}
	task := orchestrationTaskRow{taskScopeID: scopes.Task.ID, name: run.ID, queue: BenchQueue, policyID: "RunToCompletion", policyVersion: 1, template: string(template), project: BenchProject}
	// The decision the task's reconciliation would plan for it.
	decision, err := planTaskExecutionDecision(ctx, tx, task, 0, "initial", map[string]any{"dependencies_succeeded": true}, nil)
	if err != nil {
		return BenchPlan{}, err
	}
	normalized, executionID, err := submitOrchestrationDecisionTx(ctx, tx, decision)
	if err == nil && executionID == "" {
		err = fmt.Errorf("%w: the task of bench run %s was already submitted", ErrBenchConflict, run.ID)
	}
	if err != nil {
		return BenchPlan{}, err
	}
	plan.Execution = normalized
	run.ExecutionID, run.State, run.CreatedAt = executionID, BenchWaiting, now()
	kinds, err := json.Marshal(run.Kinds)
	if err != nil {
		return BenchPlan{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO bench_runs(id,host_id,device_id,class,kinds_json,node_id,peer_node_id,target,execution_id,state,requested_by,created_at,task_scope_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		run.ID, run.HostID, run.DeviceID, run.Class, string(kinds), run.NodeID, run.PeerNodeID, run.Target, executionID, run.State, run.RequestedBy, run.CreatedAt, scopes.Task.ID); err != nil {
		return BenchPlan{}, err
	}
	if err = linkBenchExecutionsTx(ctx, tx, run.ID, executionID); err != nil {
		return BenchPlan{}, err
	}
	if err = appendCoordinationEventTx(ctx, tx, "hardware_bench_requested", nil, "bench_run", run.ID, nil, map[string]any{
		"host_id": run.HostID, "device_id": run.DeviceID, "class": run.Class, "kinds": run.Kinds, "node_id": run.NodeID, "execution_id": executionID, "actor": run.RequestedBy}); err != nil {
		return BenchPlan{}, err
	}
	return plan, tx.Commit()
}

// linkBenchExecutionsTx records an execution of a run: the leader's, and a
// gang's members.
func linkBenchExecutionsTx(ctx context.Context, tx *sql.Tx, runID, executionID string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO bench_run_executions(execution_id,run_id,rank) VALUES($1,$2,0) ON CONFLICT DO NOTHING`, executionID, runID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO bench_run_executions(execution_id,run_id,rank) SELECT m.execution_id,$1,m.rank FROM gang_members m JOIN execution_gangs g ON g.id=m.gang_id
		WHERE g.leader_execution_id=$2 AND m.rank>0 ON CONFLICT DO NOTHING`, runID, executionID)
	return err
}

// benchTaskExecutionTx: an execution was submitted for task taskScopeID (its
// continuation, or a restart); when that is a bench run's task, the run
// carries on in it. A live run waits for it: the kinds it has a reference of
// are done, and the points of the others (the kind its previous attempt was
// measuring when it was suspended) are dropped, since that kind is measured
// again from the start (event hardware_bench_continued). An ended run is not
// carried on: its driver finds it ended.
func benchTaskExecutionTx(ctx context.Context, tx *sql.Tx, taskScopeID, executionID, reason string) error {
	run, err := scanBenchRun(tx.QueryRowContext(ctx, `SELECT `+benchRunColumns+` FROM bench_runs WHERE task_scope_id=$1`, taskScopeID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = linkBenchExecutionsTx(ctx, tx, run.ID, executionID); err != nil || !run.Live() || run.ExecutionID == executionID {
		return err
	}
	done := run.doneKinds()
	body, err := json.Marshal(done)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE bench_runs SET execution_id=$1,state=$2 WHERE id=$3`, executionID, BenchWaiting, run.ID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM bench_points WHERE run_id=$1 AND kind NOT IN(SELECT jsonb_array_elements_text(($2)::jsonb))`, run.ID, string(body)); err != nil {
		return err
	}
	return appendCoordinationEventTx(ctx, tx, "hardware_bench_continued", nil, "bench_run", run.ID, nil, map[string]any{
		"host_id": run.HostID, "device_id": run.DeviceID, "execution_id": executionID, "previous_execution_id": run.ExecutionID, "reason": reason, "kinds_done": done})
}

// doneKinds are the run's kinds it has a reference of, in its order.
func (r BenchRun) doneKinds() []string {
	done := []string{}
	for _, kind := range r.Kinds {
		if _, ok := r.References[kind]; ok {
			done = append(done, kind)
		}
	}
	return done
}

// benchClass is the class a device id names on a host.
func benchClass(deviceID string) string {
	switch {
	case deviceID == machine.CPU:
		return BenchCPU
	case strings.HasPrefix(deviceID, BenchDiskPrefix):
		return BenchStorage
	case strings.HasPrefix(deviceID, BenchNetPrefix):
		return BenchNetwork
	}
	return BenchGPU
}

// planBenchTx is the run a request asks for, its task's declaration, and the
// execution that declaration submits first.
func planBenchTx(ctx context.Context, tx *sql.Tx, req BenchRequest) (BenchPlan, orchestration.Validated, error) {
	run, template, err := planBenchRunTx(ctx, tx, req)
	if err != nil {
		return BenchPlan{}, orchestration.Validated{}, err
	}
	declaration, err := orchestration.Normalize(orchestration.Manifest{SchemaVersion: orchestration.SchemaVersion, Project: orchestration.Project{Name: BenchProject},
		Tasks: []orchestration.Task{{Name: run.ID, Queue: BenchQueue, DependsOn: []string{}, Policy: orchestration.PolicyRunToCompletion, Execution: template}}})
	if err != nil {
		return BenchPlan{}, orchestration.Validated{}, fmt.Errorf("bench task: %w", err)
	}
	execution, err := taskExecutionSpec(ScopePath{Project: BenchProject, Queue: BenchQueue, Task: run.ID}, declaration.Manifest.Tasks[0].Execution, run.ID, nil)
	if err != nil {
		return BenchPlan{}, orchestration.Validated{}, err
	}
	return BenchPlan{Run: run, Execution: execution, DryRun: req.DryRun}, declaration, nil
}

// planBenchRunTx is the run a request asks for and its task's execution.
func planBenchRunTx(ctx context.Context, tx *sql.Tx, req BenchRequest) (BenchRun, orchestration.Execution, error) {
	spec, err := declaredSpec(ctx, tx)
	if err != nil {
		return BenchRun{}, orchestration.Execution{}, err
	}
	var host *machine.Host
	for i := range spec.Hosts {
		if spec.Hosts[i].ID == req.HostID {
			host = &spec.Hosts[i]
		}
	}
	if host == nil {
		return BenchRun{}, orchestration.Execution{}, fmt.Errorf("%w: host %s is not declared", ErrNotFound, req.HostID)
	}
	class := benchClass(req.DeviceID)
	kinds, err := selectBenchKinds(class, req.Kinds)
	if err != nil {
		return BenchRun{}, orchestration.Execution{}, err
	}
	if req.Node != "" && !slices.Contains(host.Nodes, req.Node) {
		return BenchRun{}, orchestration.Execution{}, fmt.Errorf("node %s is not on host %s (its nodes: %s)", req.Node, host.ID, strings.Join(host.Nodes, ", "))
	}
	var live int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM bench_runs WHERE host_id=$1 AND device_id=$2 AND state IN('waiting','running')`, req.HostID, req.DeviceID).Scan(&live); err != nil {
		return BenchRun{}, orchestration.Execution{}, err
	}
	if live != 0 {
		return BenchRun{}, orchestration.Execution{}, fmt.Errorf("%w: device %s of host %s has a bench waiting or running (kairo hardware profile %s)", ErrBenchConflict, req.DeviceID, req.HostID, req.HostID)
	}
	run := BenchRun{ID: id.New("bnc"), HostID: req.HostID, DeviceID: req.DeviceID, Class: class, Kinds: kinds, RequestedBy: req.Actor}
	// Suspended for any other work (at its next point), it resumes from the
	// kind it was measuring.
	execution := orchestration.Execution{SchemaVersion: 2, Priority: BenchPriority, Checkpointable: true, Preemptible: true}
	// candidates: the host's nodes that can run the bench, in declared order.
	candidates := func(need func(nodeID string, bench NodeBench) (bool, error)) ([]string, map[string]NodeBench, error) {
		var nodes []string
		configs := map[string]NodeBench{}
		for _, nodeID := range host.Nodes {
			if req.Node != "" && nodeID != req.Node {
				continue
			}
			bench, err := nodeBenchTx(ctx, tx, nodeID)
			if err != nil {
				return nil, nil, err
			}
			if bench.Kairo == "" {
				continue
			}
			ok, err := need(nodeID, bench)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				nodes = append(nodes, nodeID)
				configs[nodeID] = bench
			}
		}
		return nodes, configs, nil
	}
	switch class {
	case BenchGPU, BenchCPU:
		policy, _, err := devicePolicyTx(ctx, tx, req.HostID, req.DeviceID)
		if err != nil {
			return BenchRun{}, orchestration.Execution{}, err
		}
		if !policy.Control {
			return BenchRun{}, orchestration.Execution{}, fmt.Errorf("device %s of host %s is declared control = false: it is never benched", req.DeviceID, req.HostID)
		}
		if policy.Controller == nil {
			return BenchRun{}, orchestration.Execution{}, fmt.Errorf("host %s declares no controller: a %s bench sweeps settings, which only a controller carries out", req.HostID, class)
		}
		var caps sql.NullString
		if err = tx.QueryRowContext(ctx, `SELECT capabilities_json FROM device_state WHERE host_id=$1 AND device_id=$2`, req.HostID, req.DeviceID).Scan(&caps); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return BenchRun{}, orchestration.Execution{}, err
		}
		if !caps.Valid {
			return BenchRun{}, orchestration.Execution{}, fmt.Errorf("the controller of host %s (%s) has not reported what device %s supports yet: start it (kairo controller) first", req.HostID, *policy.Controller, req.DeviceID)
		}
		settings, err := deviceSettingsTx(ctx, tx, req.HostID, req.DeviceID)
		if err != nil {
			return BenchRun{}, orchestration.Execution{}, err
		}
		for _, setting := range settings {
			if setting.Owner.Kind == OwnerManual {
				return BenchRun{}, orchestration.Execution{}, fmt.Errorf("device %s of host %s has a manual setting (%s): a bench measures the device's own settings; kairo hardware reset %s %s first", req.DeviceID, req.HostID, setting.Settings, req.HostID, req.DeviceID)
			}
		}
	}
	var nodes []string
	var configs map[string]NodeBench
	switch class {
	case BenchGPU:
		nodes, configs, err = candidates(func(nodeID string, bench NodeBench) (bool, error) {
			var declared int
			err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM machine_gpus WHERE uuid=$1 AND node_id=$2`, req.DeviceID, nodeID).Scan(&declared)
			return declared != 0 && bench.GPU != "", err
		})
		if err != nil {
			return BenchRun{}, orchestration.Execution{}, err
		}
		if len(nodes) == 0 {
			return BenchRun{}, orchestration.Execution{}, fmt.Errorf("no node of host %s that holds %s reports a GPU bench binary and kairo ([bench] gpu_binary and kairo_binary of its agent)", req.HostID, req.DeviceID)
		}
		run.NodeID = nodes[0]
		execution.Exclusive = []orchestration.ExclusiveRequest{{Kind: "gpu", Count: 1, Device: req.DeviceID}}
		execution.Capacity = orchestration.CapacityRequest{CPUMillis: 1000, RAMBytes: 2 << 30}
	case BenchCPU:
		var cpuMillis int64
		nodes, configs, err = candidates(func(nodeID string, _ NodeBench) (bool, error) {
			millis, err := nodeCPUMillisTx(ctx, tx, nodeID)
			if millis > 0 && cpuMillis == 0 {
				cpuMillis = millis
			}
			return millis > 0, err
		})
		if err != nil {
			return BenchRun{}, orchestration.Execution{}, err
		}
		if len(nodes) == 0 {
			return BenchRun{}, orchestration.Execution{}, fmt.Errorf("no node of host %s reports kairo ([bench] kairo_binary of its agent) and its CPU", req.HostID)
		}
		run.NodeID = nodes[0]
		// The whole node's CPU, none of it lent: nothing else runs beside it.
		execution.Capacity = orchestration.CapacityRequest{CPUMillis: cpuMillis, CPUMillisMin: cpuMillis, RAMBytes: 4 << 30}
	case BenchStorage:
		filesystem := strings.TrimPrefix(req.DeviceID, BenchDiskPrefix)
		nodes, configs, err = candidates(func(nodeID string, _ NodeBench) (bool, error) {
			var registered int
			err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_instances WHERE node_id=$1 AND kind='disk' AND stable_identity=$2`, nodeID, filesystem).Scan(&registered)
			return registered != 0, err
		})
		if err != nil {
			return BenchRun{}, orchestration.Execution{}, err
		}
		if len(nodes) == 0 {
			return BenchRun{}, orchestration.Execution{}, fmt.Errorf("no node of host %s reports kairo ([bench] kairo_binary of its agent) and the filesystem %s (a filesystem provider's path)", req.HostID, filesystem)
		}
		run.NodeID, run.Target = nodes[0], filesystem
		// The test files (disk-sustained's is 8 GiB) on the filesystem tested.
		execution.Capacity = orchestration.CapacityRequest{CPUMillis: 1000, RAMBytes: 512 << 20, Disks: []orchestration.DiskRequest{{Filesystem: filesystem, ReserveBytes: 9 << 30}}}
	case BenchNetwork:
		peer := strings.TrimPrefix(req.DeviceID, BenchNetPrefix)
		nodes, configs, err = candidates(func(nodeID string, _ NodeBench) (bool, error) { return nodeID != peer, nil })
		if err != nil {
			return BenchRun{}, orchestration.Execution{}, err
		}
		if len(nodes) == 0 {
			return BenchRun{}, orchestration.Execution{}, fmt.Errorf("no node of host %s other than %s reports kairo ([bench] kairo_binary of its agent)", req.HostID, peer)
		}
		peerBench, err := nodeBenchTx(ctx, tx, peer)
		if errors.Is(err, ErrNotFound) {
			return BenchRun{}, orchestration.Execution{}, fmt.Errorf("%w: peer node %s", ErrNotFound, peer)
		}
		if err != nil {
			return BenchRun{}, orchestration.Execution{}, err
		}
		if peerBench.Kairo == "" {
			return BenchRun{}, orchestration.Execution{}, fmt.Errorf("peer node %s reports no kairo binary ([bench] kairo_binary of its agent)", peer)
		}
		run.NodeID, run.PeerNodeID, run.Target = nodes[0], &peer, peer
		if gang, err := gangSpanningTx(ctx, tx, run.NodeID, peer, ""); err != nil || gang != "" {
			if err == nil {
				err = fmt.Errorf("%w: gang %s spans nodes %s and %s: the network between them is in use", ErrBenchConflict, gang, run.NodeID, peer)
			}
			return BenchRun{}, orchestration.Execution{}, err
		}
		configs[peer] = peerBench
		execution.Capacity = orchestration.CapacityRequest{CPUMillis: 1000, RAMBytes: 512 << 20}
		// Rank 0 (the server) on the host's node, rank 1 (the client) on the peer.
		server, client := benchSelector(run.NodeID, configs[run.NodeID]), benchSelector(peer, peerBench)
		execution.Gang = &orchestration.Gang{Size: 2, Ranks: []orchestration.GangRank{
			{Rank: 0, Argv: benchArgv(configs[run.NodeID], run.ID), CWD: benchCWD(configs[run.NodeID]), Executor: &server},
			{Rank: 1, Argv: benchArgv(peerBench, run.ID), CWD: benchCWD(peerBench), Executor: &client},
		}}
	}
	bench := configs[run.NodeID]
	execution.Argv, execution.CWD, execution.Executor = benchArgv(bench, run.ID), benchCWD(bench), benchSelector(run.NodeID, bench)
	return run, execution, nil
}

func benchArgv(bench NodeBench, runID string) []string {
	return []string{bench.Kairo, "hardware", "bench-run", "--run", runID}
}

func benchCWD(bench NodeBench) string {
	if bench.CWD != "" {
		return bench.CWD
	}
	// The node's path, whatever the daemon's OS separates paths with.
	switch i := strings.LastIndexAny(bench.Kairo, `/\`); {
	case i > 0:
		return bench.Kairo[:i]
	case i == 0:
		return bench.Kairo[:1]
	}
	return "."
}

func benchSelector(nodeID string, bench NodeBench) orchestration.ExecutorSelector {
	labels := bench.ExecutorLabels
	if labels == nil {
		labels = map[string]string{}
	}
	return orchestration.ExecutorSelector{Labels: labels, Node: nodeID}
}

// nodeBenchTx is the bench configuration a node reports (none: zero).
func nodeBenchTx(ctx context.Context, q queryer, nodeID string) (NodeBench, error) {
	var attributes string
	err := q.QueryRowContext(ctx, `SELECT attributes_json FROM nodes WHERE id=$1`, nodeID).Scan(&attributes)
	if errors.Is(err, sql.ErrNoRows) {
		return NodeBench{}, fmt.Errorf("%w: node %s", ErrNotFound, nodeID)
	}
	if err != nil {
		return NodeBench{}, err
	}
	var values struct {
		Bench NodeBench `json:"bench"`
	}
	_ = json.Unmarshal([]byte(attributes), &values)
	return values.Bench, nil
}

// nodeCPUMillisTx is the CPU a node's host provider last reported (0: none).
func nodeCPUMillisTx(ctx context.Context, q queryer, nodeID string) (int64, error) {
	var total sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT o.total_bytes FROM resource_instances r JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=r.id ORDER BY id DESC LIMIT 1)
		WHERE r.node_id=$1 AND r.kind='cpu' ORDER BY r.id LIMIT 1`, nodeID).Scan(&total)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return total.Int64, err
}

// gangSpanningTx is a live gang (other than the one led by except) with ranks
// on both nodes, or "".
func gangSpanningTx(ctx context.Context, q queryer, nodeA, nodeB, except string) (string, error) {
	var gang string
	err := q.QueryRowContext(ctx, `SELECT gp.gang_id FROM gang_placements gp
		JOIN gang_placement_leases a ON a.placement_id=gp.id JOIN leases la ON la.id=a.lease_id
		JOIN gang_placement_leases b ON b.placement_id=gp.id JOIN leases lb ON lb.id=b.lease_id
		JOIN execution_gangs g ON g.id=gp.gang_id
		WHERE gp.state IN('placed','launched') AND a.node_id=$1 AND b.node_id=$2 AND la.state IN`+liveLeaseStates+` AND lb.state IN`+liveLeaseStates+` AND g.leader_execution_id<>$3
		LIMIT 1`, nodeA, nodeB, except).Scan(&gang)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return gang, err
}

// ListBenchRuns is every bench of host (of deviceID when given), oldest
// first, with their points when points is set.
func (s *Store) ListBenchRuns(ctx context.Context, hostID, deviceID string, points bool) ([]BenchRun, error) {
	query := `SELECT ` + benchRunColumns + ` FROM bench_runs WHERE host_id=$1`
	args := []any{hostID}
	if deviceID != "" {
		query += ` AND device_id=` + bind(&args, deviceID)
	}
	runs := []BenchRun{}
	if err := eachRow(ctx, s.db, query+` ORDER BY created_at,id`, args, func(rows *sql.Rows) error {
		run, err := scanBenchRun(rows)
		if err == nil {
			runs = append(runs, run)
		}
		return err
	}); err != nil {
		return nil, err
	}
	if !points {
		return runs, nil
	}
	for i := range runs {
		var err error
		if runs[i].Points, err = benchPoints(ctx, s.db, runs[i].ID); err != nil {
			return nil, err
		}
	}
	return runs, nil
}

// GetBenchRun is one run with its points.
func (s *Store) GetBenchRun(ctx context.Context, runID string) (BenchRun, error) {
	run, err := scanBenchRun(s.db.QueryRowContext(ctx, `SELECT `+benchRunColumns+` FROM bench_runs WHERE id=$1`, runID))
	if errors.Is(err, sql.ErrNoRows) {
		return run, fmt.Errorf("%w: bench run %s", ErrNotFound, runID)
	}
	if err != nil {
		return run, err
	}
	run.Points, err = benchPoints(ctx, s.db, runID)
	return run, err
}

func benchPoints(ctx context.Context, q queryer, runID string) ([]BenchPoint, error) {
	out := []BenchPoint{}
	err := eachRow(ctx, q, `SELECT seq,kind,phase,settings_json,metrics_json,valid,flags_json,measured_at FROM bench_points WHERE run_id=$1 ORDER BY seq`, []any{runID}, func(rows *sql.Rows) error {
		var p BenchPoint
		var settings, metrics, flags string
		if err := rows.Scan(&p.Seq, &p.Kind, &p.Phase, &settings, &metrics, &p.Valid, &flags, &p.MeasuredAt); err != nil {
			return err
		}
		for _, x := range []struct {
			body string
			into any
		}{{settings, &p.Settings}, {metrics, &p.Metrics}, {flags, &p.Flags}} {
			if err := json.Unmarshal([]byte(x.body), x.into); err != nil {
				return err
			}
		}
		out = append(out, p)
		return nil
	})
	return out, err
}

// BenchOfAttempt is the bench run an attempt carries out, and its rank (0
// but for a network gang's client). ErrNotFound: the attempt is no bench's.
func (s *Store) BenchOfAttempt(ctx context.Context, attemptID string) (BenchRun, int, error) {
	var runID string
	var rank int
	err := s.db.QueryRowContext(ctx, `SELECT e.run_id,e.rank FROM attempts a JOIN bench_run_executions e ON e.execution_id=a.execution_id WHERE a.id=$1`, attemptID).Scan(&runID, &rank)
	if errors.Is(err, sql.ErrNoRows) {
		return BenchRun{}, 0, fmt.Errorf("%w: attempt %s carries out no hardware bench", ErrNotFound, attemptID)
	}
	if err != nil {
		return BenchRun{}, 0, err
	}
	run, err := scanBenchRun(s.db.QueryRowContext(ctx, `SELECT `+benchRunColumns+` FROM bench_runs WHERE id=$1`, runID))
	return run, rank, err
}

// BenchStatus is what a bench driver needs to know of its device at a moment:
// its run, the device's setting in effect and what its controller applied,
// and whether anything else is using the device.
type BenchStatus struct {
	Run     BenchRun `json:"run"`
	Rank    int      `json:"rank"`
	LeaseID string   `json:"lease_id"`
	// Programs are the bench programs of the node this rank runs on.
	Programs NodeBench `json:"programs"`
	// Capabilities: what the device's controller can set (GPU and CPU).
	Capabilities      *DeviceCapabilities `json:"capabilities,omitempty"`
	MaxSlowdown       float64             `json:"max_slowdown"`
	Generation        int64               `json:"generation"`
	AppliedGeneration *int64              `json:"applied_generation,omitempty"`
	Applied           *DeviceSettings     `json:"applied,omitempty"`
	Effective         *DeviceSetting      `json:"effective,omitempty"`
	LastError         *string             `json:"last_error,omitempty"`
	// OtherWork is what else uses the device: a process Kairo did not start
	// on the GPU, other leases on the host's nodes (CPU) or reserving the
	// filesystem (storage), a gang across the two nodes (network).
	OtherWork []string `json:"other_work"`
	// TemperatureC is the GPU's latest observed temperature.
	TemperatureC *float64 `json:"temperature_c,omitempty"`
	// NextSeq is the seq the run's next point takes: past the points an
	// earlier attempt of a resumed run left.
	NextSeq int `json:"next_seq"`
}

// Applies reports whether the device is known to hold what is in effect now:
// the controller applied the current generation.
func (b BenchStatus) Applies() bool {
	return b.AppliedGeneration != nil && *b.AppliedGeneration == b.Generation && b.Applied != nil
}

// BenchStatusOf is the status of the run attemptID carries out under leaseID.
func (s *Store) BenchStatusOf(ctx context.Context, attemptID, leaseID string) (BenchStatus, error) {
	run, rank, err := s.BenchOfAttempt(ctx, attemptID)
	if err != nil {
		return BenchStatus{}, err
	}
	status := BenchStatus{Run: run, Rank: rank, LeaseID: leaseID, OtherWork: []string{}}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return status, err
	}
	defer tx.Rollback()
	node := run.NodeID
	if rank == 1 && run.PeerNodeID != nil {
		node = *run.PeerNodeID
	}
	if status.Programs, err = nodeBenchTx(ctx, tx, node); err != nil {
		return status, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq)+1,0) FROM bench_points WHERE run_id=$1`, run.ID).Scan(&status.NextSeq); err != nil {
		return status, err
	}
	if run.Class == BenchGPU || run.Class == BenchCPU {
		policy, _, err := devicePolicyTx(ctx, tx, run.HostID, run.DeviceID)
		if err != nil {
			return status, err
		}
		status.MaxSlowdown = policy.MaxSlowdown
		device, err := hardwareDeviceTx(ctx, tx, policy)
		if err != nil {
			return status, err
		}
		status.Capabilities, status.Generation, status.AppliedGeneration, status.Applied, status.Effective, status.LastError =
			device.Capabilities, device.Generation, device.AppliedGeneration, device.Applied, device.Effective, device.LastError
	}
	add := func(query string, args ...any) error {
		return eachRow(ctx, tx, query, args, func(rows *sql.Rows) error {
			var what string
			if err := rows.Scan(&what); err != nil {
				return err
			}
			status.OtherWork = append(status.OtherWork, what)
			return nil
		})
	}
	switch run.Class {
	case BenchGPU:
		err = add(`SELECT DISTINCT c.claim_kind||' on '||r.node_id||COALESCE(' (pid '||(c.evidence_json::jsonb->>'pid')||')','') FROM external_claims c JOIN resource_instances r ON r.id=c.resource_id
			WHERE c.cleared_at IS NULL AND r.kind='gpu' AND r.stable_identity=$1`, run.DeviceID)
		if err == nil {
			var temperature sql.NullFloat64
			err = tx.QueryRowContext(ctx, `SELECT o.temperature_c FROM resource_instances r JOIN resource_observations o ON o.resource_id=r.id
				WHERE r.kind='gpu' AND r.stable_identity=$1 AND o.temperature_c IS NOT NULL ORDER BY o.observed_at DESC LIMIT 1`, run.DeviceID).Scan(&temperature)
			if errors.Is(err, sql.ErrNoRows) {
				err = nil
			}
			if temperature.Valid {
				status.TemperatureC = &temperature.Float64
			}
		}
	case BenchCPU:
		spec, err2 := declaredSpec(ctx, tx)
		if err2 != nil {
			return status, err2
		}
		var nodes []string
		for _, host := range spec.Hosts {
			if host.ID == run.HostID {
				nodes = host.Nodes
			}
		}
		for _, node := range nodes {
			if err = add(`SELECT DISTINCT 'lease '||l.id||' on '||x.node_id FROM leases l JOIN executors x ON x.id=l.executor_id JOIN lease_items li ON li.lease_id=l.id
				WHERE x.node_id=$1 AND l.state IN`+liveLeaseStates+` AND l.id<>$2 AND li.kind='cpu' AND li.quantity>0`, node, leaseID); err != nil {
				break
			}
		}
	case BenchStorage:
		err = add(`SELECT DISTINCT 'lease '||l.id||' reserves '||li.filesystem FROM leases l JOIN executors x ON x.id=l.executor_id JOIN lease_items li ON li.lease_id=l.id
			WHERE x.node_id=$1 AND l.state IN`+liveLeaseStates+` AND l.id<>$2 AND li.kind='disk' AND li.filesystem=$3`, run.NodeID, leaseID, run.Target)
	case BenchNetwork:
		var gang string
		if gang, err = gangSpanningTx(ctx, tx, run.NodeID, run.Target, run.ExecutionID); err == nil && gang != "" {
			status.OtherWork = append(status.OtherWork, "gang "+gang+" spans "+run.NodeID+" and "+run.Target)
		}
		if err == nil {
			// a gang placed the other way round spans the nodes too
			if gang, err = gangSpanningTx(ctx, tx, run.Target, run.NodeID, run.ExecutionID); err == nil && gang != "" && !slices.Contains(status.OtherWork, "gang "+gang+" spans "+run.NodeID+" and "+run.Target) {
				status.OtherWork = append(status.OtherWork, "gang "+gang+" spans "+run.NodeID+" and "+run.Target)
			}
		}
	}
	return status, err
}

// SetBenchSetting makes settings the bench setting of the run attemptID
// carries out, under its lease; the zero setting clears it (the device
// returns to the policy's setting, else its defaults).
func (s *Store) SetBenchSetting(ctx context.Context, attemptID, leaseID string, settings DeviceSettings) (DeviceSettingResult, error) {
	run, rank, err := s.BenchOfAttempt(ctx, attemptID)
	if err != nil {
		return DeviceSettingResult{}, err
	}
	if run.Class != BenchGPU && run.Class != BenchCPU || rank != 0 {
		return DeviceSettingResult{}, fmt.Errorf("a %s bench changes no setting", run.Class)
	}
	if !run.Live() {
		return DeviceSettingResult{}, fmt.Errorf("%w: bench run %s is %s", ErrBenchConflict, run.ID, run.State)
	}
	owner, actor := DeviceOwner{Kind: OwnerBench, Ref: leaseID}, "bench "+run.ID
	if settings.IsDefault() {
		return s.ClearDeviceSetting(ctx, run.HostID, run.DeviceID, owner, actor)
	}
	return s.SetDeviceSetting(ctx, run.HostID, run.DeviceID, owner, settings, actor)
}

// BenchStart is what a driver records as it starts: its build, the device's
// facts and its temperature at rest.
type BenchStart struct {
	DriverBuild      string                  `json:"driver_build"`
	Versions         map[string]BenchVersion `json:"versions,omitempty"`
	DriverVersion    string                  `json:"driver_version,omitempty"`
	Device           json.RawMessage         `json:"device,omitempty"`
	IdleTemperatureC *float64                `json:"idle_temperature_c,omitempty"`
}

// StartBenchRun marks the run attemptID carries out running and records what
// its driver found (event hardware_bench_started, or hardware_bench_resumed
// for a run that ran before). Starting again (a driver that retried, or
// starts another kind) adds the versions it now knows.
func (s *Store) StartBenchRun(ctx context.Context, attemptID string, start BenchStart) error {
	run, _, err := s.BenchOfAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	if !run.Live() {
		return fmt.Errorf("%w: bench run %s is %s", ErrBenchConflict, run.ID, run.State)
	}
	versions := run.Versions
	if versions == nil {
		versions = map[string]BenchVersion{}
	}
	for kind, v := range start.Versions {
		versions[kind] = v
	}
	body, err := json.Marshal(versions)
	if err != nil {
		return err
	}
	device := sql.NullString{String: string(start.Device), Valid: len(start.Device) != 0 && json.Valid(start.Device)}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE bench_runs SET state='running',started_at=COALESCE(started_at,$1),driver_build=$2,versions_json=$3,driver_version=COALESCE($4,driver_version),
		device_json=COALESCE($5,device_json),idle_temperature_c=COALESCE(idle_temperature_c,$6) WHERE id=$7`,
		t, start.DriverBuild, string(body), nullable(start.DriverVersion), device, start.IdleTemperatureC, run.ID); err != nil {
		return err
	}
	if run.State == BenchWaiting {
		event := "hardware_bench_started"
		if run.StartedAt != nil {
			event = "hardware_bench_resumed"
		}
		if err = appendCoordinationEventTx(ctx, tx, event, nil, "bench_run", run.ID, nil, map[string]any{
			"host_id": run.HostID, "device_id": run.DeviceID, "kinds": run.Kinds, "kinds_done": run.doneKinds(), "driver_build": start.DriverBuild, "idle_temperature_c": start.IdleTemperatureC}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// RecordBenchKind records the reference of a kind the run attemptID carries
// out is done with: a resumed run does not measure it again.
func (s *Store) RecordBenchKind(ctx context.Context, attemptID, kind string, ref BenchReference) error {
	run, _, err := s.BenchOfAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	if run.State != BenchRunning {
		return fmt.Errorf("%w: bench run %s is %s", ErrBenchConflict, run.ID, run.State)
	}
	if !slices.Contains(run.Kinds, kind) {
		return fmt.Errorf("%q is not a kind of run %s", kind, run.ID)
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	references, err := benchReferencesTx(ctx, tx, run.ID)
	if err != nil {
		return err
	}
	references[kind] = ref
	body, err := json.Marshal(references)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE bench_runs SET references_json=$1 WHERE id=$2`, string(body), run.ID); err != nil {
		return err
	}
	return tx.Commit()
}

// benchReferencesTx are the references a run has recorded (none: empty).
func benchReferencesTx(ctx context.Context, tx *sql.Tx, runID string) (map[string]BenchReference, error) {
	var body sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT references_json FROM bench_runs WHERE id=$1`, runID).Scan(&body); err != nil {
		return nil, err
	}
	references := map[string]BenchReference{}
	if body.Valid {
		if err := json.Unmarshal([]byte(body.String), &references); err != nil {
			return nil, err
		}
	}
	if references == nil {
		references = map[string]BenchReference{}
	}
	return references, nil
}

// AddBenchPoints records measurements of the run attemptID carries out; a
// point sent again (same seq) replaces the earlier copy.
func (s *Store) AddBenchPoints(ctx context.Context, attemptID string, points []BenchPoint) error {
	run, _, err := s.BenchOfAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	if run.State != "running" {
		return fmt.Errorf("%w: bench run %s is %s", ErrBenchConflict, run.ID, run.State)
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, p := range points {
		if !slices.Contains(run.Kinds, p.Kind) {
			return fmt.Errorf("point %d: %q is not a kind of run %s", p.Seq, p.Kind, run.ID)
		}
		if !slices.Contains([]string{PhaseReference, PhaseSweep, PhaseMeasure}, p.Phase) {
			return fmt.Errorf("point %d: phase %q is not %s, %s or %s", p.Seq, p.Phase, PhaseReference, PhaseSweep, PhaseMeasure)
		}
		if p.Flags == nil {
			p.Flags = []string{}
		}
		if p.MeasuredAt == "" {
			p.MeasuredAt = now()
		}
		var bodies [3][]byte
		for i, v := range []any{p.Settings, p.Metrics, p.Flags} {
			if bodies[i], err = json.Marshal(v); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO bench_points(run_id,seq,kind,phase,settings_json,metrics_json,valid,flags_json,measured_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT(run_id,seq) DO UPDATE SET kind=excluded.kind,phase=excluded.phase,settings_json=excluded.settings_json,metrics_json=excluded.metrics_json,
			valid=excluded.valid,flags_json=excluded.flags_json,measured_at=excluded.measured_at`,
			run.ID, p.Seq, p.Kind, p.Phase, string(bodies[0]), string(bodies[1]), p.Valid, string(bodies[2]), p.MeasuredAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// BenchFinish is a run's outcome: each kind's references (those recorded as
// each kind was done are kept), and whether the run as a whole is valid
// (Reason: why not, by kind).
type BenchFinish struct {
	References map[string]BenchReference `json:"references"`
	Valid      bool                      `json:"valid"`
	Reason     string                    `json:"reason,omitempty"`
}

// benchOutcome is the state a run ends in: finished when it is valid and so
// is every kind, partial when some kinds are, else invalid; and why not
// every kind is valid when the driver did not say.
func benchOutcome(kinds []string, references map[string]BenchReference, finish BenchFinish) (string, bool, string) {
	valid, reasons := 0, []string{}
	for _, kind := range kinds {
		switch ref, ok := references[kind]; {
		case !ok:
			reasons = append(reasons, kind+": not measured")
		case ref.Valid:
			valid++
		default:
			reasons = append(reasons, kind+": "+ref.Reason)
		}
	}
	reason := finish.Reason
	if reason == "" {
		reason = strings.Join(reasons, "; ")
	}
	switch {
	case finish.Valid && valid == len(kinds):
		return BenchFinished, true, ""
	case valid > 0:
		return BenchPartial, false, reason
	}
	return BenchInvalid, false, reason
}

// FinishBenchRun ends the run attemptID carries out (events
// hardware_bench_finished, and hardware_bench_invalid when no kind is valid)
// and clears its bench setting.
func (s *Store) FinishBenchRun(ctx context.Context, attemptID, leaseID string, finish BenchFinish) error {
	run, _, err := s.BenchOfAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	if run.State != BenchRunning {
		return fmt.Errorf("%w: bench run %s is %s", ErrBenchConflict, run.ID, run.State)
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	merged, err := benchReferencesTx(ctx, tx, run.ID)
	if err != nil {
		return err
	}
	for kind, ref := range finish.References {
		merged[kind] = ref
	}
	references, err := json.Marshal(merged)
	if err != nil {
		return err
	}
	state, valid, reason := benchOutcome(run.Kinds, merged, finish)
	if _, err = tx.ExecContext(ctx, `UPDATE bench_runs SET state=$1,ended_at=$2,references_json=$3,valid=$4,reason=$5 WHERE id=$6`, state, now(), string(references), valid, nullable(reason), run.ID); err != nil {
		return err
	}
	if run.Class == BenchGPU || run.Class == BenchCPU {
		settings, err := deviceSettingsTx(ctx, tx, run.HostID, run.DeviceID)
		if err != nil {
			return err
		}
		for _, setting := range settings {
			if setting.Owner.Kind == OwnerBench && setting.Owner.Ref == leaseID {
				if err = deleteDeviceSettingTx(ctx, tx, setting, "bench "+run.ID+" finished"); err != nil {
					return err
				}
			}
		}
	}
	validKinds := []string{}
	for _, kind := range run.Kinds {
		if merged[kind].Valid {
			validKinds = append(validKinds, kind)
		}
	}
	payload := map[string]any{"host_id": run.HostID, "device_id": run.DeviceID, "kinds": run.Kinds, "state": state, "valid": valid, "valid_kinds": validKinds, "reason": reason}
	if err = appendCoordinationEventTx(ctx, tx, "hardware_bench_finished", nil, "bench_run", run.ID, nil, payload); err != nil {
		return err
	}
	if state == BenchInvalid {
		if err = appendCoordinationEventTx(ctx, tx, "hardware_bench_invalid", nil, "bench_run", run.ID, nil, payload); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ReconcileBenchRuns fails the runs whose task ended without the driver
// finishing them (event hardware_bench_invalid): the driver crashed, was
// force-stopped, or never started. A suspended run's task goes on (its
// continuation carries on the run), so its run does too. A run from before
// benches were tasks fails once its one execution ended.
func (s *Store) ReconcileBenchRuns(ctx context.Context) error {
	type ended struct{ id, host, device, cause string }
	var runs []ended
	if err := eachRow(ctx, s.db, `SELECT b.id,b.host_id,b.device_id,COALESCE(e.terminal_cause,'') FROM bench_runs b JOIN execution_requests e ON e.id=b.execution_id
		LEFT JOIN orchestration_tasks t ON t.task_scope_id=b.task_scope_id
		WHERE b.state IN('waiting','running') AND (b.task_scope_id IS NULL AND e.state='terminal' OR t.state IN('succeeded','failed','blocked','stopped','cancelled'))`, nil, func(rows *sql.Rows) error {
		var r ended
		if err := rows.Scan(&r.id, &r.host, &r.device, &r.cause); err != nil {
			return err
		}
		runs = append(runs, r)
		return nil
	}); err != nil {
		return err
	}
	for _, r := range runs {
		tx, err := s.beginWrite(ctx)
		if err != nil {
			return err
		}
		reason := "the bench execution ended without its results"
		if r.cause != "" {
			reason += " (" + r.cause + ")"
		}
		result, err := tx.ExecContext(ctx, `UPDATE bench_runs SET state='failed',ended_at=$1,valid=FALSE,reason=$2 WHERE id=$3 AND state IN('waiting','running')`, now(), reason, r.id)
		if err == nil {
			if n, _ := result.RowsAffected(); n == 1 {
				err = appendCoordinationEventTx(ctx, tx, "hardware_bench_invalid", nil, "bench_run", r.id, nil, map[string]any{"host_id": r.host, "device_id": r.device, "valid": false, "reason": reason})
			}
		}
		if err == nil {
			err = tx.Commit()
		}
		tx.Rollback()
		if err != nil {
			return err
		}
	}
	return nil
}

// RelSpread is (max-min)/mean of values (0 for fewer than two, +Inf when the
// mean is not positive).
func RelSpread(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))
	if mean <= 0 {
		return math.Inf(1)
	}
	return (slices.Max(values) - slices.Min(values)) / mean
}
