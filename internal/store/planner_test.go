package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// planCluster is a test installation: nodes with an executor each (labels
// site=<node>, pool=all), CPU, RAM, a "/" filesystem and GPUs.
type planCluster struct {
	t     *testing.T
	store *Store
	ctx   context.Context
}

func newPlanCluster(t *testing.T) *planCluster {
	return &planCluster{t: t, store: openCoordinationStore(t), ctx: context.Background()}
}

const gib = int64(1 << 30)

func (c *planCluster) node(id string, gpus int, cpuMillis, ramGiB, diskGiB int64) {
	c.t.Helper()
	s := c.store
	if err := s.UpsertNode(c.ctx, Node{ID: id, Name: id, OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		c.t.Fatal(err)
	}
	if err := s.UpsertExecutor(c.ctx, Executor{ID: "exec-" + id, NodeID: id, Kind: "linux", Attributes: json.RawMessage(`{"site":"` + id + `","pool":"all"}`), Enabled: true}); err != nil {
		c.t.Fatal(err)
	}
	if err := s.UpsertProvider(c.ctx, id+"-host", id, "host", nil); err != nil {
		c.t.Fatal(err)
	}
	add := func(resourceID, kind, identity string) {
		if err := s.UpsertResourceInstance(c.ctx, ResourceInstance{ID: resourceID, NodeID: id, ProviderID: id + "-host", Kind: kind, StableIdentity: identity, Binding: json.RawMessage(`{}`), AdminState: "enabled"}); err != nil {
			c.t.Fatal(err)
		}
	}
	add(id+"-cpu", "cpu", "logical-cpu")
	c.observe(id+"-cpu", cpuMillis, cpuMillis)
	add(id+"-ram", "ram", "physical-memory")
	c.observe(id+"-ram", ramGiB*gib, ramGiB*gib)
	add(id+"-disk", "disk", "/")
	c.observe(id+"-disk", diskGiB*gib, diskGiB*gib)
	for i := 0; i < gpus; i++ {
		uuid := fmt.Sprintf("GPU-%s-%d", id, i)
		add("gpu-"+uuid, "gpu", uuid)
		declareTestGPU(c.t, s, uuid, id)
		c.observe("gpu-"+uuid, 24*gib, 23*gib)
	}
}

func (c *planCluster) observe(resourceID string, total, free int64) {
	c.t.Helper()
	time.Sleep(2 * time.Millisecond) // (newer than the last coordination transition)
	stamp := time.Now().UTC()
	if err := c.store.RecordObservation(c.ctx, Observation{ResourceID: resourceID, ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
		c.t.Fatal(err)
	}
}

func planSpec(name, project string, priority, gpus int, cpuMillis, ramGiB int64) ExecutionSpec {
	spec := ExecutionSpec{
		ClientRequestID:  name,
		Scope:            ScopePath{Project: project, Queue: "q", Task: name},
		Argv:             []string{"run", name},
		CWD:              "/work",
		ExecutorSelector: json.RawMessage(`{"labels":{"pool":"all"}}`),
		Priority:         priority,
		Checkpointable:   true,
		Preemptible:      true,
		Capacity:         CapacityRequest{CPUMillis: cpuMillis, RAMBytes: ramGiB * gib, Strength: "admitted"},
	}
	if gpus > 0 {
		spec.Exclusive = []ExclusiveRequest{{Kind: "gpu", Count: gpus, SameNode: true, OnExternalClaim: "wait", OnUnattributedActivity: "wait", OnStaleObservation: "wait"}}
	}
	return spec
}

func (c *planCluster) submit(spec ExecutionSpec) ExecutionRequest {
	c.t.Helper()
	execution, _, err := c.store.SubmitExecution(c.ctx, spec)
	if err != nil {
		c.t.Fatal(err)
	}
	return execution
}

func (c *planCluster) reserve(executorID string) *Reservation {
	c.t.Helper()
	reservation, err := c.store.ReserveNext(c.ctx, executorID)
	if err != nil {
		c.t.Fatal(err)
	}
	return reservation
}

// start reserves the next execution on executorID and runs it.
func (c *planCluster) start(executorID string) *Launch {
	c.t.Helper()
	reservation := c.reserve(executorID)
	if reservation == nil {
		c.t.Fatalf("nothing to reserve on %s", executorID)
	}
	if err := c.store.MarkLeasePrepared(c.ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		c.t.Fatal(err)
	}
	launch, err := c.store.AuthorizeLaunch(c.ctx, reservation)
	if err != nil {
		c.t.Fatal(err)
	}
	if err = c.store.ActivateLaunch(c.ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 4000, "proc:4000:starttime:"+launch.Attempt.ID); err != nil {
		c.t.Fatal(err)
	}
	return launch
}

func (c *planCluster) wait(executionID string) AdmissionWait {
	c.t.Helper()
	waits, err := c.store.AdmissionWaits(c.ctx, []string{executionID})
	if err != nil {
		c.t.Fatal(err)
	}
	return waits[executionID]
}

func (c *planCluster) expectReserved(executorID string, want ExecutionRequest) *Reservation {
	c.t.Helper()
	got := c.reserve(executorID)
	if got == nil || got.Execution.ID != want.ID {
		c.t.Fatalf("%s reserved %+v, want %s", executorID, got, want.ClientRequestID)
	}
	return got
}

// An execution that fits nowhere holds what it can use where it is going;
// later executions run on the rest (backfill) but not on what it holds.
func TestPlannerBackfillsAroundAHeldBlockedExecution(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 2, 16000, 64, 500)
	c.submit(planSpec("running", "p1", 0, 1, 2000, 8))
	c.start("exec-a")
	big := c.submit(planSpec("big", "p2", 10, 2, 4000, 8))
	small := c.submit(planSpec("small", "p3", 5, 1, 1000, 1))
	cpu := c.submit(planSpec("cpu", "p3", 1, 0, 4000, 8))
	c.expectReserved("exec-a", cpu)
	if w := c.wait(big.ID); w.Reason != "gpu_leased" || !strings.Contains(w.Message, "p1/running") {
		t.Fatalf("big waits for %+v", w)
	}
	if w := c.wait(small.ID); w.Reason != "held_for_higher_priority" {
		t.Fatalf("small took or waits for %+v", w)
	}
	// big's CPU stays held: a job that would take it waits.
	greedy := c.submit(planSpec("greedy", "p3", 1, 0, 10000, 1))
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("reserved %s into big's hold", r.Execution.ClientRequestID)
	}
	if w := c.wait(greedy.ID); w.Reason != "insufficient_cpu" {
		t.Fatalf("greedy waits for %+v", w)
	}
}

// Nothing is held for an execution stopped by what does not end by itself.
func TestPlannerHoldsNothingForAForeignGPUProcess(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 2, 16000, 64, 500)
	if err := c.store.ObserveClaim(c.ctx, ExternalClaim{ID: "foreign", ResourceID: "gpu-GPU-a-0", ClaimKind: "external_process"}); err != nil {
		t.Fatal(err)
	}
	big := c.submit(planSpec("big", "p1", 10, 2, 1000, 1))
	small := c.submit(planSpec("small", "p2", 5, 1, 1000, 1))
	r := c.expectReserved("exec-a", small)
	if len(r.Resources) != 1 || r.Resources[0].ID != "gpu-GPU-a-1" {
		t.Fatalf("small got %+v", r.Resources)
	}
	if w := c.wait(big.ID); w.Reason != "external_claim" {
		t.Fatalf("big waits for %+v", w)
	}
}

// CPU work goes to a node without free GPUs, whichever executor polls.
func TestPlannerKeepsCPUWorkOffGPUNodes(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 1, 16000, 64, 500)
	c.node("b", 0, 16000, 64, 500)
	cpu := c.submit(planSpec("cpu", "p1", 0, 0, 2000, 4))
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("the GPU node reserved %s", r.Execution.ClientRequestID)
	}
	if w := c.wait(cpu.ID); w.Reason != "awaiting_executor" || !strings.Contains(w.Message, "exec-b") {
		t.Fatalf("cpu waits for %+v", w)
	}
	c.expectReserved("exec-b", cpu)
}

// An executor that stopped polling gets no placements.
func TestPlannerSkipsAnExecutorThatStoppedPolling(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 0, 16000, 64, 500)
	c.node("b", 1, 16000, 64, 500)
	if _, err := c.store.db.Exec(`UPDATE executors SET last_seen_at=? WHERE id='exec-a'`, time.Now().UTC().Add(-10*time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	cpu := c.submit(planSpec("cpu", "p1", 0, 0, 2000, 4))
	c.expectReserved("exec-b", cpu)
}

// Within a priority, the project holding the least goes first.
func TestPlannerSharesAPriorityAmongProjects(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 3, 16000, 64, 500)
	c.submit(planSpec("p1-0", "p1", 0, 1, 1000, 1))
	c.start("exec-a")
	c.submit(planSpec("p1-1", "p1", 0, 1, 1000, 1))
	c.start("exec-a")
	c.submit(planSpec("p1-2", "p1", 0, 1, 1000, 1))
	late := c.submit(planSpec("p2-0", "p2", 0, 1, 1000, 1))
	c.expectReserved("exec-a", late)
}

// A lease's RAM is counted once: the part its attempt already uses is in
// the observed free memory, not also in the reservations.
func TestPlannerCountsUsedRAMOnce(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 0, 16000, 64, 500)
	c.submit(planSpec("resident", "p1", 0, 0, 1000, 20))
	running := c.start("exec-a")
	c.observe("a-ram", 64*gib, 40*gib) // 20 GiB of it, 4 GiB of the system
	next := c.submit(planSpec("next", "p2", 0, 0, 1000, 30))
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("reserved %s without knowing what resident uses", r.Execution.ClientRequestID)
	}
	if w := c.wait(next.ID); w.Reason != "insufficient_ram" {
		t.Fatalf("next waits for %+v", w)
	}
	used := 20 * gib
	if err := c.store.RecordAttemptUsage(c.ctx, running.Attempt.ID, &used, nil); err != nil {
		t.Fatal(err)
	}
	c.expectReserved("exec-a", next)
}

// Preemption is not for GPUs alone: RAM a lower-priority cooperative
// execution holds is reclaimed for a waiting one.
func TestPlannerPreemptsForCapacity(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 0, 16000, 64, 500)
	c.submit(planSpec("victim", "p1", 0, 0, 2000, 50))
	victim := c.start("exec-a")
	c.submit(planSpec("waiter", "p2", 10, 0, 2000, 30))
	commands, err := c.store.EnsurePriorityPreemption(c.ctx, "exec-a")
	if err != nil || len(commands) != 1 {
		t.Fatalf("commands=%v err=%v", commands, err)
	}
	var attemptID string
	if err = c.store.db.QueryRow(`SELECT attempt_id FROM commands WHERE id=?`, commands[0]).Scan(&attemptID); err != nil || attemptID != victim.Attempt.ID {
		t.Fatalf("suspended %s, want %s (%v)", attemptID, victim.Attempt.ID, err)
	}
	again, err := c.store.EnsurePriorityPreemption(c.ctx, "exec-a")
	if err != nil || len(again) != 1 || again[0] != commands[0] {
		t.Fatalf("second call: %v %v", again, err)
	}
}

// A waiting execution below the top priority may preempt too, when the top
// one cannot run anywhere anyway.
func TestPlannerPreemptsBelowTheTopPriority(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 1, 16000, 64, 500)
	c.submit(planSpec("victim", "p1", 0, 1, 1000, 1))
	c.start("exec-a")
	c.submit(planSpec("huge", "p2", 20, 2, 1000, 1)) // no node has two GPUs
	c.submit(planSpec("middle", "p3", 10, 1, 1000, 1))
	commands, err := c.store.EnsurePriorityPreemption(c.ctx, "exec-a")
	if err != nil || len(commands) != 1 {
		t.Fatalf("commands=%v err=%v", commands, err)
	}
}

// A node with a filesystem under the watermark admits nothing, and says so.
func TestPlannerStopsAdmissionUnderTheDiskWatermark(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 0, 16000, 64, 100)
	c.observe("a-disk", 100*gib, 3*gib)
	job := c.submit(planSpec("job", "p1", 0, 0, 1000, 1))
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("reserved %s on a full disk", r.Execution.ClientRequestID)
	}
	if w := c.wait(job.ID); w.Reason != "disk_low_watermark" || !strings.Contains(w.Message, "/ has 3.0 GiB free") {
		t.Fatalf("job waits for %+v", w)
	}
	var alerts, events int
	_ = c.store.db.QueryRow(`SELECT COUNT(*) FROM node_alerts WHERE node_id='a'`).Scan(&alerts)
	_ = c.store.db.QueryRow(`SELECT COUNT(*) FROM coordination_events WHERE event_type='disk_low_watermark'`).Scan(&events)
	if alerts != 1 || events != 1 {
		t.Fatalf("alerts=%d events=%d", alerts, events)
	}
	c.observe("a-disk", 100*gib, 50*gib)
	c.expectReserved("exec-a", job)
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("unexpected %+v", r)
	}
	_ = c.store.db.QueryRow(`SELECT COUNT(*) FROM node_alerts WHERE node_id='a'`).Scan(&alerts)
	if alerts != 0 {
		t.Fatalf("alert not cleared: %d", alerts)
	}
}

// An execution no live executor can run says so.
func TestPlannerRecordsThatNoExecutorMatches(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 0, 16000, 64, 500)
	spec := planSpec("orphan", "p1", 0, 0, 1000, 1)
	spec.ExecutorSelector = json.RawMessage(`{"labels":{"site":"nowhere"}}`)
	orphan := c.submit(spec)
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("unexpected %+v", r)
	}
	if w := c.wait(orphan.ID); w.Reason != "no_executor" {
		t.Fatalf("orphan waits for %+v", w)
	}
}
