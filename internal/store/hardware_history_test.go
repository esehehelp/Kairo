package store

import (
	"context"
	"testing"
	"time"

	"kairo/internal/machine"
)

func historyRows(t *testing.T, s *Store, resourceID string) (n int, reserved int64) {
	t.Helper()
	if err := s.db.QueryRow(`SELECT COUNT(*),COALESCE(MAX(reserved),0) FROM resource_history WHERE resource_id=$1`, resourceID).Scan(&n, &reserved); err != nil {
		t.Fatal(err)
	}
	return n, reserved
}

func observeAt(t *testing.T, s *Store, resourceID string, at time.Time, total, free int64) {
	t.Helper()
	if err := s.RecordObservation(context.Background(), Observation{ResourceID: resourceID, ObservedAt: at.UTC().Format(time.RFC3339Nano), ValidUntil: at.Add(time.Minute).UTC().Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
		t.Fatal(err)
	}
}

// An observation is kept in the history at most every historyInterval per
// resource, with what the resource's live leases reserved of it then.
func TestHistoryKeepsAnObservationEveryTenSecondsWithWhatWasReserved(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 0, 16000, 64, 500)
	before, _ := historyRows(t, c.store, "a-ram")
	base := time.Now().Add(-time.Hour)
	observeAt(t, c.store, "a-ram", base, 64*gib, 60*gib)
	observeAt(t, c.store, "a-ram", base.Add(4*time.Second), 64*gib, 59*gib)
	if n, _ := historyRows(t, c.store, "a-ram"); n != before+1 {
		t.Fatalf("%d rows after two observations 4 s apart, want %d", n, before+1)
	}
	observeAt(t, c.store, "a-ram", base.Add(11*time.Second), 64*gib, 58*gib)
	if n, _ := historyRows(t, c.store, "a-ram"); n != before+2 {
		t.Fatalf("%d rows after a third 11 s later, want %d", n, before+2)
	}
	c.observe("a-ram", 64*gib, 64*gib) // fresh again: the ones above expired an hour ago
	c.submit(planSpec("job", "p", 0, 0, 2000, 8))
	c.start("exec-a")
	observeAt(t, c.store, "a-ram", time.Now().Add(time.Minute), 64*gib, 50*gib)
	if _, reserved := historyRows(t, c.store, "a-ram"); reserved != 8*gib {
		t.Fatalf("reserved %d, want %d", reserved, 8*gib)
	}
	// Pruning removes what is older than the retention, nothing newer.
	if removed, err := c.store.PruneHistory(context.Background(), 30*time.Minute); err != nil || removed < 2 {
		t.Fatalf("pruned %d (%v), want the two rows of an hour ago at least", removed, err)
	}
	if n, _ := historyRows(t, c.store, "a-ram"); n == 0 {
		t.Fatal("pruning removed the recent rows too")
	}
}

// Samples are averaged into buckets that hold a sample; a metric without a
// sample in a bucket is null, and device samples (power) join the series of
// their device.
func TestHardwareSamplesBucketsAndJoinsDeviceSamples(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 0, 16000, 64, 500)
	ctx := context.Background()
	since := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)
	observeAt(t, c.store, "a-ram", since.Add(5*time.Second), 64*gib, 60*gib)
	observeAt(t, c.store, "a-ram", since.Add(20*time.Second), 64*gib, 56*gib)
	observeAt(t, c.store, "a-ram", since.Add(3*time.Minute), 64*gib, 48*gib)
	// a CPU package power sample of a-cpu, and the series of host h over a
	if _, err := c.store.ApplyMachines(ctx, machine.Spec{Hosts: []machine.Host{{ID: "h", Nodes: []string{"a"}, Controller: "a", MaxSlowdown: 0.05}}}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.store.testExec(`INSERT INTO device_samples(device,resource_id,observed_at,valid_until,power_w) VALUES('a-cpu','a-cpu',$1,$2,80),('host/h/cpu',NULL,$1,$2,120)`,
		sampleStamp(since.Add(10*time.Second)), sampleStamp(since.Add(70*time.Second))); err != nil {
		t.Fatal(err)
	}
	out, err := c.store.HardwareSamples(ctx, since, since.Add(5*time.Minute), time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.StepSeconds != 60 {
		t.Fatalf("step %d", out.StepSeconds)
	}
	series := map[string]HardwareSeries{}
	for _, s := range out.Series {
		series[s.Key] = s
	}
	ram := series["a-ram"]
	if ram.Kind != "ram" || len(ram.Nodes) != 1 || ram.Nodes[0] != "a" || len(ram.T) != 2 {
		t.Fatalf("ram series %+v", ram)
	}
	if ram.T[0] != since.UnixMilli() || ram.T[1] != since.Add(3*time.Minute).UnixMilli() {
		t.Fatalf("ram buckets %v", ram.T)
	}
	if used := ram.Metrics["used"]; used[0] == nil || *used[0] != float64(6*gib) || used[1] == nil || *used[1] != float64(16*gib) {
		t.Fatalf("ram used %v", ram.Metrics["used"])
	}
	cpu := series["a-cpu"]
	if cpu.Kind != "cpu" || len(cpu.T) != 1 || cpu.Metrics["power_w"][0] == nil || *cpu.Metrics["power_w"][0] != 80 || cpu.Metrics["total"][0] != nil {
		t.Fatalf("cpu series %+v", cpu)
	}
	if host := series["host/h/cpu"]; host.Kind != "host_cpu" || len(host.T) != 1 || *host.Metrics["power_w"][0] != 120 || len(host.Nodes) != 1 || host.Nodes[0] != "a" {
		t.Fatalf("host series %+v", host)
	}
	// key limits the answer
	only, err := c.store.HardwareSamples(ctx, since, since.Add(5*time.Minute), time.Minute, []string{"a-ram"})
	if err != nil || len(only.Series) != 1 || only.Series[0].Key != "a-ram" {
		t.Fatalf("key=a-ram: %+v %v", only.Series, err)
	}
}

// A GPU declared on two nodes is one series, by its UUID, of both nodes.
func TestHardwareSamplesListAGPUOnceForEveryNode(t *testing.T) {
	s := openCoordinationStore(t)
	setupSharedGPU(t, s)
	out, err := s.HardwareSamples(context.Background(), time.Now().Add(-time.Hour), time.Now().Add(time.Minute), time.Minute, []string{"GPU-S"})
	if err != nil || len(out.Series) != 1 {
		t.Fatalf("%+v %v", out.Series, err)
	}
	gpu := out.Series[0]
	if gpu.Kind != "gpu" || len(gpu.Nodes) != 2 || len(gpu.Resources) != 2 || len(gpu.T) == 0 || gpu.Metrics["mem_total"][0] == nil || *gpu.Metrics["mem_total"][0] != float64(12<<30) {
		t.Fatalf("gpu series %+v", gpu)
	}
}

// Lease intervals: a live lease has no end, a released one ends when it was
// released; key limits them.
func TestLeaseIntervalsCoverLiveAndReleasedLeases(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 0, 16000, 64, 500)
	ctx := context.Background()
	c.submit(planSpec("done", "p", 0, 0, 1000, 1))
	launch := c.start("exec-a")
	epoch := launch.Lease.CoordinationEpoch
	if err := c.store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := c.store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		t.Fatal(err)
	}
	c.submit(planSpec("live", "p", 0, 0, 2000, 2))
	c.start("exec-a")
	leases, err := c.store.LeaseIntervals(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Minute), []string{"a-cpu"})
	if err != nil || len(leases) != 2 {
		t.Fatalf("%+v %v", leases, err)
	}
	byTask := map[string]LeaseInterval{}
	for _, l := range leases {
		byTask[l.Task] = l
	}
	done, live := byTask["done"], byTask["live"]
	if done.End == nil || done.Project != "p" || done.Queue != "q" || done.Quantity != 1000 || done.Kind != "cpu" || done.Node != "a" {
		t.Fatalf("released lease %+v", done)
	}
	if live.End != nil || live.Quantity != 2000 || live.Start == "" {
		t.Fatalf("live lease %+v", live)
	}
	// A window after the released one ended holds only the live one.
	end, _ := time.Parse(time.RFC3339Nano, *done.End)
	later, err := c.store.LeaseIntervals(ctx, end.Add(time.Second), time.Now().Add(time.Minute), []string{"a-cpu"})
	if err != nil || len(later) != 1 || later[0].Task != "live" {
		t.Fatalf("after the release: %+v %v", later, err)
	}
}
