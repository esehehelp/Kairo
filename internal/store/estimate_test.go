package store

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestEstimateKeysMaskWhatChangesBetweenRunsOfOneKind(t *testing.T) {
	train := func(config string) []string {
		return estimateKeys("p", "q", "train-7", []string{"uv", "run", "--no-sync", "python", "-m", "neo_ime.cli", "train", "--config", config})
	}
	a, b := train("configs/shunran-v0.9-30m.toml"), train(`configs\shunran-v0.10-4m.toml`)
	if a[0] != b[0] {
		t.Fatalf("configs differing in numbers only have keys %q and %q", a[0], b[0])
	}
	if c := train("configs/sumire-v8.toml"); c[0] == a[0] || c[1] != a[1] {
		t.Fatalf("another config: keys %q, want the same subcommand key %q and another config key", c[:2], a[1])
	}
	bench := estimateKeys("p", "q", "train-7", []string{"uv", "run", "python", "-m", "neo_ime.cli", "bench", "--config", "configs/shunran-v0.9-30m.toml"})
	if bench[1] == a[1] || bench[2] != a[2] {
		t.Fatalf("another subcommand: keys %q, want another subcommand key and the same Task key", bench[:3])
	}
	if got := commandBase(`D:\tools\bench-f8ce6a2.exe`); got != "bench" {
		t.Fatalf("commandBase = %q", got)
	}
	if got := maskNumbers("eval-f8ce6a2-s14000"); got != "eval-#-s#" {
		t.Fatalf("maskNumbers = %q", got)
	}
	pwsh := estimateKeys("p", "q", "", []string{"pwsh.exe", "-NoProfile", "-Command", "uv run python scripts/collect.py --dry-run"})
	if !strings.Contains(pwsh[0], "\x00collect\x00") {
		t.Fatalf("a -Command line has key %q", pwsh[0])
	}
}

func TestRemainingExpectsAnOverrunToTakeAsLongAgain(t *testing.T) {
	for _, c := range []struct{ total, done, want time.Duration }{
		{2 * time.Hour, 30 * time.Minute, 90 * time.Minute},
		{10 * time.Minute, time.Hour, time.Hour},
		{10 * time.Minute, 10 * time.Minute, 10 * time.Minute},
		{10 * time.Second, 20 * time.Second, overrunFloor},
	} {
		if got := remaining(c.total, c.done); got != c.want {
			t.Errorf("remaining(%v, %v) = %v, want %v", c.total, c.done, got, c.want)
		}
	}
	if got := median([]float64{5, 1, 3, 2}); got != 2.5 {
		t.Fatalf("median = %v", got)
	}
}

// ran records a run of spec on node a that took d and exited 0, for
// estimates.
func (c *planCluster) ran(spec ExecutionSpec, d time.Duration) {
	c.t.Helper()
	c.submit(spec)
	launch := c.start("exec-a")
	epoch := launch.Lease.CoordinationEpoch
	if err := c.store.RecordTerminal(c.ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 0, ""); err != nil {
		c.t.Fatal(err)
	}
	if err := c.store.FinalizeQuiescence(c.ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		c.t.Fatal(err)
	}
	end := time.Now().UTC()
	c.setAttemptTimes(launch.Attempt.ID, end.Add(-d), end)
}

// started moves the start of a running attempt back by d.
func (c *planCluster) started(launch *Launch, ago time.Duration) {
	c.t.Helper()
	c.setAttemptTimes(launch.Attempt.ID, time.Now().UTC().Add(-ago), time.Time{})
}

func (c *planCluster) setAttemptTimes(attemptID string, started, exited time.Time) {
	c.t.Helper()
	var exitedAt any
	if !exited.IsZero() {
		exitedAt = exited.Format(time.RFC3339Nano)
	}
	if _, err := c.store.exec(c.ctx, `UPDATE attempts SET started_at=$1,exited_at=COALESCE($2,exited_at) WHERE id=$3`, started.Format(time.RFC3339Nano), exitedAt, attemptID); err != nil {
		c.t.Fatal(err)
	}
	c.store.runs.invalidate()
}

// A blocked execution holds what it can use from when it is expected to
// start; work expected to end by then runs on it, longer work and work
// without an estimate wait.
func TestPlannerBackfillsWorkThatEndsBeforeAHeldExecutionStarts(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 2, 16000, 64, 500)
	c.ran(planSpec("train-1", "p1", 0, 0, 1000, 1), 2*time.Hour)
	c.ran(planSpec("eval-1", "p3", 0, 0, 1000, 1), 10*time.Minute)
	c.ran(planSpec("long-1", "p3", 0, 0, 1000, 1), 5*time.Hour)

	c.submit(planSpec("train-2", "p1", 0, 1, 2000, 8))
	c.started(c.start("exec-a"), 30*time.Minute) // 1h30m left
	big := c.submit(planSpec("big", "p2", 10, 2, 4000, 8))
	long := c.submit(planSpec("long-2", "p3", 5, 1, 1000, 1))
	fresh := c.submit(planSpec("fresh", "p4", 5, 1, 1000, 1))
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("reserved %s into big's hold", r.Execution.ClientRequestID)
	}
	if w := c.wait(big.ID); w.Reason != "gpu_leased" || !strings.Contains(w.Message, "expected to start in about 1h30m") {
		t.Fatalf("big waits for %+v", w)
	}
	for _, e := range []ExecutionRequest{long, fresh} {
		if w := c.wait(e.ID); w.Reason != "held_for_higher_priority" {
			t.Fatalf("%s waits for %+v", e.ClientRequestID, w)
		}
	}
	eval := c.submit(planSpec("eval-2", "p3", 5, 1, 1000, 1))
	c.expectReserved("exec-a", eval)
}

// A run past its estimate is expected to run as long again as it has.
func TestPlannerExpectsAnOverrunningRunToTakeAsLongAgain(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 2, 16000, 64, 500)
	c.ran(planSpec("train-1", "p1", 0, 0, 1000, 1), 10*time.Minute)
	c.ran(planSpec("eval-1", "p3", 0, 0, 1000, 1), 30*time.Minute)
	c.ran(planSpec("long-1", "p3", 0, 0, 1000, 1), 2*time.Hour)

	c.submit(planSpec("train-2", "p1", 0, 1, 2000, 8))
	c.started(c.start("exec-a"), time.Hour)
	big := c.submit(planSpec("big", "p2", 10, 2, 4000, 8))
	long := c.submit(planSpec("long-2", "p3", 5, 1, 1000, 1))
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("reserved %s into big's hold", r.Execution.ClientRequestID)
	}
	if w := c.wait(big.ID); !strings.Contains(w.Message, "expected to start in about 1h00m") {
		t.Fatalf("big waits for %+v", w)
	}
	if w := c.wait(long.ID); w.Reason != "held_for_higher_priority" {
		t.Fatalf("long waits for %+v", w)
	}
	eval := c.submit(planSpec("eval-2", "p3", 5, 1, 1000, 1))
	c.expectReserved("exec-a", eval)
}

// ranUsing records a run like ran whose attempt was seen to use ram bytes.
func (c *planCluster) ranUsing(spec ExecutionSpec, d time.Duration, ram int64) {
	c.t.Helper()
	c.submit(spec)
	launch := c.start("exec-a")
	if err := c.store.RecordAttemptUsage(c.ctx, launch.Attempt.ID, &ram, nil); err != nil {
		c.t.Fatal(err)
	}
	epoch := launch.Lease.CoordinationEpoch
	if err := c.store.RecordTerminal(c.ctx, launch.Attempt.ID, launch.Lease.ID, epoch, 0, ""); err != nil {
		c.t.Fatal(err)
	}
	if err := c.store.FinalizeQuiescence(c.ctx, launch.Attempt.ID, launch.Lease.ID, epoch); err != nil {
		c.t.Fatal(err)
	}
	end := time.Now().UTC()
	c.setAttemptTimes(launch.Attempt.ID, end.Add(-d), end)
}

// A running lease's RAM reservation counts up to what runs like it were seen
// to take (half again, plus 512 MiB), once three such runs were sampled and
// its own use is known now: one that reserved 8 GiB and whose like took
// 300 MiB leaves the rest to others. With two runs, or its use not known,
// it counts in full.
func TestPlannerCountsRAMUpToWhatRunsLikeItTook(t *testing.T) {
	run := func(t *testing.T, history int, sampled bool) (*planCluster, ExecutionRequest) {
		c := newPlanCluster(t)
		c.node("a", 0, 16000, 64, 500)
		for i := 1; i <= history; i++ {
			c.ranUsing(planSpec(fmt.Sprintf("compress-%d", i), "p1", 0, 0, 1000, 8), time.Minute, 300<<20)
		}
		c.submit(planSpec("compress-9", "p1", 0, 0, 1000, 8))
		running := c.start("exec-a")
		if sampled {
			used := int64(100 << 20)
			if err := c.store.RecordAttemptUsage(c.ctx, running.Attempt.ID, &used, nil); err != nil {
				t.Fatal(err)
			}
		}
		c.observe("a-ram", 64*gib, 10*gib)
		// 10 GiB free: 9 GiB fits once compress-9 counts ~1 GiB, not 8
		return c, c.submit(planSpec("other", "p2", 0, 0, 1000, 9))
	}
	t.Run("three runs like it", func(t *testing.T) {
		c, other := run(t, 3, true)
		c.expectReserved("exec-a", other)
	})
	for _, tc := range []struct {
		name    string
		history int
		sampled bool
	}{{"two runs like it", 2, true}, {"its use not known", 3, false}} {
		t.Run(tc.name, func(t *testing.T) {
			c, other := run(t, tc.history, tc.sampled)
			if r := c.reserve("exec-a"); r != nil {
				t.Fatalf("reserved %s", r.Execution.ClientRequestID)
			}
			if w := c.wait(other.ID); w.Reason != "insufficient_ram" {
				t.Fatalf("other waits for %+v", w)
			}
		})
	}
}

func TestExpectedRAMNeedsThreeRunsOfASpecificKey(t *testing.T) {
	keys := estimateKeys("p", "q", "compress-4", []string{"run", "compress-4"})
	h := &runHistory{ram: map[string][]int64{keys[2]: {1 << 30, 2 << 30}, keys[3]: {1 << 20, 1 << 20, 1 << 20}}}
	if got, ok := h.expectedRAM(keys); ok {
		t.Fatalf("from two runs, or a queue's: %d", got)
	}
	h.ram[keys[2]] = append(h.ram[keys[2]], 1<<29)
	if got, ok := h.expectedRAM(keys); !ok || got != 3<<30+ramHeadroom {
		t.Fatalf("expected %d %v, want %d", got, ok, 3<<30+ramHeadroom)
	}
}
