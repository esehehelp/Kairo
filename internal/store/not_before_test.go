package store

import (
	"strings"
	"testing"
	"time"
)

func at(d time.Duration) string { return time.Now().Add(d).Format(time.RFC3339) }

// Before notBeforeHold ahead of its not_before an execution is not admitted
// and holds nothing: other work runs on what it will need.
func TestPlannerAdmitsNothingAndHoldsNothingLongBeforeNotBefore(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 1, 16000, 64, 500)
	spec := planSpec("predict", "rhllem", 10, 1, 2000, 16)
	spec.NotBefore = at(3 * time.Hour)
	predict := c.submit(spec)
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("reserved %s before its not_before", r.Execution.ClientRequestID)
	}
	if w := c.wait(predict.ID); w.Reason != "not_before" || !strings.Contains(w.Message, "nothing is held for it before") {
		t.Fatalf("predict waits for %+v", w)
	}
	fresh := c.submit(planSpec("fresh", "p4", 0, 1, 1000, 1))
	c.expectReserved("exec-a", fresh)
}

// Within notBeforeHold of its not_before, what it needs is held: work expected
// to end by then runs on it, longer work and work without an estimate wait;
// once the time has come it is admitted.
func TestPlannerHoldsForANearNotBefore(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 1, 16000, 64, 500)
	c.ran(planSpec("eval-1", "p3", 0, 0, 1000, 1), 10*time.Minute)
	c.ran(planSpec("long-1", "p3", 0, 0, 1000, 1), 5*time.Hour)
	spec := planSpec("predict", "rhllem", 10, 1, 2000, 16)
	spec.NotBefore = at(time.Hour)
	predict := c.submit(spec)
	long := c.submit(planSpec("long-2", "p3", 0, 1, 1000, 1))
	fresh := c.submit(planSpec("fresh", "p4", 0, 1, 1000, 1))
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("reserved %s into the hold of predict", r.Execution.ClientRequestID)
	}
	if w := c.wait(predict.ID); w.Reason != "not_before" || w.NodeID == nil || *w.NodeID != "a" || !strings.Contains(w.Message, "is held for it") {
		t.Fatalf("predict waits for %+v", w)
	}
	for _, e := range []ExecutionRequest{long, fresh} {
		if w := c.wait(e.ID); w.Reason != "held_for_higher_priority" {
			t.Fatalf("%s waits for %+v", e.ClientRequestID, w)
		}
	}
	eval := c.submit(planSpec("eval-2", "p3", 0, 1, 1000, 1))
	c.expectReserved("exec-a", eval)

	// The time has come (declared an hour ago): an execution is admitted.
	c2 := newPlanCluster(t)
	c2.node("a", 1, 16000, 64, 500)
	past := planSpec("predict", "rhllem", 10, 1, 2000, 16)
	past.NotBefore = at(-time.Hour)
	c2.expectReserved("exec-a", c2.submit(past))
}

// Within a priority, the execution that must start soonest goes first, before
// fair share and submission order; a finish_by counts less the estimated run
// time.
func TestPlannerOrdersAPriorityByDeadline(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 1, 16000, 64, 500)
	early := c.submit(planSpec("early", "p1", 5, 1, 1000, 1))
	late := planSpec("late", "p2", 5, 1, 1000, 1)
	late.StartBy = at(10 * time.Hour)
	soon := planSpec("soon", "p3", 5, 1, 1000, 1)
	soon.StartBy = at(time.Hour)
	lateExecution, soonExecution := c.submit(late), c.submit(soon)
	c.expectReserved("exec-a", soonExecution)
	_ = early
	_ = lateExecution

	// finish_by in 3h with a 2h30m run must start before a start_by in 1h.
	c2 := newPlanCluster(t)
	c2.node("a", 1, 16000, 64, 500)
	c2.ran(planSpec("report-1", "p1", 0, 0, 1000, 1), 150*time.Minute)
	starting := planSpec("starting", "p2", 5, 1, 1000, 1)
	starting.StartBy = at(time.Hour)
	report := planSpec("report-2", "p1", 5, 1, 1000, 1)
	report.FinishBy = at(3 * time.Hour)
	c2.submit(starting)
	c2.expectReserved("exec-a", c2.submit(report))
}

// A wait says when a start_by has passed or a finish_by is expected to be
// missed; nothing is stopped for either.
func TestPlannerWaitSaysWhenADeadlineIsMissed(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 1, 16000, 64, 500)
	c.ran(planSpec("job-1", "p1", 0, 0, 1000, 1), 2*time.Hour)
	c.submit(planSpec("running", "p0", 0, 1, 1000, 1))
	c.start("exec-a")
	missed := planSpec("missed", "p2", 5, 1, 1000, 1)
	missed.StartBy = at(-time.Minute)
	ending := planSpec("job-2", "p1", 5, 1, 1000, 1)
	ending.FinishBy = at(time.Hour)
	missedExecution, endingExecution := c.submit(missed), c.submit(ending)
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("reserved %s on a leased GPU", r.Execution.ClientRequestID)
	}
	if w := c.wait(missedExecution.ID); !strings.Contains(w.Message, "its start_by") || !strings.Contains(w.Message, "has passed") {
		t.Fatalf("missed waits for %+v", w)
	}
	if w := c.wait(endingExecution.ID); !strings.Contains(w.Message, "expected to end after its finish_by") {
		t.Fatalf("job-2 waits for %+v", w)
	}
}

// The hold goes where the execution fits by its not_before: a free node, not
// one whose GPUs are leased with no end in sight.
func TestPlannerHoldsForANotBeforeWhereItWillFit(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 2, 16000, 64, 500)
	c.node("b", 2, 16000, 64, 500)
	for _, name := range []string{"busy-1", "busy-2"} {
		c.submit(planSpec(name, "p1", 0, 1, 1000, 1))
		c.start("exec-a")
	}
	spec := planSpec("predict", "rhllem", 10, 1, 1000, 1)
	spec.NotBefore = at(time.Hour)
	predict := c.submit(spec)
	fresh := c.submit(planSpec("fresh", "p4", 0, 2, 1000, 1))
	if r := c.reserve("exec-b"); r != nil {
		t.Fatalf("exec-b reserved %s into the hold of predict", r.Execution.ClientRequestID)
	}
	if w := c.wait(predict.ID); w.NodeID == nil || *w.NodeID != "b" || !strings.Contains(w.Message, "what it needs on b is held for it") {
		t.Fatalf("predict waits for %+v", w)
	}
	if w := c.wait(fresh.ID); w.Reason == "" {
		t.Fatalf("fresh does not wait")
	}
}

// Nothing is suspended for room only a not_before hold takes.
func TestPlannerSuspendsNothingForANotBeforeHold(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 2, 16000, 64, 500)
	c.submit(planSpec("victim", "p1", 0, 1, 1000, 1))
	c.start("exec-a")
	spec := planSpec("predict", "rhllem", 10, 1, 1000, 1)
	spec.NotBefore = at(time.Hour)
	c.submit(spec)
	c.submit(planSpec("mid", "p2", 5, 1, 1000, 1))
	commands, err := c.store.EnsurePriorityPreemption(c.ctx, "exec-a")
	if err != nil || len(commands) != 0 {
		t.Fatalf("preemption for a not_before hold: %v %v", commands, err)
	}
}

// A start_by long past (a rerun days later) neither orders nor is reported.
func TestPlannerForgetsAStaleDeadline(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 1, 16000, 64, 500)
	c.submit(planSpec("running", "p0", 0, 1, 1000, 1))
	c.start("exec-a")
	first := c.submit(planSpec("first", "p1", 5, 1, 1000, 1))
	stale := planSpec("stale", "p2", 5, 1, 1000, 1)
	stale.StartBy = at(-48 * time.Hour)
	staleExecution := c.submit(stale)
	if r := c.reserve("exec-a"); r != nil {
		t.Fatalf("reserved %s on a leased GPU", r.Execution.ClientRequestID)
	}
	if w := c.wait(staleExecution.ID); strings.Contains(w.Message, "start_by") {
		t.Fatalf("stale waits for %+v", w)
	}
	if w := c.wait(first.ID); w.Reason != "gpu_leased" {
		t.Fatalf("first waits for %+v", w)
	}

	// It does not go first either: the earlier submission does.
	c2 := newPlanCluster(t)
	c2.node("a", 1, 16000, 64, 500)
	earlier := c2.submit(planSpec("earlier", "p1", 5, 1, 1000, 1))
	c2.submit(stale)
	c2.expectReserved("exec-a", earlier)
}
