package store

import (
	"encoding/json"
	"testing"
)

// An attempt that rejected a preemption suspend is not chosen again (it would
// not be asked again either): another cooperative victim makes the room.
func TestPreemptionTurnsToAnotherVictimAfterARejection(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 2, 16000, 64, 100)
	c.submit(planSpec("old", "p", 1, 1, 1000, 1))
	old := c.start("exec-a")
	c.submit(planSpec("young", "p", 1, 1, 1000, 1))
	young := c.start("exec-a")
	c.submit(planSpec("waiter", "p", 10, 1, 1000, 1))

	commands, err := c.store.EnsurePriorityPreemption(c.ctx, "exec-a")
	if err != nil || len(commands) != 1 {
		t.Fatalf("first preemption: %v %v", commands, err)
	}
	var attempt string
	if err = c.store.db.QueryRow(`SELECT attempt_id FROM commands WHERE id=$1`, commands[0]).Scan(&attempt); err != nil || attempt != young.Attempt.ID {
		t.Fatalf("the youngest victim goes first: %s %v", attempt, err)
	}
	if err = c.store.AckCommand(c.ctx, young.Attempt.ID, young.Lease.ID, young.Lease.CoordinationEpoch, commands[0], "rejected", json.RawMessage(`{"reason":"not now"}`)); err != nil {
		t.Fatal(err)
	}
	commands, err = c.store.EnsurePriorityPreemption(c.ctx, "exec-a")
	if err != nil || len(commands) != 1 {
		t.Fatalf("after the rejection: %v %v", commands, err)
	}
	if err = c.store.db.QueryRow(`SELECT attempt_id FROM commands WHERE id=$1`, commands[0]).Scan(&attempt); err != nil || attempt != old.Attempt.ID {
		t.Fatalf("the other victim was not chosen: %s %v", attempt, err)
	}
}
