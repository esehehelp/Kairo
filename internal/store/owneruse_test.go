package store

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// ownerSeen reports, as node's host provider, its owner's use of resourceID
// (none: not seen now), with an observation of resourceID.
func (c *planCluster) ownerSeen(node, resourceID string, seen bool) {
	c.t.Helper()
	stamp := time.Now().UTC()
	total, free := int64(24*gib), int64(23*gib)
	observations := []Observation{{ResourceID: resourceID, ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano),
		TotalBytes: &total, FreeBytes: &free, Evidence: json.RawMessage(`{}`)}}
	var claims []ExternalClaim
	if seen {
		claims = []ExternalClaim{{ID: "owner-" + resourceID, ResourceID: resourceID, ClaimKind: ClaimOwnerUse, Evidence: json.RawMessage(`{"source":"observed"}`)}}
	}
	if err := c.store.ApplyObservationBatch(c.ctx, node+"-host", nil, observations, claims); err != nil {
		c.t.Fatal(err)
	}
}

func (c *planCluster) ownerClaimActive(id string) bool {
	c.t.Helper()
	var active bool
	if err := c.store.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM external_claims WHERE id=$1 AND cleared_at IS NULL)`, id).Scan(&active); err != nil {
		c.t.Fatal(err)
	}
	return active
}

// suspendsFor is the attempts asked to suspend for their owner's use.
func (c *planCluster) suspendsFor() []string {
	c.t.Helper()
	rows, err := c.store.db.Query(`SELECT a.execution_id FROM commands m JOIN attempts a ON a.id=m.attempt_id WHERE m.kind='suspend' AND m.origin=$1 ORDER BY m.created_at`, ClaimOwnerUse)
	if err != nil {
		c.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			c.t.Fatal(err)
		}
		out = append(out, id)
	}
	return out
}

func (c *planCluster) reconcileOwners() int {
	c.t.Helper()
	n, err := c.store.ReconcileOwnerUse(c.ctx)
	if err != nil {
		c.t.Fatal(err)
	}
	return n
}

// The work holding a GPU its owner uses, that can checkpoint, is asked to
// suspend once; nothing new is placed on that GPU meanwhile (owner_use).
func TestTheOwnersUseOfAGPUMakesItsWorkGiveWay(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 1, 8000, 32, 100)
	c.node("b", 1, 8000, 32, 100)
	held := c.submit(planSpec("held", "p", 1, 1, 1000, 1))
	if got := c.start("exec-a"); got == nil || got.Execution.ID != held.ID {
		t.Fatalf("started %+v", got)
	}
	c.ownerSeen("a", "gpu-GPU-a-0", true)
	if n := c.reconcileOwners(); n != 1 {
		t.Fatalf("%d suspends asked", n)
	}
	if got := c.suspendsFor(); len(got) != 1 || got[0] != held.ID {
		t.Fatalf("suspends %v", got)
	}
	if n := c.reconcileOwners(); n != 0 {
		t.Fatalf("asked again: %d", n)
	}
	// node b's GPU its owner uses too: nothing new goes there
	c.ownerSeen("b", "gpu-GPU-b-0", true)
	next := c.submit(planSpec("next", "p", 1, 1, 1000, 1))
	if got := c.reserve("exec-b"); got != nil {
		t.Fatalf("placed on a GPU its owner uses: %+v", got.Execution.ID)
	}
	var reasonB string
	if err := c.store.db.QueryRow(`SELECT reason_code FROM admission_waits WHERE execution_id=$1`, next.ID).Scan(&reasonB); err != nil {
		t.Fatal(err)
	}
	if reasonB != "owner_use" && reasonB != "gpu_leased" {
		t.Fatalf("reason %s", reasonB)
	}
}

// An observed owner's use holds ownerUseHold after it was last seen, even
// once its provider no longer reports it; then it ends. One set by hand
// holds until owner-done or its until, whatever the providers report.
func TestTheOwnersUseHoldsForAWhileAfterItWasLastSeen(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 1, 8000, 32, 100)
	c.ownerSeen("a", "gpu-GPU-a-0", true)
	c.ownerSeen("a", "gpu-GPU-a-0", false)
	if !c.ownerClaimActive("owner-gpu-GPU-a-0") {
		t.Fatal("ended as soon as it was not seen")
	}
	stale := time.Now().UTC().Add(-ownerUseHold - time.Minute).Format(time.RFC3339Nano)
	if _, err := c.store.testExec(`UPDATE external_claims SET last_observed_at=$1 WHERE id='owner-gpu-GPU-a-0'`, stale); err != nil {
		t.Fatal(err)
	}
	c.ownerSeen("a", "gpu-GPU-a-0", false)
	if c.ownerClaimActive("owner-gpu-GPU-a-0") {
		t.Fatal("held past its hold")
	}
	// gone quiet: the reconcile ends it
	c.ownerSeen("a", "gpu-GPU-a-0", true)
	if _, err := c.store.testExec(`UPDATE external_claims SET last_observed_at=$1 WHERE id='owner-gpu-GPU-a-0'`, stale); err != nil {
		t.Fatal(err)
	}
	c.reconcileOwners()
	if c.ownerClaimActive("owner-gpu-GPU-a-0") {
		t.Fatal("held past its hold with its provider quiet")
	}

	// by hand
	if _, err := c.store.UseNodeAsOwner(c.ctx, "a", OwnerUseRequest{GPUs: []string{"GPU-x"}}); !errors.Is(err, ErrOwnerUseRefused) {
		t.Fatalf("a GPU the node lacks: %v", err)
	}
	if _, err := c.store.UseNodeAsOwner(c.ctx, "a", OwnerUseRequest{Until: "2020-01-01T00:00:00Z"}); !errors.Is(err, ErrOwnerUseRefused) {
		t.Fatalf("a time passed: %v", err)
	}
	uses, err := c.store.UseNodeAsOwner(c.ctx, "a", OwnerUseRequest{Actor: "owner"})
	if err != nil || len(uses) != 3 {
		t.Fatalf("every resource of the node: %+v %v", uses, err)
	}
	c.ownerSeen("a", "gpu-GPU-a-0", false)
	if !c.ownerClaimActive("owner-manual-gpu-GPU-a-0") || !c.ownerClaimActive("owner-manual-a-cpu") {
		t.Fatal("ended by a provider")
	}
	if n, err := c.store.OwnerDone(c.ctx, "a", "owner"); err != nil || n != 3 {
		t.Fatalf("done: %d %v", n, err)
	}
	if c.ownerClaimActive("owner-manual-gpu-GPU-a-0") {
		t.Fatal("still held after owner-done")
	}
	until := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	if _, err := c.store.UseNodeAsOwner(c.ctx, "a", OwnerUseRequest{RAM: true, Until: until}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.store.testExec(`UPDATE external_claims SET evidence_json=replace(evidence_json,$1,'2020-01-01T00:00:00Z') WHERE id='owner-manual-a-ram'`, until); err != nil {
		t.Fatal(err)
	}
	c.reconcileOwners()
	if c.ownerClaimActive("owner-manual-a-ram") {
		t.Fatal("held past its until")
	}
}

// Its owner's use of a node's CPU suspends the work there that holds the
// most of it, one at a time while it goes on; the node takes nothing new.
// Set by hand, every piece of work there that can checkpoint gives way.
func TestTheOwnersUseOfACPUMakesTheBiggestGiveWayFirst(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 0, 8000, 32, 100)
	small := c.submit(planSpec("small", "p", 1, 0, 1000, 1))
	if got := c.start("exec-a"); got == nil || got.Execution.ID != small.ID {
		t.Fatalf("started %+v", got)
	}
	big := c.submit(planSpec("big", "p", 1, 0, 4000, 1))
	if got := c.start("exec-a"); got == nil || got.Execution.ID != big.ID {
		t.Fatalf("started %+v", got)
	}
	c.ownerSeen("a", "a-cpu", true)
	c.reconcileOwners()
	c.reconcileOwners()
	if got := c.suspendsFor(); len(got) != 1 || got[0] != big.ID {
		t.Fatalf("suspends %v, want %s alone", got, big.ID)
	}
	waiting := c.submit(planSpec("waiting", "p", 1, 0, 1000, 1))
	if got := c.reserve("exec-a"); got != nil {
		t.Fatalf("placed on a node whose CPU its owner uses: %s", got.Execution.ID)
	}
	var reason, detail string
	if err := c.store.db.QueryRow(`SELECT reason_code,detail_json FROM admission_waits WHERE execution_id=$1`, waiting.ID).Scan(&reason, &detail); err != nil || reason != "owner_use" || !strings.Contains(detail, "its owner is using its cpu") {
		t.Fatalf("wait %s %s %v", reason, detail, err)
	}
	if _, err := c.store.UseNodeAsOwner(c.ctx, "a", OwnerUseRequest{CPU: true}); err != nil {
		t.Fatal(err)
	}
	c.reconcileOwners()
	if got := c.suspendsFor(); len(got) != 2 || got[1] != small.ID {
		t.Fatalf("suspends %v after owner-use", got)
	}
}
