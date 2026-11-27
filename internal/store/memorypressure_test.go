package store

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"
)

// ramSeen reports, as node's host provider, its RAM's observation: free of
// its 32 GiB, the floor of 3 GiB and, unless since is zero, since when less
// than the floor is available; valid for a minute after at.
func (c *planCluster) ramSeen(node string, free int64, since, at time.Time) {
	c.t.Helper()
	total := int64(32 * gib)
	evidence := map[string]any{"floor_bytes": 3 * gib}
	if !since.IsZero() {
		evidence["under_floor_since"] = since.UTC().Format(time.RFC3339Nano)
	}
	body, _ := json.Marshal(evidence)
	observations := []Observation{{ResourceID: node + "-ram", ObservedAt: at.UTC().Format(time.RFC3339Nano), ValidUntil: at.Add(time.Minute).UTC().Format(time.RFC3339Nano),
		TotalBytes: &total, FreeBytes: &free, Evidence: body}}
	if err := c.store.ApplyObservationBatch(c.ctx, node+"-host", nil, observations, nil); err != nil {
		c.t.Fatal(err)
	}
}

func (c *planCluster) reconcilePressure() int {
	c.t.Helper()
	n, err := c.store.ReconcileMemoryPressure(c.ctx)
	if err != nil {
		c.t.Fatal(err)
	}
	return n
}

// suspendsOf is the executions asked to suspend for origin, in order.
func (c *planCluster) suspendsOf(origin string) []string {
	c.t.Helper()
	var out []string
	err := eachRow(c.ctx, c.store.db, `SELECT a.execution_id FROM commands m JOIN attempts a ON a.id=m.attempt_id WHERE m.kind='suspend' AND m.origin=$1 ORDER BY m.created_at`,
		[]any{origin}, func(rows *sql.Rows) error {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			out = append(out, id)
			return nil
		})
	if err != nil {
		c.t.Fatal(err)
	}
	return out
}

// A node under its RAM floor for a while gets RAM back from its work that
// can checkpoint, the attempt holding the most first and one at a time; a
// short dip, or an observation no longer valid, gives nothing back. It is no
// owner's use: no claim, and nothing more is held off the node.
func TestANodeShortOfRAMGetsItBackFromItsBiggestWorkFirst(t *testing.T) {
	c := newPlanCluster(t)
	c.node("a", 0, 8000, 32, 100)
	small := c.submit(planSpec("small", "p", 1, 0, 1000, 1))
	if got := c.start("exec-a"); got == nil || got.Execution.ID != small.ID {
		t.Fatalf("started %+v", got)
	}
	big := c.submit(planSpec("big", "p", 1, 0, 1000, 4))
	if got := c.start("exec-a"); got == nil || got.Execution.ID != big.ID {
		t.Fatalf("started %+v", got)
	}
	now := time.Now()
	c.ramSeen("a", 2*gib, now.Add(-10*time.Second), now)
	if n := c.reconcilePressure(); n != 0 {
		t.Fatalf("a short dip: %d suspends", n)
	}
	c.ramSeen("a", 2*gib, now.Add(-time.Hour), now.Add(-2*time.Minute))
	if n := c.reconcilePressure(); n != 0 {
		t.Fatalf("an observation no longer valid: %d suspends", n)
	}
	c.ramSeen("a", 2*gib, now.Add(-time.Minute), now)
	c.reconcilePressure()
	c.reconcilePressure()
	if got := c.suspendsOf(OriginMemoryPressure); len(got) != 1 || got[0] != big.ID {
		t.Fatalf("suspends %v, want %s alone", got, big.ID)
	}
	var claims int
	if err := c.store.db.QueryRow(`SELECT COUNT(*) FROM external_claims WHERE cleared_at IS NULL`).Scan(&claims); err != nil || claims != 0 {
		t.Fatalf("%d claims %v", claims, err)
	}
	var reason string
	if err := c.store.db.QueryRow(`SELECT reason FROM commands WHERE origin=$1`, OriginMemoryPressure).Scan(&reason); err != nil || reason == "" {
		t.Fatalf("reason %q %v", reason, err)
	}
	// the first checkpointed, the node still under its floor: the next
	if _, err := c.store.testExec(`UPDATE commands SET state='checkpointed' WHERE origin=$1`, OriginMemoryPressure); err != nil {
		t.Fatal(err)
	}
	c.reconcilePressure()
	if got := c.suspendsOf(OriginMemoryPressure); len(got) != 2 || got[1] != small.ID {
		t.Fatalf("suspends %v, then %s", got, small.ID)
	}
}
