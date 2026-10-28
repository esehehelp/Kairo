package store

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// Writers run one at a time (beginWrite takes the decision lock), so
// deciding operations racing on a pool of connections end as if they had
// run one after the other.

// Many planning passes race for one GPU that two waiting executions want:
// exactly one reserves it, every round, as in any serial order.
func TestRacingReservationsForOneGPUReserveItOnce(t *testing.T) {
	c := newPlanCluster(t)
	c.node("n", 1, 16000, 64, 500)
	if err := c.store.UpsertExecutor(c.ctx, Executor{ID: "exec-n-2", NodeID: "n", Kind: "linux", Attributes: json.RawMessage(`{"site":"n","pool":"all"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	c.submit(planSpec("first", "p", 5, 1, 1000, 4))
	c.submit(planSpec("second", "p", 5, 1, 1000, 4))
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		var mu sync.Mutex
		var reserved []*Reservation
		start := make(chan struct{})
		for i := 0; i < 16; i++ {
			executor := "exec-n"
			if i%2 == 1 {
				executor = "exec-n-2"
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				r, err := c.store.ReserveNext(c.ctx, executor)
				if err != nil {
					t.Errorf("round %d: reserve on %s: %v", round, executor, err)
					return
				}
				if r != nil {
					mu.Lock()
					reserved = append(reserved, r)
					mu.Unlock()
				}
			}()
		}
		close(start)
		wg.Wait()
		if len(reserved) != 1 {
			t.Fatalf("round %d: %d reservations of the one GPU", round, len(reserved))
		}
		var live, gpuItems int
		if err := c.store.db.QueryRow(`SELECT COUNT(*),COUNT(*) FILTER (WHERE li.kind='gpu') FROM leases l JOIN lease_items li ON li.lease_id=l.id WHERE l.state<>'released' AND li.kind='gpu'`).Scan(&live, &gpuItems); err != nil || live != 1 || gpuItems != 1 {
			t.Fatalf("round %d: %d live GPU lease items: %v", round, live, err)
		}
		r := reserved[0]
		c.observe("gpu-GPU-n-0", 24*gib, 23*gib) // (release needs an observation after the lease)
		if err := c.store.ReleaseReservation(c.ctx, r.Lease.ID, r.Lease.CoordinationEpoch, "test round over"); err != nil {
			t.Fatal(err)
		}
		c.observe("gpu-GPU-n-0", 24*gib, 23*gib)
	}
}

// The same submission sent many times at once makes one execution; every
// other answer is a duplicate of it.
func TestRacingIdenticalSubmissionsMakeOneExecution(t *testing.T) {
	store := openCoordinationStore(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ids := map[string]int{}
	fresh := 0
	start := make(chan struct{})
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// (each its own spec: normalization edits it)
			execution, duplicate, err := store.SubmitExecution(context.Background(), testExecutionSpec("raced"))
			if err != nil {
				t.Errorf("submit: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			ids[execution.ID]++
			if !duplicate {
				fresh++
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(ids) != 1 || fresh != 1 {
		t.Fatalf("executions %v, %d not duplicates", ids, fresh)
	}
}

// A writing transaction holds the decision lock until it ends: another
// write waits for it, a read does not. A write that does not go through
// beginWrite is refused.
func TestWritersWaitForEachOtherAndReadersDoNot(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	if err := store.UpsertNode(ctx, Node{ID: "n", Name: "n", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	holder, err := store.beginWrite(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err = holder.ExecContext(ctx, `UPDATE nodes SET name='held' WHERE id='n'`); err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() {
		_, err := store.exec(ctx, `UPDATE nodes SET os='windows' WHERE id='n'`)
		written <- err
	}()
	var name string
	if err = store.db.QueryRowContext(ctx, `SELECT name FROM nodes WHERE id='n'`).Scan(&name); err != nil || name != "n" {
		t.Fatalf("a read during the write saw %q: %v", name, err)
	}
	select {
	case err := <-written:
		t.Fatalf("a second write went through while the first held the lock: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err = holder.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-written; err != nil {
		t.Fatal(err)
	}
	var os string
	if err = store.db.QueryRowContext(ctx, `SELECT name,os FROM nodes WHERE id='n'`).Scan(&name, &os); err != nil || name != "held" || os != "windows" {
		t.Fatalf("after both writes: %q %q %v", name, os, err)
	}
	if _, err = store.db.ExecContext(ctx, `UPDATE nodes SET os='linux' WHERE id='n'`); err == nil {
		t.Fatal("a write outside beginWrite was accepted")
	}
}
