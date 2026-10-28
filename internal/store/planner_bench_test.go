package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"

	"kairo/internal/pgtest"
)

// statementCounter counts the statements sent to the server (each one
// round trip once prepared: begin, the lock, every query, commit).
type statementCounter struct{ n atomic.Int64 }

func (c *statementCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	c.n.Add(1)
	return ctx
}

func (c *statementCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// benchCluster is the installation the store benchmarks run on: 4 nodes
// with 2 GPUs each, every GPU held by a running execution, and 200 waiting
// executions (4 projects) that each want a GPU. A planning pass places
// nothing, so every pass is the same full pass: load the model, try every
// candidate, record the waits.
func benchCluster(b *testing.B) (*planCluster, *Launch, *statementCounter) {
	b.Helper()
	counter := &statementCounter{}
	st, err := open(pgtest.DSN(b), true, counter)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { st.Close() })
	c := &planCluster{t: b, store: st, ctx: context.Background()}
	for n := 0; n < 4; n++ {
		c.node(fmt.Sprintf("n%d", n), 2, 32000, 128, 1000)
	}
	for i := 0; i < 8; i++ {
		c.submit(planSpec(fmt.Sprintf("run-%d", i), "p0", 5, 1, 1000, 4))
	}
	var running *Launch
	for n := 0; n < 4; n++ {
		for {
			r, err := c.store.ReserveNext(c.ctx, fmt.Sprintf("exec-n%d", n))
			if err != nil {
				b.Fatal(err)
			}
			if r == nil {
				break
			}
			if err = c.store.MarkLeasePrepared(c.ctx, r.Lease.ID, r.Lease.CoordinationEpoch); err != nil {
				b.Fatal(err)
			}
			launch, err := c.store.AuthorizeLaunch(c.ctx, r)
			if err != nil {
				b.Fatal(err)
			}
			if err = c.store.ActivateLaunch(c.ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 4000, "proc:4000:"+launch.Attempt.ID); err != nil {
				b.Fatal(err)
			}
			running = launch
		}
	}
	if running == nil {
		b.Fatal("setup: nothing was started")
	}
	for i := 0; i < 200; i++ {
		c.submit(planSpec(fmt.Sprintf("wait-%d", i), fmt.Sprintf("p%d", i%4), 5, 1, 1000, 4))
	}
	if r, err := c.store.ReserveNext(c.ctx, "exec-n0"); err != nil || r != nil {
		b.Fatalf("setup: a pass reserved %v, err %v", r, err)
	}
	return c, running, counter
}

// perOp reports the statements (round trips) one operation took.
func perOp(b *testing.B, counter *statementCounter, before int64) {
	b.ReportMetric(float64(counter.n.Load()-before)/float64(b.N), "stmts/op")
}

// BenchmarkPlannerPass: one full planning pass (ReserveNext placing nothing).
func BenchmarkPlannerPass(b *testing.B) {
	c, _, counter := benchCluster(b)
	before := counter.n.Load()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if r, err := c.store.ReserveNext(c.ctx, "exec-n0"); err != nil || r != nil {
			b.Fatalf("pass reserved %v, err %v", r, err)
		}
	}
	perOp(b, counter, before)
}

// BenchmarkHeartbeat: an attempt's heartbeat (the hottest worker API call).
func BenchmarkHeartbeat(b *testing.B) {
	c, launch, counter := benchCluster(b)
	progress := json.RawMessage(`{"unit":"step","current":5}`)
	before := counter.n.Load()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.store.Heartbeat(c.ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, progress); err != nil {
			b.Fatal(err)
		}
	}
	perOp(b, counter, before)
}

// BenchmarkListExecutions: the execution list of the API (100 rows).
func BenchmarkListExecutions(b *testing.B) {
	c, _, counter := benchCluster(b)
	before := counter.n.Load()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if list, err := c.store.ListExecutions(c.ctx, ExecutionFilter{State: "waiting", Limit: 100}); err != nil || len(list) != 100 {
			b.Fatalf("listed %d, err %v", len(list), err)
		}
	}
	perOp(b, counter, before)
}

// BenchmarkRoundTrip: one statement to the server and back (what each
// stmts/op above costs on top of the work).
func BenchmarkRoundTrip(b *testing.B) {
	store := openCoordinationStore(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var one int
		if err := store.db.QueryRow(`SELECT 1`).Scan(&one); err != nil {
			b.Fatal(err)
		}
	}
}
