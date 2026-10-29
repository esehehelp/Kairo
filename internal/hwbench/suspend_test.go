package hwbench

import (
	"context"
	"slices"
	"testing"

	"kairo/internal/store"
)

// kindPoints counts the points of kind d holds.
func kindPoints(d *fakeDaemon, kind string) int {
	n := 0
	for _, p := range d.points {
		if p.Kind == kind {
			n++
		}
	}
	return n
}

// startedKinds are the kinds r started a primitive of.
func startedKinds(r *fakeRunner) []string {
	var kinds []string
	for _, argv := range r.argvs {
		p := &fakeProcess{argv: argv}
		kinds = append(kinds, p.workload())
	}
	return kinds
}

func TestASuspendIsAcknowledgedAtTheNextPointAndAContinuationFinishesTheRun(t *testing.T) {
	ctx := context.Background()
	d := newFakeDaemon(gpuRun("pcie-h2d", "gemm", "copy"), gpuCaps())
	// Preempted while gemm is measured: the suspend is there once two of its
	// points are in.
	d.onCommands = func(_ int, d *fakeDaemon) {
		if kindPoints(d, "gemm") == 2 && len(d.commands) == 0 && len(d.acks) == 0 {
			d.commands = append(d.commands, store.Command{ID: "cmd-1", Kind: "suspend", Origin: "priority_preemption", State: "pending"})
		}
	}
	r := &fakeRunner{d: d}
	if err := testDriver(d, r).Run(ctx, "bnc-1"); err != nil {
		t.Fatal(err)
	}
	// Acknowledged as an SDK's safe point does, at the next point: nothing
	// more of gemm is measured, copy is not started, the run not finished.
	var phases []string
	for _, a := range d.acks {
		if a.command != "cmd-1" {
			t.Fatalf("ack of %s", a.command)
		}
		phases = append(phases, a.phase)
	}
	if !slices.Equal(phases, []string{"accepted", "checkpointing", "checkpointed"}) {
		t.Fatalf("acks: %+v", d.acks)
	}
	if kindPoints(d, "gemm") != 2 || !slices.Equal(startedKinds(r), []string{"pcie", "gemm"}) || d.finish != nil {
		t.Fatalf("after the suspend: %d gemm points, kinds %v, finish %+v", kindPoints(d, "gemm"), startedKinds(r), d.finish)
	}
	ref, _ := d.acks[2].payload["continuation_ref"].(string)
	c, err := ParseContinuation(ref)
	if err != nil || c.Run != "bnc-1" || !slices.Equal(c.Done, []string{"pcie-h2d"}) || !slices.Equal(d.recorded, []string{"pcie-h2d"}) {
		t.Fatalf("continuation %q: %+v %v (recorded %v)", ref, c, err, d.recorded)
	}
	if last := d.sets[len(d.sets)-1]; !last.IsDefault() {
		t.Fatalf("the bench setting was left: %s", last)
	}

	// The continuation: the store kept pcie-h2d's reference and point, and
	// dropped gemm's points (benchTaskExecutionTx).
	next := newFakeDaemon(gpuRun("pcie-h2d", "gemm", "copy"), gpuCaps())
	next.status.Run.References = map[string]store.BenchReference{"pcie-h2d": d.status.Run.References["pcie-h2d"]}
	for seq, p := range d.points {
		if p.Kind == "pcie-h2d" {
			next.points[seq] = p
			next.status.NextSeq = max(next.status.NextSeq, seq+1)
		}
	}
	nr := &fakeRunner{d: next}
	driver := testDriver(next, nr)
	driver.Continuation = ref
	if err = driver.Run(ctx, "bnc-1"); err != nil {
		t.Fatal(err)
	}
	// The kinds done are not measured again; gemm is, from the start.
	if !slices.Equal(startedKinds(nr), []string{"gemm", "copy"}) || len(next.acks) != 0 {
		t.Fatalf("resumed: kinds %v, acks %+v", startedKinds(nr), next.acks)
	}
	if next.finish == nil || !next.finish.Valid || next.finish.Reason != "" || len(next.finish.References) != 3 {
		t.Fatalf("finish: %+v", next.finish)
	}
	// 20 gemm points (3 references), 16 copy points, after pcie-h2d's one.
	if kindPoints(next, "gemm") != 20 || kindPoints(next, "copy") != 16 || kindPoints(next, "pcie-h2d") != 1 || next.points[next.status.NextSeq].Kind != "gemm" {
		t.Fatalf("points: gemm %d, copy %d, pcie-h2d %d", kindPoints(next, "gemm"), kindPoints(next, "copy"), kindPoints(next, "pcie-h2d"))
	}
	for _, p := range next.points {
		if p.Seq < next.status.NextSeq && p.Kind != "pcie-h2d" {
			t.Fatalf("a point replaced one the run had: %+v", p)
		}
	}
}

func TestASuspendDuringTheWarmUpNeedNotWaitForIt(t *testing.T) {
	d := newFakeDaemon(gpuRun("gemm"), gpuCaps())
	// Poll 1 is before the kind, 2 is 15 s into its warm-up.
	d.suspendAt(2)
	r := &fakeRunner{d: d}
	if err := testDriver(d, r).Run(context.Background(), "bnc-1"); err != nil {
		t.Fatal(err)
	}
	if len(d.acks) != 3 || d.acks[2].phase != "checkpointed" || len(d.points) != 0 || d.finish != nil || len(d.recorded) != 0 {
		t.Fatalf("acks %+v, %d points, finish %+v", d.acks, len(d.points), d.finish)
	}
	c, err := ParseContinuation(d.acks[2].payload["continuation_ref"].(string))
	if err != nil || len(c.Done) != 0 {
		t.Fatalf("continuation: %+v %v", c, err)
	}
}

func TestOtherCommandsAndWithdrawnSuspendsAreSkipped(t *testing.T) {
	d := newFakeDaemon(gpuRun("pcie-h2d", "pcie-d2h"), gpuCaps())
	d.onCommands = func(polls int, d *fakeDaemon) {
		if polls == 1 {
			d.commands = append(d.commands, store.Command{ID: "cmd-x", Kind: "inspect"})
		}
	}
	// A suspend the daemon no longer has (withdrawn) is 404 at its ack.
	daemon := &withdrawing{fakeDaemon: d}
	if err := testDriver(d, &fakeRunner{d: d}).withDaemon(daemon).Run(context.Background(), "bnc-1"); err != nil {
		t.Fatal(err)
	}
	if d.finish == nil || !d.finish.Valid || len(d.acks) != 0 {
		t.Fatalf("finish %+v, acks %+v", d.finish, d.acks)
	}
}

// withdrawing delivers a suspend at every poll that is withdrawn before its
// acknowledgement.
type withdrawing struct{ *fakeDaemon }

func (w *withdrawing) Commands(ctx context.Context) ([]store.Command, error) {
	commands, err := w.fakeDaemon.Commands(ctx)
	return append(commands, store.Command{ID: "cmd-gone", Kind: "suspend"}), err
}

func (d *Driver) withDaemon(daemon Daemon) *Driver {
	d.Daemon = daemon
	return d
}

func TestAStopSignalLeavesTheRunWithoutACheckpoint(t *testing.T) {
	d := newFakeDaemon(gpuRun("gemm", "copy"), gpuCaps())
	r := &fakeRunner{d: d}
	driver := testDriver(d, r)
	stopped := false
	driver.Stopping = func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		// The signal comes once three gemm points are in.
		stopped = stopped || kindPoints(d, "gemm") >= 3
		return stopped
	}
	if err := driver.Run(context.Background(), "bnc-1"); err != nil {
		t.Fatal(err)
	}
	// Left at once (the point being measured dropped), nothing published: a
	// force stop plans no continuation.
	if kindPoints(d, "gemm") != 3 || len(d.acks) != 0 || d.finish != nil || len(d.recorded) != 0 || !slices.Equal(startedKinds(r), []string{"gemm"}) {
		t.Fatalf("%d gemm points, acks %+v, finish %+v, kinds %v", kindPoints(d, "gemm"), d.acks, d.finish, startedKinds(r))
	}
	if last := d.sets[len(d.sets)-1]; !last.IsDefault() {
		t.Fatalf("the bench setting was left: %s", last)
	}
}
