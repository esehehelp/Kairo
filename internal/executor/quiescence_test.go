package executor

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"kairo/internal/store"
)

// fakeQuiescence serves one exited attempt as a quiescence candidate and
// records its finalization.
type fakeQuiescence struct {
	Coordinator
	mu        sync.Mutex
	candidate store.QuiescenceCandidate
	finalized int
}

func (f *fakeQuiescence) ListQuiescenceCandidates(context.Context, string) ([]store.QuiescenceCandidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.finalized > 0 {
		return nil, nil
	}
	return []store.QuiescenceCandidate{f.candidate}, nil
}

func (f *fakeQuiescence) FinalizeQuiescence(context.Context, string, string, int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finalized++
	return nil
}

// An attempt whose registered processes exited is not released while a
// process it started without registering it still runs.
func TestQuiescenceWaitsForWhatTheAttemptLeftRunning(t *testing.T) {
	cmd, handle, child := startTestTree(t, helperOrphan)
	attemptID := "att-" + helperOrphan
	tree := processTree{attemptID: attemptID, pid: cmd.Process.Pid, handle: handle}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if alive, _ := tree.launcherAlive(); !alive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("launcher did not exit")
		}
		time.Sleep(50 * time.Millisecond)
	}
	fake := &fakeQuiescence{candidate: store.QuiescenceCandidate{
		Attempt: store.Attempt{ID: attemptID, State: "exited", CoordinationEpoch: 1},
		Lease:   store.Lease{ID: "lea", CoordinationEpoch: 1, State: "releasing"},
	}}
	e := NewLocal("e", fake, t.TempDir(), slog.Default())
	e.trees = map[string]*treeHandle{attemptID: handle}
	ctx := context.Background()
	if err := e.reconcileQuiescence(ctx); err != nil {
		t.Fatal(err)
	}
	if fake.finalized != 0 {
		t.Fatalf("released while child %d runs", child)
	}
	if err := e.killTree(ctx, processTree{attemptID: attemptID, handle: handle}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		if err := e.reconcileQuiescence(ctx); err != nil {
			t.Fatal(err)
		}
		fake.mu.Lock()
		done := fake.finalized > 0
		fake.mu.Unlock()
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("not released once nothing was left")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// What an exited attempt left running is asked to stop once it has been seen
// for leftoverGrace, killed leftoverKillAfter later, and the lease released
// once nothing is left.
func TestQuiescenceEndsWhatTheAttemptLeftRunning(t *testing.T) {
	savedGrace, savedKill := leftoverGrace, leftoverKillAfter
	leftoverGrace, leftoverKillAfter = 0, 2*time.Second
	t.Cleanup(func() { leftoverGrace, leftoverKillAfter = savedGrace, savedKill })
	cmd, handle, child := startTestTree(t, helperOrphanStub)
	attemptID := "att-" + helperOrphanStub
	tree := processTree{attemptID: attemptID, pid: cmd.Process.Pid, handle: handle}
	deadline := time.Now().Add(10 * time.Second)
	for alive, _ := tree.launcherAlive(); alive; alive, _ = tree.launcherAlive() {
		if time.Now().After(deadline) {
			t.Fatal("launcher did not exit")
		}
		time.Sleep(50 * time.Millisecond)
	}
	fake := &fakeQuiescence{candidate: store.QuiescenceCandidate{
		Attempt: store.Attempt{ID: attemptID, State: "exited", CoordinationEpoch: 1},
		Lease:   store.Lease{ID: "lea", CoordinationEpoch: 1, State: "releasing"},
	}}
	var logs bytes.Buffer
	e := NewLocal("e", fake, t.TempDir(), slog.New(slog.NewTextHandler(&logs, nil)))
	e.trees = map[string]*treeHandle{attemptID: handle}
	ctx := context.Background()
	pass := func() {
		t.Helper()
		if err := e.reconcileQuiescence(ctx); err != nil {
			t.Fatal(err)
		}
	}
	pass() // seen
	pass() // asked to stop: it ignores that
	time.Sleep(300 * time.Millisecond)
	if !processAlive(child) || fake.finalized != 0 || !strings.Contains(logs.String(), "asking what the exited attempt left running to stop") {
		t.Fatalf("after the ask: child alive %v, finalized %d\n%s", processAlive(child), fake.finalized, logs.String())
	}
	deadline = time.Now().Add(20 * time.Second)
	for {
		pass()
		fake.mu.Lock()
		done := fake.finalized > 0
		fake.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not released; child alive %v\n%s", processAlive(child), logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	if processAlive(child) || !strings.Contains(logs.String(), "killed what the exited attempt left running") {
		t.Fatalf("child alive %v\n%s", processAlive(child), logs.String())
	}
}
