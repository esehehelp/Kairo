package executor

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"kairo/internal/store"
)

// fakeTerminations serves one termination (node quarantine or gang) and
// records when the executor reports it carried out.
type fakeTerminations struct {
	Coordinator
	mu     sync.Mutex
	term   store.QuarantineTermination
	marked []string
}

func (f *fakeTerminations) ListQuarantineTerminations(context.Context, string) ([]store.QuarantineTermination, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.marked) > 0 {
		return nil, nil
	}
	return []store.QuarantineTermination{f.term}, nil
}

func (f *fakeTerminations) MarkQuarantineTerminated(_ context.Context, attemptID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, attemptID)
	return nil
}

func (f *fakeTerminations) done() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.marked) > 0
}

// A termination whose launcher already exited still ends what the launcher
// left behind (found through the job object, or the processes carrying the
// attempt's KAIRO_ATTEMPT_ID), and is recorded only once nothing is left.
func TestTerminationEndsWhatTheLauncherLeftBehindBeforeItIsRecorded(t *testing.T) {
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
	fake := &fakeTerminations{term: store.QuarantineTermination{AttemptID: attemptID, ExecutionID: "exe"}}
	e := NewLocal("e", fake, t.TempDir(), slog.Default())
	e.running[attemptID] = cmd.Process.Pid
	e.trees = map[string]*treeHandle{attemptID: handle}
	ctx := context.Background()
	if err := e.carryOutQuarantineTerminations(ctx); err != nil {
		t.Fatal(err)
	}
	if fake.done() {
		t.Fatal("recorded as terminated while the left-behind child ran")
	}
	deadline = time.Now().Add(terminationGrace + 20*time.Second)
	for !fake.done() {
		if time.Now().After(deadline) {
			t.Fatal("termination never recorded")
		}
		time.Sleep(200 * time.Millisecond)
		if err := e.carryOutQuarantineTerminations(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if processAlive(child) {
		t.Fatalf("child %d survived the termination", child)
	}
}
