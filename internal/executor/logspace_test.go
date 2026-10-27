package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"kairo/internal/store"
)

// logSpaceStore is a store with an executor "e" on node "n" and one
// execution of task p/q/t that runs the test binary in mode.
func logSpaceStore(t *testing.T, mode string) *store.Store {
	t.Helper()
	ctx := context.Background()
	st := openProviderStore(t)
	if err := st.UpsertExecutor(ctx, store.Executor{ID: "e", NodeID: "n", Kind: "local", Attributes: json.RawMessage(`{"environment":"log-test"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.SubmitExecution(ctx, store.ExecutionSpec{
		ClientRequestID: "log-space", Scope: store.ScopePath{Project: "p", Queue: "q", Task: "t"},
		Argv: []string{os.Args[0], mode}, CWD: t.TempDir(), ExecutorSelector: json.RawMessage(`{"labels":{"environment":"log-test"}}`),
	}); err != nil {
		t.Fatal(err)
	}
	return st
}

func logSpaceExecutor(st Coordinator, logDir string, free func(string) (int64, error)) *Local {
	e := NewLocal("e", st, logDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	e.NodeID, e.Kind = "n", "local"
	e.Attributes = map[string]string{"environment": "log-test"}
	e.MinLogFreeBytes = 1 << 30
	e.FreeSpace = free
	return e
}

// An executor whose log filesystem has less than the minimum free does not
// launch: the execution ends as a launch failure that names the shortage, and
// no process (nor log file) is started.
func TestLaunchIsRefusedWhileTheLogFilesystemIsShortOfSpace(t *testing.T) {
	ctx := context.Background()
	st := logSpaceStore(t, helperPrint)
	logDir := t.TempDir()
	var asked atomic.Value
	e := logSpaceExecutor(st, logDir, func(dir string) (int64, error) {
		asked.Store(dir)
		return 10 << 20, nil
	})
	if err := e.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if asked.Load() != logDir {
		t.Fatalf("free space asked of %v, not the log directory", asked.Load())
	}
	executions, err := st.ListExecutions(ctx, store.ExecutionFilter{})
	if err != nil || len(executions) != 1 {
		t.Fatalf("executions: %+v %v", executions, err)
	}
	want := "launch_aborted: launch failed before activation: the filesystem of the attempt log directory " + logDir + " has 10.0 MiB free, below the minimum of 1.0 GiB (log_min_free_mib)"
	if x := executions[0]; x.State != "terminal" || x.TerminalCause == nil || *x.TerminalCause != want {
		t.Fatalf("execution: %s %v, want cause %q", x.State, x.TerminalCause, want)
	}
	if files, _ := os.ReadDir(logDir); len(files) != 0 {
		t.Fatalf("log files of a refused launch: %v", files)
	}

	// A measurement that fails refuses the launch as well.
	st = logSpaceStore(t, helperPrint)
	e = logSpaceExecutor(st, t.TempDir(), func(string) (int64, error) { return 0, errors.New("statfs: input/output error") })
	if err := e.tick(ctx); err != nil {
		t.Fatal(err)
	}
	executions, _ = st.ListExecutions(ctx, store.ExecutionFilter{})
	if x := executions[0]; x.TerminalCause == nil || !strings.HasSuffix(*x.TerminalCause, "the free space for attempt logs is unknown: statfs: input/output error") {
		t.Fatalf("cause: %v", x.TerminalCause)
	}
}

// An attempt that exits with its log filesystem full gets the problem
// recorded on it, and the executor ships its logs before recording the exit.
func TestAttemptExitRecordsAFullLogFilesystemAndFlushesLogsFirst(t *testing.T) {
	ctx := context.Background()
	st := logSpaceStore(t, helperPrint)
	logDir := t.TempDir()
	var checks atomic.Int32
	e := logSpaceExecutor(st, logDir, func(string) (int64, error) {
		if checks.Add(1) == 1 {
			return 2 << 30, nil // at launch
		}
		return 0, nil // filled up while it ran
	})
	type flush struct {
		attempt string
		exited  bool
		stdout  string
	}
	flushes := make(chan flush, 1)
	e.FlushLogs = func(ctx context.Context, attemptID string) error {
		attempts, err := st.ListAttempts(ctx, store.AttemptFilter{})
		if err != nil {
			return err
		}
		body, _ := os.ReadFile(filepath.Join(logDir, attemptID+".stdout.log"))
		flushes <- flush{attemptID, attempts[0].ExitCode != nil, string(body)}
		return nil
	}
	if err := e.tick(ctx); err != nil {
		t.Fatal(err)
	}
	var attempt store.AttemptView
	waitFor(t, "the attempt's exit", func() bool {
		attempts, err := st.ListAttempts(ctx, store.AttemptFilter{})
		if err != nil || len(attempts) != 1 || attempts[0].ExitCode == nil {
			return false
		}
		attempt = attempts[0]
		return true
	})
	f := <-flushes
	if f.attempt != attempt.ID || f.exited || f.stdout != "hello from the attempt\n" {
		t.Fatalf("flush: %+v (attempt %s)", f, attempt.ID)
	}
	events, err := st.ListEvents(ctx, store.EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	var problem *store.LogProblem
	for _, event := range events {
		if event.EventType == "attempt_log_problem" && event.AggregateID == attempt.ID {
			problem = &store.LogProblem{}
			_ = json.Unmarshal(event.Payload, problem)
		}
	}
	if problem == nil || problem.Problem != store.LogSpaceExhausted || problem.Detail != logDir+" has 0 B free; writes to the logs fail (at exit)" {
		t.Fatalf("problem: %+v", problem)
	}
}

func TestDiskFreeMeasuresTheLogDirectory(t *testing.T) {
	free, err := diskFree(t.TempDir())
	if err != nil || free <= 0 {
		t.Fatalf("free=%d err=%v", free, err)
	}
	if _, err := diskFree(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("a missing directory has free space")
	}
}

// fakeLogProblems records the log problems an executor reports, refusing the
// first one (an unreachable daemon).
type fakeLogProblems struct {
	Coordinator
	mu       sync.Mutex
	refuse   int
	recorded []string
}

func (f *fakeLogProblems) RecordAttemptLogProblem(_ context.Context, attemptID string, p store.LogProblem) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse > 0 {
		f.refuse--
		return errors.New("daemon unreachable")
	}
	f.recorded = append(f.recorded, attemptID+" "+p.Problem+": "+p.Detail)
	return nil
}

// While attempts run, a shortage of log space is recorded on each of them
// once per problem; a report the store did not take is made again.
func TestRunningAttemptsGetLogSpaceShortagesRecordedOnce(t *testing.T) {
	ctx := context.Background()
	fake := &fakeLogProblems{refuse: 1}
	free := atomic.Int64{}
	free.Store(100 << 20)
	e := logSpaceExecutor(fake, "/srv/kairo/attempts", func(string) (int64, error) { return free.Load(), nil })
	e.LogCheckInterval = 1
	e.running["att_1"] = &exec.Cmd{}
	check := func() {
		e.lastLogCheck = e.lastLogCheck.Add(-1)
		e.watchLogSpace(ctx)
	}
	check() // refused
	check()
	check()
	free.Store(0)
	check()
	check()
	free.Store(4 << 30) // plenty again: nothing new
	check()
	want := []string{
		"att_1 log_space_low: /srv/kairo/attempts has 100.0 MiB free, below the minimum of 1.0 GiB",
		"att_1 log_space_exhausted: /srv/kairo/attempts has 0 B free; writes to the logs fail",
	}
	if strings.Join(fake.recorded, "\n") != strings.Join(want, "\n") {
		t.Fatalf("recorded:\n%s", strings.Join(fake.recorded, "\n"))
	}
	// Nothing runs: nothing is checked.
	delete(e.running, "att_1")
	calls := 0
	e.FreeSpace = func(string) (int64, error) { calls++; return 0, nil }
	check()
	if calls != 0 {
		t.Fatal("log space checked with no attempt running")
	}
}
