package executor

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"kairo/internal/processidentity"
)

// fakeExits records the exits an executor records.
type fakeExits struct {
	Coordinator
	mu    sync.Mutex
	exits map[string]int
}

func (f *fakeExits) RecordTerminal(_ context.Context, attemptID, _ string, _ int64, exitCode int, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.exits == nil {
		f.exits = map[string]int{}
	}
	f.exits[attemptID] = exitCode
	return nil
}

func (f *fakeExits) exit(attemptID string) (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	code, ok := f.exits[attemptID]
	return code, ok
}

// An agent restarted while an attempt runs adopts it: the new instance finds
// the attempt's record, and once the command ends records the exit its shim
// wrote. A record whose shim is gone without an exit is not adopted.
func TestARestartedAgentRecordsTheExitOfAnAttemptItAdopted(t *testing.T) {
	dir := t.TempDir()
	flag := filepath.Join(dir, "go")
	cmd := exec.Command(os.Args[0], helperExitWhen, flag)
	cmd.Env = append(os.Environ(), "KAIRO_ATTEMPT_ID=att")
	prepareProcessTree(cmd)
	hideTestConsole(cmd)
	job := ""
	if runtime.GOOS == "windows" {
		job = "kairo-test-adopt-" + strconv.Itoa(os.Getpid())
	}
	if err := os.MkdirAll(recordsDir(dir), 0o755); err != nil {
		t.Fatal(err)
	}
	// launched as an agent launches an attempt
	shim, goAhead, err := shimCommand(cmd, exitPath(dir, "att"), nil)
	if err != nil {
		t.Fatal(err)
	}
	readEnd := shim.Stdin.(*os.File)
	if err = shim.Start(); err != nil {
		t.Fatal(err)
	}
	readEnd.Close()
	t.Cleanup(func() { _ = shim.Process.Kill(); _ = shim.Wait() })
	tree, err := attachTree(shim, job)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := processidentity.ForPID(shim.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeRecord(dir, AttemptRecord{AttemptID: "att", ExecutorID: "e", LeaseID: "lease", Epoch: 3, PID: shim.Process.Pid, Identity: identity, Job: job}); err != nil {
		t.Fatal(err)
	}
	if err = sendGoAhead(goAhead); err != nil {
		t.Fatal(err)
	}
	tree.close() // the launching agent goes away, its handles with it
	if err = writeRecord(dir, AttemptRecord{AttemptID: "gone", ExecutorID: "e", PID: 0x7ffffff0, Identity: "gone"}); err != nil {
		t.Fatal(err)
	}

	records := AdoptableAttempts(dir, map[string]bool{"e": true}, slog.Default())
	if len(records) != 1 || records[0].AttemptID != "att" {
		t.Fatalf("adoptable: %+v", records)
	}
	if _, err = os.Stat(recordPath(dir, "gone")); !os.IsNotExist(err) {
		t.Fatalf("the record of a shim gone without its exit was kept: %v", err)
	}
	fake := &fakeExits{}
	e := NewLocal("e", fake, dir, slog.Default())
	e.Adopt(records, []string{"att"})
	e.mu.Lock()
	pid, h := e.running["att"], e.trees["att"]
	e.mu.Unlock()
	if pid != shim.Process.Pid {
		t.Fatalf("adopted pid %d, want %d", pid, shim.Process.Pid)
	}
	if !adoptedTreeOpened(h) {
		t.Fatalf("the adopted attempt's job object was not opened again: %+v", h)
	}
	if err = os.WriteFile(flag, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if code, ok := fake.exit("att"); ok {
			if code != 7 {
				t.Fatalf("recorded exit %d, want 7", code)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the adopted attempt's exit was not recorded")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err = os.Stat(recordPath(dir, "att")); !os.IsNotExist(err) {
		t.Fatalf("record kept after the exit: %v", err)
	}
	e.mu.Lock()
	_, still := e.running["att"]
	e.mu.Unlock()
	if still {
		t.Fatal("still counted as running")
	}
}

// A shim given no go-ahead (its agent went away first) runs nothing and
// writes no exit.
func TestAShimWithoutTheGoAheadRunsNothing(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "ran")
	cmd := exec.Command(os.Args[0], helperExitWhen, marker)
	prepareProcessTree(cmd)
	hideTestConsole(cmd)
	shim, goAhead, err := shimCommand(cmd, filepath.Join(dir, "exit"), nil)
	if err != nil {
		t.Fatal(err)
	}
	readEnd := shim.Stdin.(*os.File)
	if err = shim.Start(); err != nil {
		t.Fatal(err)
	}
	readEnd.Close()
	goAhead.Close()
	err = shim.Wait()
	if code := shim.ProcessState.ExitCode(); code != shimNoGoAhead {
		t.Fatalf("exit %d (%v), want %d", code, err, shimNoGoAhead)
	}
	if _, err = os.Stat(filepath.Join(dir, "exit")); !os.IsNotExist(err) {
		t.Fatalf("an exit was written: %v", err)
	}
}
