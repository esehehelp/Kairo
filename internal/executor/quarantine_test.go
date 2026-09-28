//go:build !windows

package executor

import (
	"context"
	"log/slog"
	"os/exec"
	"testing"
	"time"

	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// quarantineCoordinator serves one termination and records what the executor
// reports about it.
type quarantineCoordinator struct {
	Coordinator
	termination store.QuarantineTermination
	withdrawn   bool // released after the listing
	signalled   int
	terminated  int
}

func (c *quarantineCoordinator) ListQuarantineTerminations(context.Context, string) ([]store.QuarantineTermination, error) {
	if c.terminated != 0 {
		return nil, nil
	}
	return []store.QuarantineTermination{c.termination}, nil
}

func (c *quarantineCoordinator) MarkQuarantineSignalled(context.Context, string) (string, error) {
	if c.withdrawn {
		return "", store.ErrNotFound
	}
	c.signalled++
	if c.termination.SignalledAt == nil {
		at := time.Now().UTC().Format(time.RFC3339Nano)
		c.termination.SignalledAt = &at
	}
	return *c.termination.SignalledAt, nil
}

func (c *quarantineCoordinator) MarkQuarantineTerminated(context.Context, string) error {
	c.terminated++
	return nil
}

// startTree starts a launcher whose descendant ignores SIGTERM.
func startTree(t *testing.T) (*exec.Cmd, store.QuarantineTermination, chan struct{}) {
	t.Helper()
	cmd := exec.Command("sh", "-c", `trap "" TERM; sleep 60`)
	prepareProcessTree(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() { _ = signalProcessTree(cmd.Process.Pid, true) })
	identity, err := processidentity.ForPID(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	time.Sleep(300 * time.Millisecond) // let the shell install its trap
	return cmd, store.QuarantineTermination{AttemptID: "att", ExecutionID: "exe", PID: &pid, ProcessIdentity: &identity}, done
}

func quarantineExecutor(c Coordinator) *Local {
	return NewLocal("executor", c, "", slog.New(slog.DiscardHandler))
}

func TestQuarantineTerminationEscalatesAndSettlesOnlyWhenTreeIsGone(t *testing.T) {
	_, termination, done := startTree(t)
	c := &quarantineCoordinator{termination: termination}
	e := quarantineExecutor(c)
	ctx := context.Background()
	if err := e.carryOutQuarantineTerminations(ctx); err != nil {
		t.Fatal(err)
	}
	if c.signalled != 1 || c.terminated != 0 {
		t.Fatalf("first tick: signalled=%d terminated=%d", c.signalled, c.terminated)
	}
	time.Sleep(300 * time.Millisecond)
	// SIGTERM is ignored and the grace period has not passed: still alive, not settled.
	if err := e.carryOutQuarantineTerminations(ctx); err != nil {
		t.Fatal(err)
	}
	if c.terminated != 0 {
		t.Fatal("termination settled while the tree is alive")
	}
	select {
	case <-done:
		t.Fatal("SIGTERM-ignoring launcher exited")
	default:
	}
	// grace period over, by the executor's clock: forced kill
	e.quarantineSignalled["att"] = time.Now().Add(-killGrace)
	if err := e.carryOutQuarantineTerminations(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("tree survived the forced kill")
	}
	deadline := time.Now().Add(15 * time.Second)
	for c.terminated == 0 {
		if time.Now().After(deadline) {
			t.Fatal("termination never settled")
		}
		if err := e.carryOutQuarantineTerminations(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if c.signalled != 1 {
		t.Fatalf("signalled recorded %d times", c.signalled)
	}
}

func TestWithdrawnQuarantineTerminationSendsNoSignal(t *testing.T) {
	_, termination, done := startTree(t)
	c := &quarantineCoordinator{termination: termination, withdrawn: true}
	if err := quarantineExecutor(c).carryOutQuarantineTerminations(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("withdrawn termination killed the attempt")
	default:
	}
	if c.terminated != 0 {
		t.Fatal("withdrawn termination settled")
	}
}

func TestQuarantineTerminationOfAnExitedTreeSendsNoSignal(t *testing.T) {
	cmd, termination, done := startTree(t)
	_ = signalProcessTree(cmd.Process.Pid, true)
	<-done
	c := &quarantineCoordinator{termination: termination}
	if err := quarantineExecutor(c).carryOutQuarantineTerminations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.signalled != 0 || c.terminated != 1 {
		t.Fatalf("signalled=%d terminated=%d", c.signalled, c.terminated)
	}
}

// Without proof of the tree's state nothing is marked: the termination is retried.
func TestQuarantineTerminationWithUnknownLivenessIsRetried(t *testing.T) {
	c := &quarantineCoordinator{termination: store.QuarantineTermination{AttemptID: "att", ExecutionID: "exe"}}
	e := quarantineExecutor(c)
	for range 2 {
		if err := e.carryOutQuarantineTerminations(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if c.signalled != 0 || c.terminated != 0 {
		t.Fatalf("signalled=%d terminated=%d", c.signalled, c.terminated)
	}
}

// An executor restarted after the first signal times the grace period from
// when it first sees the termination, and signals again meanwhile.
func TestQuarantineTerminationSignalledBeforeRestartGetsAFreshGracePeriod(t *testing.T) {
	_, termination, done := startTree(t)
	long := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano) // daemon clock, possibly skewed
	termination.SignalledAt = &long
	c := &quarantineCoordinator{termination: termination}
	e := quarantineExecutor(c)
	if err := e.carryOutQuarantineTerminations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if first, ok := e.quarantineSignalled["att"]; !ok || time.Since(first) > time.Minute {
		t.Fatalf("grace period not started by the executor's clock: %v %v", first, ok)
	}
	time.Sleep(200 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("forced kill without a grace period")
	default:
	}
	if c.signalled != 0 {
		t.Fatalf("signalled re-recorded %d times", c.signalled)
	}
}
