package extcmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// lockedBuffer is a log sink the test can read while the command logs to it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureDefaultLog routes slog.Default to a buffer until the test ends.
func captureDefaultLog(t *testing.T) *lockedBuffer {
	t.Helper()
	logs := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return logs
}

// TestExtcmdHelperProcess is the child that the tests below run as the command:
// this test binary, restarted with only this test selected.
func TestExtcmdHelperProcess(t *testing.T) {
	switch os.Getenv("KAIRO_EXTCMD_HELPER") {
	case "print":
		// os.Exit keeps the testing framework's "PASS" out of stdout.
		fmt.Print("42")
		os.Exit(0)
	case "fail":
		fmt.Fprint(os.Stderr, "no device")
		os.Exit(3)
	case "sleep":
		time.Sleep(time.Minute)
	}
}

func helperCommand(t *testing.T, mode string) (string, []string) {
	t.Helper()
	t.Setenv("KAIRO_EXTCMD_HELPER", mode)
	return os.Args[0], []string{"-test.run=^TestExtcmdHelperProcess$"}
}

func TestOutputReturnsStandardOutputOnly(t *testing.T) {
	name, args := helperCommand(t, "print")
	out, err := Output(context.Background(), name, args...)
	if err != nil || string(out) != "42" {
		t.Fatalf("output %q, error %v", out, err)
	}
}

func TestOutputErrorCarriesTheCommandsStderr(t *testing.T) {
	name, args := helperCommand(t, "fail")
	_, err := Output(context.Background(), name, args...)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || !strings.Contains(err.Error(), "no device") {
		t.Fatalf("error does not carry the command's failure: %v", err)
	}
}

func TestOutputKillsACommandAtItsDeadlineAndLogsIt(t *testing.T) {
	logs := captureDefaultLog(t)
	name, args := helperCommand(t, "sleep")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Output(ctx, name, args...)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the deadline error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("command was not killed at its deadline (took %s)", elapsed)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "external command timed out") || !strings.Contains(logs.String(), "-test.run=^TestExtcmdHelperProcess$") {
		t.Fatalf("timeout not logged at WARN with the command: %s", logs.String())
	}
}
