// Package extcmd runs the programs an executor depends on (nvidia-smi, wsl.exe,
// taskkill) under a deadline the caller sets, so that a hung one cannot hold a
// tick, and logs the ones that are slow or time out.
package extcmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"
)

// slowCommand is how long a command may run before its duration is logged.
const slowCommand = 2 * time.Second

// waitDelay bounds how long Output waits for the pipes of a command that was
// killed: a grandchild that inherited them must not hold the caller forever.
const waitDelay = 5 * time.Second

// Output runs name with args and returns its standard output. When ctx ends
// before the command does, the command is killed (with its process tree on
// Windows) and the error wraps the context error. A slow command is logged at
// INFO with its duration, a timed-out one at WARN with its command line.
func Output(ctx context.Context, name string, args ...string) ([]byte, error) {
	line := strings.Join(append([]string{name}, args...), " ")
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stderr = &stderr
	cmd.Cancel = func() error { return killTree(cmd) }
	cmd.WaitDelay = waitDelay
	start := time.Now()
	out, err := cmd.Output()
	elapsed := time.Since(start)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		slog.Default().Warn("external command timed out", "command", line, "duration_seconds", elapsed.Seconds())
		return nil, fmt.Errorf("%s did not finish: %w", line, ctx.Err())
	}
	if elapsed > slowCommand {
		slog.Default().Info("external command was slow", "command", line, "duration_seconds", elapsed.Seconds())
	}
	if err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("%s: %w: %s", name, err, msg)
		}
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return out, nil
}
