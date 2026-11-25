package executor

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"kairo/internal/store"
)

// An executor's backend comes from its kind: every kind runs containers
// and sweeps them; one that is no container kind runs nothing.
func TestTheBackendComesFromTheKind(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, c := range []struct {
		kind   string
		labels map[string]string
		want   string
	}{
		{"linux", nil, "executor.ociBackend"},
		{"incus", nil, "executor.ociBackend"},
		{"pct", nil, "executor.ociBackend"},
		{"wslc", nil, "executor.ociBackend"},
	} {
		e := NewLocal("x", nil, t.TempDir(), logger)
		e.Kind, e.Attributes = c.kind, c.labels
		b := e.backend()
		if got := fmt.Sprintf("%T", b); got != c.want {
			t.Errorf("%s %v: backend %s, want %s", c.kind, c.labels, got, c.want)
		}
		if _, sweeps := b.(instanceSweeper); !sweeps {
			t.Errorf("%s: sweeps no instances", c.kind)
		}
	}
}

// An executor of a kind that runs no containers (here) runs nothing.
func TestAKindThatRunsNoContainersRunsNothing(t *testing.T) {
	e := NewLocal("linux-x", nil, t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	e.Kind = "linux"
	launch := &store.Launch{Attempt: store.Attempt{ID: "att_1"}, Image: "docker.io/library/python@sha256:" + strings.Repeat("a", 64), Argv: []string{"python"}, CWD: "/work"}
	if _, err := e.backend().launcher(launch); err == nil || !strings.Contains(err.Error(), "runs no containers") {
		t.Errorf("kind linux ran a launch: %v", err)
	}
}
