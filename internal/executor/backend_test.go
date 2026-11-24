package executor

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"testing"

	"kairo/internal/store"
)

// An executor's backend comes from its kind: Incus runs containers and
// sweeps their instances; every other kind runs on the host.
func TestTheBackendComesFromTheKind(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, c := range []struct {
		kind   string
		labels map[string]string
		want   string
	}{
		{"linux", nil, "executor.hostBackend"},
		{"windows", map[string]string{"environment": "windows"}, "executor.hostBackend"},
		{"incus", nil, "executor.incusBackend"},
	} {
		e := NewLocal("x", nil, t.TempDir(), logger)
		e.Kind, e.Attributes = c.kind, c.labels
		b := e.backend()
		if got := fmt.Sprintf("%T", b); got != c.want {
			t.Errorf("%s %v: backend %s, want %s", c.kind, c.labels, got, c.want)
		}
		_, sweeps := b.(instanceSweeper)
		if sweeps != (c.kind == "incus") {
			t.Errorf("%s: sweeps instances %v", c.kind, sweeps)
		}
	}
}

// A launch in an image is refused by a backend that runs no containers, a
// launch without one by the container backend.
func TestBackendsRefuseWhatTheyCannotRun(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	executor := func(kind string) *Local {
		e := NewLocal(kind+"-x", nil, t.TempDir(), logger)
		e.Kind = kind
		return e
	}
	launch := func() *store.Launch {
		return &store.Launch{Attempt: store.Attempt{ID: "att_1"}, Argv: []string{"python"}, CWD: "/work"}
	}
	image := launch()
	image.Image = "docker.io/library/python@sha256:" + strings.Repeat("a", 64)
	if _, _, err := executor("linux").backend().launcher(image); err == nil || !strings.Contains(err.Error(), "does not run containers") {
		t.Errorf("the host ran an image: %v", err)
	}
	if _, _, err := executor("incus").backend().launcher(launch()); err == nil || !strings.Contains(err.Error(), "names no image") {
		t.Errorf("incus ran no image: %v", err)
	}
	cmd, conf, err := executor("linux").backend().launcher(launch())
	if err != nil && !errors.Is(err, exec.ErrNotFound) || cmd == nil || conf != nil || cmd.Dir != "/work" || cmd.Args[0] != "python" {
		t.Errorf("host launcher %v %v %v", cmd, conf, err)
	}
}
