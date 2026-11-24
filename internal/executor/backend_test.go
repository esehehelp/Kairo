package executor

import (
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"kairo/internal/store"
)

// An executor's backend comes from its kind (or a WSL label), and says how
// it reaches the node: a WSL executor reads none of the node's paths,
// serves only the stores that name it, sets TMPDIR alone and keeps its
// processes in its distro; the host and Incus ones do the opposite.
func TestTheBackendComesFromTheKind(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, c := range []struct {
		kind   string
		labels map[string]string
		want   string
		node   bool
		temps  []string
	}{
		{"linux", nil, "executor.hostBackend", true, []string{"TMPDIR", "TMP", "TEMP"}},
		{"windows", map[string]string{"environment": "windows"}, "executor.hostBackend", true, []string{"TMPDIR", "TMP", "TEMP"}},
		{"wsl2", map[string]string{"distro": "Ubuntu"}, "executor.wslBackend", false, []string{"TMPDIR"}},
		{"local", map[string]string{"environment": "wsl2"}, "executor.wslBackend", false, []string{"TMPDIR"}},
		{"incus", nil, "executor.incusBackend", true, []string{"TMPDIR", "TMP", "TEMP"}},
	} {
		e := NewLocal("x", nil, t.TempDir(), logger)
		e.Kind, e.Attributes = c.kind, c.labels
		b := e.backend()
		if got := fmt.Sprintf("%T", b); got != c.want {
			t.Errorf("%s %v: backend %s, want %s", c.kind, c.labels, got, c.want)
		}
		if b.readsNodePaths() != c.node || b.servesNodeStores() != c.node || b.tree("att_1").wsl == c.node {
			t.Errorf("%s: reads node paths %v, serves node stores %v, tree %+v", c.kind, b.readsNodePaths(), b.servesNodeStores(), b.tree("att_1"))
		}
		if !slices.Equal(b.tempNames(), c.temps) {
			t.Errorf("%s: temp names %v", c.kind, b.tempNames())
		}
		_, sweeps := b.(instanceSweeper)
		if sweeps != (c.kind == "incus") {
			t.Errorf("%s: sweeps instances %v", c.kind, sweeps)
		}
	}
}

// A launch in an image is refused by a backend that runs no containers, a
// launch without one by the container backend; inside WSL a code snapshot
// and confinement are refused.
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
	for _, kind := range []string{"linux", "wsl2"} {
		if _, _, err := executor(kind).backend().launcher(image); err == nil || !strings.Contains(err.Error(), "does not run containers") {
			t.Errorf("%s ran an image: %v", kind, err)
		}
	}
	if _, _, err := executor("incus").backend().launcher(launch()); err == nil || !strings.Contains(err.Error(), "names no image") {
		t.Errorf("incus ran no image: %v", err)
	}
	src := launch()
	src.Source = &store.LaunchSource{ID: "src_" + strings.Repeat("0", 32)}
	if _, _, err := executor("wsl2").backend().launcher(src); err == nil || !strings.Contains(err.Error(), "inside WSL") {
		t.Errorf("a code snapshot inside WSL: %v", err)
	}
	confined := launch()
	confined.Confine = true
	if _, _, err := executor("wsl2").backend().launcher(confined); err == nil || !strings.Contains(err.Error(), "inside WSL") {
		t.Errorf("confined inside WSL: %v", err)
	}
	cmd, conf, err := executor("linux").backend().launcher(launch())
	if err != nil || conf != nil || cmd.Dir != "/work" || !strings.HasSuffix(cmd.Args[0], "python") {
		t.Errorf("host launcher %v %v %v", cmd, conf, err)
	}
}
