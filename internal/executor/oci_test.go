package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// fakeRuntime is a runtime that runs nothing: its containers are a list.
type fakeRuntime struct {
	list    []ociContainer
	removed []string
	exit    int
	ran     bool
}

func (f *fakeRuntime) run(_ context.Context, spec containerSpec, _ func(string, ...any), made func(string)) (string, int, error) {
	f.ran = true
	made("ctr-" + spec.Instance)
	writeShimExit(filepath.Join(spec.RunDir, "exit"), f.exit) // as the container's init does
	code, _ := readExitFile(filepath.Join(spec.RunDir, "exit"))
	return "ctr-" + spec.Instance, code, nil
}
func (f *fakeRuntime) stop(context.Context, string) error { return nil }
func (f *fakeRuntime) remove(_ context.Context, h string) error {
	f.removed = append(f.removed, h)
	return nil
}
func (f *fakeRuntime) containers(context.Context) ([]ociContainer, error) { return f.list, nil }

// The sweep removes the containers of attempts that no longer run here: a
// stopped one always, a running one only when its argv is out of the host's
// sight (in a VM); never one whose attempt runs here.
func TestTheSweepRemovesContainersOfAttemptsNoLongerRunning(t *testing.T) {
	for _, c := range []struct {
		caps ociCaps
		want []string
	}{
		{ociCaps{argvOnHost: true}, []string{"stopped"}},
		{ociCaps{vm: true}, []string{"stopped", "running"}},
	} {
		f := &fakeRuntime{list: []ociContainer{
			{attempt: "att_a", handle: "stopped"},
			{attempt: "att_b", running: true, handle: "running"},
			{attempt: "att_here", handle: "here"},
		}}
		var logs bytes.Buffer
		e := NewLocal("ctr", nil, t.TempDir(), slog.New(slog.NewTextHandler(&logs, nil)))
		e.running = map[string]int{"att_here": 1}
		e.storeWork().sweepContainers(context.Background(), f, c.caps)
		if !slices.Equal(f.removed, c.want) {
			t.Fatalf("%+v: removed %v, want %v\n%s", c.caps, f.removed, c.want, logs.String())
		}
	}
}

// The runner has the spec's runtime run the container, exits with the exit
// its init wrote, removes the container and the spec (which holds the
// attempt's token).
func TestTheRunnerRunsAndRemovesTheContainer(t *testing.T) {
	f := &fakeRuntime{exit: 7}
	old := runtimeFor
	runtimeFor = func(kind string) (ociRuntime, bool) { return f, kind == "incus" }
	t.Cleanup(func() { runtimeFor = old })
	dir := t.TempDir()
	spec := containerSpec{Instance: "kairo-att-1", AttemptID: "att_1", Image: "x", RunDir: dir, Argv: []string{"true"}, Runtime: "incus"}
	body, _ := json.Marshal(spec)
	specPath := filepath.Join(dir, "spec.json")
	if err := os.WriteFile(specPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if exit := ociRun(specPath); exit != 7 || !f.ran || !slices.Equal(f.removed, []string{"ctr-kairo-att-1"}) {
		t.Fatalf("exit %d, ran %v, removed %v", exit, f.ran, f.removed)
	}
	if _, err := os.Stat(specPath); !os.IsNotExist(err) {
		t.Fatalf("the spec stayed: %v", err)
	}
	// a runtime that does not run here: never started
	spec.Runtime = "wslc"
	body, _ = json.Marshal(spec)
	os.WriteFile(specPath, body, 0o600)
	if exit := ociRun(specPath); exit != shimNotStarted {
		t.Fatalf("an unknown runtime: exit %d", exit)
	}
	// a spec from before runtimes were named: Incus, or pct with pct settings
	if specRuntime(containerSpec{}) != "incus" || specRuntime(containerSpec{PCT: &pctSettings{}}) != "pct" {
		t.Fatal("old specs' runtimes")
	}
}
