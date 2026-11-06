package executor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// fakeOutputs serves stores and scratch (fakeScratch) and records output
// directories.
type fakeOutputs struct {
	*fakeScratch
	outputErr error
	outputs   []store.RunOutput
}

func (f *fakeOutputs) RecordRunOutput(_ context.Context, out store.RunOutput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.outputErr != nil {
		return f.outputErr
	}
	f.outputs = append(f.outputs, out)
	return nil
}

// An attempt gets its run's output directory on the volatile store its
// scratch goes on, recorded and named by KAIRO_OUTPUT_DIR; every attempt of
// the run finds the same directory with what was written there before, and
// reports measure it.
func TestLaunchGetsItsRunsOutputDirectory(t *testing.T) {
	root := t.TempDir()
	fake := &fakeOutputs{fakeScratch: &fakeScratch{fakeStores: &fakeStores{}}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	volatile := filepath.Join(root, "volatile")
	e.stores = []servedStore{
		{decl: machine.Store{ID: "a-durable", Class: machine.StoreDurable, Root: filepath.Join(root, "durable")}, fs: localStoreFS{}, usable: true},
		{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true},
	}
	launch := testLaunch()
	launch.RunID = "exe_1"
	e.makeOutput(launch)
	want := filepath.Join(volatile, outputDirName, "exe_1")
	if marker, err := os.ReadFile(filepath.Join(want, outputMarkerName)); err != nil || string(marker) != "exe_1\n" {
		t.Fatalf("marker: %q %v", marker, err)
	}
	if len(fake.outputs) != 1 || fake.outputs[0] != (store.RunOutput{AttemptID: "att_1", LeaseID: "lease_1", Epoch: 3, RunID: "exe_1", StoreID: "c-scratch", ExecutorID: "exec", Root: volatile, Path: want}) {
		t.Fatalf("recorded: %+v", fake.outputs)
	}
	if got, _ := envValue(e.launchEnv(launch), "KAIRO_OUTPUT_DIR"); got != want {
		t.Fatalf("KAIRO_OUTPUT_DIR = %q", got)
	}
	if _, ok := envValue(e.launchEnv(launch), "KAIRO_OUTPUT_DIR"); ok {
		t.Fatal("the output directory was named again after the environment was built")
	}
	if err := os.WriteFile(filepath.Join(want, "step-5.pt"), []byte("checkpoint"), 0o644); err != nil {
		t.Fatal(err)
	}

	// the run's next attempt (a continuation) finds it as it was
	next := testLaunch()
	next.Attempt.ID, next.RunID = "att_2", "exe_1"
	e.makeOutput(next)
	if got, _ := envValue(e.launchEnv(next), "KAIRO_OUTPUT_DIR"); got != want {
		t.Fatalf("the continuation's KAIRO_OUTPUT_DIR = %q", got)
	}
	if body, err := os.ReadFile(filepath.Join(want, "step-5.pt")); err != nil || string(body) != "checkpoint" {
		t.Fatalf("what the run wrote before: %q %v", body, err)
	}
	if len(fake.outputs) != 2 || fake.outputs[1].AttemptID != "att_2" || fake.outputs[1].Path != want {
		t.Fatalf("recorded: %+v", fake.outputs)
	}
	// another run gets another directory; scratch is apart
	other := testLaunch()
	other.Attempt.ID, other.RunID = "att_3", "exe_9"
	e.makeOutput(other)
	e.makeScratch(other)
	ctx := context.Background()
	runs, _, err := localStoreFS{}.ListKind(ctx, volatile, kindOutput)
	if err != nil || strings.Join(runs, ",") != "exe_1,exe_9" {
		t.Fatalf("output directories: %v %v", runs, err)
	}
	if scratch, _, err := (localStoreFS{}).List(ctx, volatile); err != nil || strings.Join(scratch, ",") != "att_3" {
		t.Fatalf("scratch: %v %v", scratch, err)
	}
	// an output directory is never removed as a scratch, nor as another run's
	if err := (localStoreFS{}).Remove(ctx, volatile, want, "exe_1", false); !errors.Is(err, errOutsideRoot) {
		t.Fatalf("removed as a scratch: %v", err)
	}
	if err := (localStoreFS{}).RemoveKind(ctx, volatile, kindOutput, want, "exe_9", false); !errors.Is(err, errOutsideRoot) {
		t.Fatalf("removed as another run's: %v", err)
	}
	if !exists(filepath.Join(want, "step-5.pt")) {
		t.Fatal("the output directory was touched")
	}

	// reports measure them
	obs := e.observeStore(ctx, e.stores[1])
	if !obs.OutputsScanned || obs.Outputs["exe_1"] != int64(len("checkpoint")+len("exe_1\n")) || len(obs.Outputs) != 2 {
		t.Fatalf("observed outputs: %+v scanned %v", obs.Outputs, obs.OutputsScanned)
	}

	// without a run (an older daemon) or a volatile store there is none; one
	// the daemon did not record is not named
	none := testLaunch()
	none.Attempt.ID = "att_4"
	e.makeOutput(none)
	if _, ok := envValue(e.launchEnv(none), "KAIRO_OUTPUT_DIR"); ok || len(fake.outputs) != 3 {
		t.Fatalf("an output directory without a run: %+v", fake.outputs)
	}
	fake.outputErr = errors.New("refused")
	refused := testLaunch()
	refused.Attempt.ID, refused.RunID = "att_5", "exe_5"
	e.makeOutput(refused)
	if _, ok := envValue(e.launchEnv(refused), "KAIRO_OUTPUT_DIR"); ok || !strings.Contains(logs.String(), "attempt starts without an output directory: not recorded") {
		t.Fatalf("an unrecorded output directory was named:\n%s", logs.String())
	}
	fake.outputErr = nil
	e.stores = e.stores[:1]
	durableOnly := testLaunch()
	durableOnly.Attempt.ID, durableOnly.RunID = "att_6", "exe_6"
	e.makeOutput(durableOnly)
	if exists(filepath.Join(root, "durable", outputDirName)) || len(fake.outputs) != 3 {
		t.Fatalf("an output directory on a durable store: %+v", fake.outputs)
	}
}
