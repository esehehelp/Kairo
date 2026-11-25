package executor

import (
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

func testLaunch() *store.Launch {
	ref := "checkpoint://one"
	return &store.Launch{
		Execution:            store.ExecutionRequest{ID: "exe_1"},
		Attempt:              store.Attempt{ID: "att_1"},
		Lease:                store.Lease{ID: "lease_1", CoordinationEpoch: 3},
		WorkerToken:          "kairo_worker_secret",
		Argv:                 []string{"python", "train.py", "--steps", "10"},
		CWD:                  "/work",
		Resources:            []store.ResourceInstance{{ID: "gpu-0", Binding: json.RawMessage(`{"cuda_index":"0"}`)}, {ID: "gpu-1", Binding: json.RawMessage(`{"cuda_index":"1"}`)}},
		InputContinuationRef: &ref,
	}
}

// launcherOf is the launcher the test backend builds for launch: the
// attempt's environment as Kairo gives it (attemptEnv) over this process's.
func launcherOf(t *testing.T, e *Local, launch *store.Launch) *exec.Cmd {
	t.Helper()
	cmd, err := procBackend{e}.launcher(launch)
	if err != nil && (cmd == nil || !errors.Is(err, exec.ErrNotFound)) {
		t.Fatal(err)
	}
	return cmd // as built, also when argv[0] is not on this machine
}

func envValue(env []string, name string) (string, bool) {
	for _, kv := range env {
		if k, v, _ := strings.Cut(kv, "="); k == name {
			return v, true
		}
	}
	return "", false
}

func TestNativeLaunchEnvironment(t *testing.T) {
	t.Setenv("KAIRO_TOKEN", "kairo_admin_operator")
	t.Setenv("KAIRO_ATTEMPT_TOKEN", "kairo_worker_stale")
	e := &Local{ID: "win-local", NodeID: "host", Kind: "windows", APIURL: "https://127.0.0.1:7474", Attributes: map[string]string{}}
	cmd := launcherOf(t, e, testLaunch())
	if !slices.Equal(cmd.Args, []string{"python", "train.py", "--steps", "10"}) || cmd.Dir != "/work" {
		t.Fatalf("command %q in %q", cmd.Args, cmd.Dir)
	}
	if got, _ := envValue(cmd.Env, "KAIRO_ATTEMPT_TOKEN"); got != "kairo_worker_secret" {
		t.Fatalf("attempt token = %q", got)
	}
	if _, ok := envValue(cmd.Env, "KAIRO_TOKEN"); ok {
		t.Fatal("the operator token reached the attempt")
	}
	if _, ok := envValue(cmd.Env, "KAIRO_API_CA"); ok {
		t.Fatal("KAIRO_API_CA set although the daemon serves plain HTTP")
	}
	count := 0
	for _, kv := range cmd.Env {
		if strings.HasPrefix(kv, "KAIRO_ATTEMPT_TOKEN=") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("KAIRO_ATTEMPT_TOKEN set %d times", count)
	}
}

// A leased GPU is named by UUID, not by nvidia-smi's index, so CUDA's own
// device order cannot hand the attempt another card.
func TestGPUBindingIsTheUUID(t *testing.T) {
	launch := testLaunch()
	launch.Resources = []store.ResourceInstance{
		{ID: "gpu-b", Binding: json.RawMessage(`{"cuda_index":"1","uuid":"GPU-bbbb"}`)},
		{ID: "gpu-a", Binding: json.RawMessage(`{"cuda_index":"0","uuid":"GPU-aaaa"}`)},
	}
	e := &Local{ID: "host-a", NodeID: "node", Kind: "linux", APIURL: "https://127.0.0.1:7474", Attributes: map[string]string{}}
	env := e.launchEnv(launch)
	for name, want := range map[string]string{"CUDA_VISIBLE_DEVICES": "GPU-bbbb,GPU-aaaa", "KAIRO_RESOURCE_BINDINGS": "GPU-bbbb,GPU-aaaa", "KAIRO_RESOURCE_IDS": "gpu-b,gpu-a"} {
		if got, ok := envValue(env, name); !ok || got != want {
			t.Fatalf("%s = %q (%v)", name, got, ok)
		}
	}
}

// Declared env replaces the parent's variables (case-insensitively on
// Windows, so a declared "path" replaces PATH) but never what Kairo sets.
func TestDeclaredEnvOverridesTheParentButNotKairo(t *testing.T) {
	t.Setenv("PYTHONUTF8", "0")
	launch := testLaunch()
	launch.Env = map[string]string{"PYTHONUTF8": "1", "path": `C:\only`, "RANK": "7", "CUDA_VISIBLE_DEVICES": "9", "Zeta": "z", "Alpha": "a"}
	e := &Local{ID: "win-local", NodeID: "host", Kind: "windows", APIURL: "https://127.0.0.1:7474", Attributes: map[string]string{}}
	cmd := launcherOf(t, e, launch)
	values := func(name string) []string {
		var out []string
		for _, kv := range cmd.Env {
			k, v, _ := strings.Cut(kv, "=")
			if k == name || (runtime.GOOS == "windows" && strings.EqualFold(k, name)) {
				out = append(out, v)
			}
		}
		return out
	}
	for name, want := range map[string]string{"PYTHONUTF8": "1", "RANK": "7", "CUDA_VISIBLE_DEVICES": "0,1", "Alpha": "a"} {
		if got := values(name); len(got) != 1 || got[0] != want {
			t.Fatalf("%s = %q", name, got)
		}
	}
	if runtime.GOOS == "windows" {
		if got := values("PATH"); len(got) != 1 || got[0] != `C:\only` {
			t.Fatalf("PATH = %q", got)
		}
	}
	if alpha, zeta := slices.Index(cmd.Env, "Alpha=a"), slices.Index(cmd.Env, "Zeta=z"); alpha < 0 || zeta < alpha {
		t.Fatalf("declared env out of order: %q", cmd.Env)
	}
}

// An input is handed as KAIRO_INPUT_<NAME> (the name in upper case); a
// launch one of whose inputs cannot be had here, or is not a directory here
// now, is refused saying which and why.
func TestInputsAreHandedByPathOrRefuseTheLaunch(t *testing.T) {
	dir := t.TempDir()
	e := &Local{ID: "exec", NodeID: "node"}
	launch := testLaunch()
	launch.Inputs = []store.LaunchInput{{Name: "init", Path: dir}, {Name: "Shards_2", Path: dir}}
	env := e.launchEnv(launch)
	for _, name := range []string{"KAIRO_INPUT_INIT", "KAIRO_INPUT_SHARDS_2"} {
		if v, ok := envValue(env, name); !ok || v != dir {
			t.Fatalf("%s = %q, %v", name, v, ok)
		}
	}
	if err := e.checkInputs(launch); err != nil {
		t.Fatal(err)
	}
	launch.Inputs = append(launch.Inputs, store.LaunchInput{Name: "gone", Path: dir + "/gone"})
	if err := e.checkInputs(launch); err == nil || !strings.Contains(err.Error(), "input gone") {
		t.Fatalf("a missing input: %v", err)
	}
	launch.Inputs = []store.LaunchInput{{Name: "far", Problem: "it is on node host-a-gpu, not on node"}}
	if err := e.checkInputs(launch); err == nil || !strings.Contains(err.Error(), "input far: it is on node host-a-gpu") {
		t.Fatalf("an input elsewhere: %v", err)
	}
	if _, ok := envValue(e.launchEnv(launch), "KAIRO_INPUT_FAR"); ok {
		t.Fatal("an input without a path is handed")
	}
}

// A launch naming a store gets its scratch on it; one naming a store this
// executor does not serve, usable and volatile, is refused.
func TestALaunchNamingAStoreUsesItOrIsRefused(t *testing.T) {
	base := t.TempDir()
	first, named := filepath.Join(base, "first"), filepath.Join(base, "named")
	e := &Local{ID: "exec", NodeID: "node"}
	e.storeWork().stores = []servedStore{
		{decl: machine.Store{ID: "a-first", Class: machine.StoreVolatile, Root: first}, fs: localStoreFS{}, usable: true},
		{decl: machine.Store{ID: "b-named", Class: machine.StoreVolatile, Root: named}, fs: localStoreFS{}, usable: true},
		{decl: machine.Store{ID: "c-durable", Class: machine.StoreDurable, Root: base}, fs: localStoreFS{}, usable: true},
	}
	launch := testLaunch()
	if s, ok := e.storeWork().storeFor(launch); !ok || s.decl.ID != "a-first" {
		t.Fatalf("default store %+v %v", s.decl, ok)
	}
	launch.Store = "b-named"
	if s, ok := e.storeWork().storeFor(launch); !ok || s.decl.ID != "b-named" || e.storeWork().checkStore(launch) != nil {
		t.Fatalf("named store %+v %v", s.decl, ok)
	}
	for _, id := range []string{"c-durable", "nowhere"} {
		launch.Store = id
		if err := e.storeWork().checkStore(launch); err == nil || !strings.Contains(err.Error(), id) {
			t.Fatalf("%s: %v", id, err)
		}
	}
}
