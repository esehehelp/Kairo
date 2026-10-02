package executor

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

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
		Gang:                 &store.GangLaunch{GangID: "gang_1", Rank: 1, Size: 2, MasterAddr: "10.0.0.1", MasterPort: 29500},
	}
}

func envValue(env []string, name string) (string, bool) {
	for _, kv := range env {
		if k, v, _ := strings.Cut(kv, "="); k == name {
			return v, true
		}
	}
	return "", false
}

// A WSL attempt receives its variables through WSLENV, so the attempt token
// never appears on the wsl.exe command line; the operator's own KAIRO_*
// variables do not reach it.
func TestWSLLaunchPassesTheEnvironmentThroughWSLENV(t *testing.T) {
	t.Setenv("KAIRO_TOKEN", "kairo_admin_operator")
	t.Setenv("WSLENV", "USERPROFILE/p")
	e := &Local{ID: "ubuntu-wsl", NodeID: "host", Kind: "wsl2", APIURL: "https://192.168.1.12:7474", APICA: "Q0E=", Attributes: map[string]string{"distro": "Ubuntu", "interconnect_ifname": "eth1"}}
	cmd := e.command(testLaunch())
	if want := []string{"-d", "Ubuntu", "--cd", "/work", "--", "python", "train.py", "--steps", "10"}; !slices.Equal(cmd.Args[1:], want) {
		t.Fatalf("args = %q", cmd.Args)
	}
	for _, arg := range cmd.Args {
		if strings.Contains(arg, "KAIRO_") || strings.Contains(arg, "kairo_worker_") {
			t.Fatalf("launch variable on the command line: %q", cmd.Args)
		}
	}
	for name, want := range map[string]string{
		"KAIRO_ATTEMPT_TOKEN": "kairo_worker_secret", "KAIRO_ATTEMPT_ID": "att_1", "KAIRO_EXECUTION_ID": "exe_1",
		"KAIRO_API_URL": "https://192.168.1.12:7474", "KAIRO_API_CA": "Q0E=", "KAIRO_CONTINUATION_REF": "checkpoint://one",
		"CUDA_VISIBLE_DEVICES": "0,1", "KAIRO_RESOURCE_IDS": "gpu-0,gpu-1", "RANK": "1", "WORLD_SIZE": "2",
		"MASTER_ADDR": "10.0.0.1", "NCCL_SOCKET_IFNAME": "eth1",
	} {
		if got, ok := envValue(cmd.Env, name); !ok || got != want {
			t.Fatalf("%s = %q (%v)", name, got, ok)
		}
	}
	for _, gone := range []string{"KAIRO_TOKEN", "KAIRO_LEASE_ID", "KAIRO_COORDINATION_EPOCH"} {
		if _, ok := envValue(cmd.Env, gone); ok {
			t.Fatalf("%s reached the attempt", gone)
		}
	}
	wslenv, _ := envValue(cmd.Env, "WSLENV")
	shared := strings.Split(wslenv, ":")
	for _, name := range []string{"USERPROFILE/p", "KAIRO_ATTEMPT_TOKEN/u", "KAIRO_ATTEMPT_ID/u", "KAIRO_API_CA/u", "CUDA_VISIBLE_DEVICES/u", "RANK/u", "MASTER_PORT/u", "NCCL_SOCKET_IFNAME/u"} {
		if !slices.Contains(shared, name) {
			t.Fatalf("WSLENV %q lacks %s", wslenv, name)
		}
	}
}

func TestNativeLaunchEnvironment(t *testing.T) {
	t.Setenv("KAIRO_TOKEN", "kairo_admin_operator")
	t.Setenv("KAIRO_ATTEMPT_TOKEN", "kairo_worker_stale")
	e := &Local{ID: "windows-local", NodeID: "host", Kind: "windows", APIURL: "https://127.0.0.1:7474", Attributes: map[string]string{}}
	cmd := e.command(testLaunch())
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
