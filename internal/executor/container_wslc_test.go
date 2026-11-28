package executor

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// A wslc container runs Kairo's init as root (it names the attempt's user
// and starts the argv as it), with the mounts Kairo hands it and the GPU
// when the attempt leased one.
func TestWSLCRunsTheInitAsRoot(t *testing.T) {
	spec := containerSpec{Instance: "kairo-att-0123", AttemptID: "att_0123", Image: "docker.io/library/python@sha256:abc",
		GPUs: []string{"GPU-1"}, Mounts: []containerMount{{Source: `C:\k\run`, Path: "/kairo/run"}, {Source: `C:\k\src`, Path: "/kairo/src", ReadOnly: true}}}
	want := []string{"run", "--name", "kairo-att-0123", "--user", "0:0", "--entrypoint", "/kairo/bin/kairo",
		"--label", "kairo.attempt=att_0123", "--stop-signal", "SIGTERM", "--stop-timeout", "30", "--shm-size", "16g", "--pull", "never",
		"--gpus", "all", "--volume", `C:\k\run:/kairo/run`, "--volume", `C:\k\src:/kairo/src:ro`,
		"docker.io/library/python@sha256:abc", containerInitArg, containerSpecIn}
	if got := wslcRunArgs(spec); !reflect.DeepEqual(got, want) {
		t.Fatalf("wslc run %q\nwant %q", got, want)
	}
	if id, ok := attemptOfInstance(containerInstance("att_0123abcd")); !ok || id != "att_0123abcd" {
		t.Fatalf("attempt of %s: %s %v", containerInstance("att_0123abcd"), id, ok)
	}
	for _, other := range []string{"kokoro-verify", "kairo-att-", "kairo-att-a-b"} {
		if _, ok := attemptOfInstance(other); ok {
			t.Fatalf("%s taken as an attempt's", other)
		}
	}
}

// A wslc container is given the host's GPUs all or none: an attempt that
// leases fewer than the host has is not started (it could use another's),
// nor one where they cannot be counted. A container is taken for having
// processes in any state but those of one stopped or never started.
func TestAWSLCContainerLeasesAllOfTheHostsGPUs(t *testing.T) {
	saved := wslcGPUs
	defer func() { wslcGPUs = saved }()
	ctx := context.Background()
	wslcGPUs = func(context.Context) (int, error) { return 2, nil }
	if err := wslcGPUsWhole(ctx, nil); err != nil {
		t.Fatalf("no GPU leased: %v", err)
	}
	if err := wslcGPUsWhole(ctx, []string{"GPU-a"}); err == nil {
		t.Fatal("one of two GPUs leased, started")
	}
	if err := wslcGPUsWhole(ctx, []string{"GPU-a", "GPU-b"}); err != nil {
		t.Fatalf("both leased: %v", err)
	}
	wslcGPUs = func(context.Context) (int, error) { return 0, errors.New("no nvidia-smi") }
	if err := wslcGPUsWhole(ctx, []string{"GPU-a"}); err == nil {
		t.Fatal("GPUs not counted, started")
	}
	for state, alive := range map[string]bool{"running": true, "paused": true, "restarting": true, "removing": true, "exited": false, "created": false, "dead": false} {
		if wslcAlive(state) != alive {
			t.Fatalf("%s: alive %v", state, !alive)
		}
	}
}

// Another installation's containers on the host (another prefix) are none of
// this agent's: never taken for its attempts' leftovers, nor adopted.
func TestAContainerOfAnotherInstallationIsNoneOfThisAgents(t *testing.T) {
	defer func(p string) { ContainerPrefix = p }(ContainerPrefix)
	production := containerInstance("att_0123abcd")
	ContainerPrefix = "kairo-t-"
	if err := ValidContainerPrefix(ContainerPrefix); err != nil {
		t.Fatal(err)
	}
	test := containerInstance("att_0123abcd")
	if test != "kairo-t-att-0123abcd" {
		t.Fatalf("named %s", test)
	}
	if id, ok := attemptOfInstance(test); !ok || id != "att_0123abcd" {
		t.Fatalf("its own: %q %v", id, ok)
	}
	if _, ok := attemptOfInstance(production); ok {
		t.Fatalf("%s taken for this installation's", production)
	}
	for _, bad := range []string{"", "Kairo-", "kairo", "-kairo-", "kairo_t-"} {
		if ValidContainerPrefix(bad) == nil {
			t.Fatalf("prefix %q accepted", bad)
		}
	}
}
