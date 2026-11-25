package executor

import (
	"reflect"
	"testing"
)

// A wslc container runs the image's init as an ordinary user, with the
// mounts Kairo hands it and the GPU when the attempt leased one.
func TestWSLCRunsTheInitAsAnOrdinaryUser(t *testing.T) {
	spec := containerSpec{Instance: "kairo-att-0123", AttemptID: "att_0123", Image: "docker.io/library/python@sha256:abc",
		GPUs: []string{"GPU-1"}, Mounts: []containerMount{{Source: `C:\k\run`, Path: "/kairo/run"}, {Source: `C:\k\src`, Path: "/kairo/src", ReadOnly: true}}}
	want := []string{"run", "--name", "kairo-att-0123", "--user", "1000:1000", "--entrypoint", "/kairo/bin/kairo",
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
