package executor

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"kairo/internal/store"
)

// The variables naming what Kairo hands an attempt name it where it is
// mounted in the container: a path itself, or one under it (a published
// checkpoint), the longest match first.
func TestTranslatePaths(t *testing.T) {
	paths := map[string]string{
		"/srv/store/scratch/att_1":       "/kairo/scratch",
		"/srv/store/outputs/exe_1":       "/kairo/output",
		"/srv/store/outputs/exe_1-inner": "/kairo/inputs/inner",
		"/srv/store/cache/p":             "/kairo/cache",
	}
	got := translatePaths([]string{
		"KAIRO_SCRATCH=/srv/store/scratch/att_1",
		"KAIRO_CONTINUATION_REF=/srv/store/outputs/exe_1/ckpt/step_10.pt",
		"KAIRO_INPUT_INNER=/srv/store/outputs/exe_1-inner",
		"KAIRO_CACHE_DIR=/srv/store/cache/p/",
		"X=/srv/store/cache/p/torch",
		"OTHER=/srv/store/outputs/exe_10",
		"PLAIN=value",
	}, paths)
	want := []string{
		"KAIRO_SCRATCH=/kairo/scratch",
		"KAIRO_CONTINUATION_REF=/kairo/output/ckpt/step_10.pt",
		"KAIRO_INPUT_INNER=/kairo/inputs/inner",
		"KAIRO_CACHE_DIR=/kairo/cache/",
		"X=/kairo/cache/torch",
		"OTHER=/srv/store/outputs/exe_10",
		"PLAIN=value",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// In the container the attempt's variables replace the image's of the same
// name; with a GPU the driver's libraries come first and its programs last.
func TestContainerEnv(t *testing.T) {
	image := []string{"PATH=/usr/local/bin:/usr/bin", "LANG=C.UTF-8", "LD_LIBRARY_PATH=/opt/lib", "container=lxc"}
	attempt := []string{"LANG=ja_JP.UTF-8", "KAIRO_ATTEMPT_ID=att_1"}
	got := containerEnv(image, attempt, false)
	if containerEnvValue(got, "LANG") != "ja_JP.UTF-8" || containerEnvValue(got, "PATH") != "/usr/local/bin:/usr/bin" || len(got) != 5 {
		t.Fatalf("env %v", got)
	}
	got = containerEnv(image, attempt, true)
	if containerEnvValue(got, "LD_LIBRARY_PATH") != "/kairo/nvidia/lib:/opt/lib" || containerEnvValue(got, "PATH") != "/usr/local/bin:/usr/bin:/kairo/nvidia/bin" {
		t.Fatalf("gpu env %v", got)
	}
	if got := containerEnv(nil, nil, true); containerEnvValue(got, "LD_LIBRARY_PATH") != "/kairo/nvidia/lib" || !strings.HasSuffix(containerEnvValue(got, "PATH"), ":/kairo/nvidia/bin") {
		t.Fatalf("gpu env without the image's %v", got)
	}
}

// An execution in an image runs only on an executor that runs containers,
// and such an executor runs nothing else; instance names are Incus names.
func TestContainerLaunchesAreRefusedElsewhere(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	host := NewLocal("host", nil, t.TempDir(), logger)
	host.Kind = "linux"
	launch := &store.Launch{Attempt: store.Attempt{ID: "att_0123"}, Image: "docker.io/library/python@sha256:" + strings.Repeat("a", 64), Argv: []string{"python"}, CWD: "/"}
	if _, _, err := host.backend().launcher(launch); err == nil || !strings.Contains(err.Error(), "does not run containers") {
		t.Fatalf("a host executor: %v", err)
	}
	ctr := NewLocal("ctr", nil, t.TempDir(), logger)
	ctr.Kind = "incus"
	launch.Image = ""
	if _, err := ctr.containerCommand(launch); err == nil || !strings.Contains(err.Error(), "names no image") {
		t.Fatalf("no image: %v", err)
	}
	if got := containerInstance("att_0123abcd"); got != "kairo-att-0123abcd" {
		t.Fatalf("instance %q", got)
	}
}

// The OCI remote of a registry is found by its address, as Incus 7 lists
// it (Addrs) and as Incus 6 did (Addr), the first by name.
func TestFindOCIRemote(t *testing.T) {
	list := `{"images":{"Addrs":["https://images.linuxcontainers.org"],"Protocol":"simplestreams"},
		"kairo-docker-io":{"Addrs":["https://docker.io"],"Protocol":"oci"},
		"docker":{"Addrs":["https://docker.io/"],"Protocol":"oci"},
		"old":{"Addr":"https://ghcr.io","Protocol":"oci"}}`
	for addr, want := range map[string]string{"https://docker.io": "docker", "https://ghcr.io": "old", "https://quay.io": ""} {
		if got, err := findOCIRemote([]byte(list), addr); err != nil || got != want {
			t.Errorf("findOCIRemote(%s) = %q, %v; want %q", addr, got, err, want)
		}
	}
}
