package executor

import (
	"io"
	"log/slog"
	"slices"
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
	// an agent on Windows: its paths with either separator
	got = translatePaths([]string{
		`HOME=C:\_kairo-volatile\scratch\att_1\home`,
		`KAIRO_CONTINUATION_REF=C:\_kairo-volatile\outputs\exe_1\ckpt\step_10.pt`,
		"X=C:/_kairo-volatile/scratch/att_1/tmp",
		`KAIRO_SCRATCH=C:\_kairo-volatile\scratch\att_1`,
		`OTHER=C:\_kairo-volatile\scratch\att_10`,
	}, map[string]string{`C:\_kairo-volatile\scratch\att_1`: "/kairo/scratch", `C:\_kairo-volatile\outputs\exe_1`: "/kairo/output"})
	want = []string{
		"HOME=/kairo/scratch/home",
		"KAIRO_CONTINUATION_REF=/kairo/output/ckpt/step_10.pt",
		"X=/kairo/scratch/tmp",
		"KAIRO_SCRATCH=/kairo/scratch",
		`OTHER=C:\_kairo-volatile\scratch\att_10`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("Windows: got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
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
	if _, err := host.backend().launcher(launch); err == nil || !strings.Contains(err.Error(), "runs no containers") {
		t.Fatalf("a host executor: %v", err)
	}
	ctr := NewLocal("ctr", nil, t.TempDir(), logger)
	ctr.Kind = "incus"
	launch.Image = ""
	if _, err := ctr.containerCommand(launch, containerOptions{runArg: containerRunArg}); err == nil || !strings.Contains(err.Error(), "names no image") {
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

// The attempt's user is the image's own for its uid when it has one; else
// kairo (kairo-attempt where the image uses that name for another id), with
// the lines its passwd and group lack.
func TestTheAttemptUserIsNamedInTheImage(t *testing.T) {
	base := "root:x:0:0:root:/root:/bin/bash\nnobody:x:65534:65534:nobody:/nonexistent:/usr/sbin/nologin\n"
	groups := "root:x:0:\nnogroup:x:65534:\n"
	for _, tc := range []struct {
		name, passwd, group, want, wantPasswd, wantGroup string
	}{
		{"none", base, groups, "kairo", "kairo:x:1000:1000:Kairo attempt:/kairo/scratch/home:/bin/sh", "kairo:x:1000:"},
		{"the image's", base + "ubuntu:x:1000:1000:Ubuntu:/home/ubuntu:/bin/bash\n", groups + "ubuntu:x:1000:\n", "ubuntu", "", ""},
		{"its name taken", base + "kairo:x:999:999::/:/bin/sh", groups + "kairo:x:999:", "kairo-attempt",
			"kairo-attempt:x:1000:1000:Kairo attempt:/kairo/scratch/home:/bin/sh", "kairo-attempt:x:1000:"},
		{"no files", "", "", "kairo", "kairo:x:1000:1000:Kairo attempt:/kairo/scratch/home:/bin/sh", "kairo:x:1000:"},
	} {
		name, passwd, group := attemptAccount(tc.passwd, tc.group)
		if name != tc.want || passwd != tc.wantPasswd || group != tc.wantGroup {
			t.Errorf("%s: %q %q %q", tc.name, name, passwd, group)
		}
	}
}

// A continuation run in newer code than its checkpoint was made in runs its
// streams' stages where the checkpoint's ran (issue #49): that snapshot is
// handed to it besides its own, and its loaders run there with it as their
// source; otherwise they run with the argv.
func TestStreamStagesRunWhereTheCheckpointsRan(t *testing.T) {
	program, stages := &store.LaunchSource{ID: "src_new"}, &store.LaunchSource{ID: "src_old"}
	streams := []store.LaunchStream{{Name: "mix"}}
	for _, c := range []struct {
		launch store.Launch
		want   *store.LaunchSource
	}{
		{store.Launch{Source: program, StreamSource: stages, Streams: streams}, stages},
		{store.Launch{StreamSource: stages, Streams: streams}, stages},
		{store.Launch{Source: program, StreamSource: &store.LaunchSource{ID: "src_new"}, Streams: streams}, nil},
		{store.Launch{Source: program, StreamSource: stages}, nil},
		{store.Launch{Source: program, Streams: streams}, nil},
	} {
		if got := streamSourceOf(&c.launch); got != c.want {
			t.Fatalf("%+v: %+v, want %+v", c.launch, got, c.want)
		}
	}
	env := []string{"KAIRO_SOURCE_DIR=/kairo/src", "KAIRO_SOURCE_ID=src_new", "PATH=/usr/bin"}
	dir, got := stagePlace(containerSpec{CWD: "/kairo/src/train"}, env)
	if dir != "/kairo/src/train" || strings.Join(got, " ") != strings.Join(env, " ") {
		t.Fatalf("with the argv: %s %v", dir, got)
	}
	dir, got = stagePlace(containerSpec{CWD: "/kairo/src/train", StreamCWD: "/kairo/stream-src/train", StreamSource: "src_old"}, env)
	if dir != "/kairo/stream-src/train" || !slices.Contains(got, "KAIRO_SOURCE_DIR=/kairo/stream-src") || !slices.Contains(got, "KAIRO_SOURCE_ID=src_old") ||
		!slices.Contains(got, "PATH=/usr/bin") || slices.Contains(got, "KAIRO_SOURCE_DIR=/kairo/src") {
		t.Fatalf("where the checkpoint's ran: %s %v", dir, got)
	}
	if env[0] != "KAIRO_SOURCE_DIR=/kairo/src" {
		t.Fatalf("the argv's environment changed: %v", env)
	}
}
