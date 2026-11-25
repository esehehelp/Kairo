package orchestration

import (
	"strings"
	"testing"
)

const imageManifest = `schema_version = 1
[project]
name = "neo"
[sources.code]
paths = ["src"]
[images.py]
ref = "python:3.12-slim"
[[tasks]]
name = "train"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["python", "-m", "train"]
source = "code"
image = "py"
`

const pin = "docker.io/library/python@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b"

// A task runs in a declared image, its reference written in full; the pins
// an apply sends are not part of the digest and must be of the image's
// repository; an image not declared, a gang (no more), or a cwd that is not absolute
// without a source are refused.
func TestATaskRunsInADeclaredImage(t *testing.T) {
	v, err := Parse([]byte(imageManifest))
	if err != nil {
		t.Fatal(err)
	}
	if v.Manifest.Images["py"].Ref != "docker.io/library/python:3.12-slim" || v.Manifest.Tasks[0].Execution.Image != "py" {
		t.Fatalf("parsed %+v", v.Manifest.Images)
	}
	m := v.Manifest
	m.ImagePins = map[string]string{"py": pin}
	again, err := Normalize(m)
	if err != nil || again.Digest != v.Digest || again.TaskDigests["train"] != v.TaskDigests["train"] || again.Manifest.ImagePins["py"] != pin {
		t.Fatalf("pins changed the digest: %s %s %v", again.Digest, v.Digest, err)
	}
	m.ImagePins = map[string]string{"py": strings.Replace(pin, "library/python", "library/ruby", 1)}
	if _, err := Normalize(m); err == nil || !strings.Contains(err.Error(), "is not of") {
		t.Fatalf("a pin of another repository: %v", err)
	}
	m.ImagePins = map[string]string{"py": "docker.io/library/python:3.12"}
	if _, err := Normalize(m); err == nil {
		t.Fatal("a pin without a digest")
	}
	for _, bad := range []struct{ from, to, want string }{
		{`image = "py"`, `image = "other"`, "which is no [images.other]"},
		{`ref = "python:3.12-slim"`, `ref = "Python"`, "repository element"},
		{"source = \"code\"\n", "cwd = \"work\"\n", "must be an absolute path in the image"},
		{`image = "py"`, "image = \"py\"\n[tasks.execution.gang]\nsize = 2", "gangs are no more"},
	} {
		if _, err := Parse([]byte(strings.Replace(imageManifest, bad.from, bad.to, 1))); err == nil || !strings.Contains(err.Error(), bad.want) {
			t.Fatalf("%s: %v", bad.to, err)
		}
	}
	// without a source, an absolute cwd in the image
	if _, err := Parse([]byte(strings.Replace(imageManifest, "source = \"code\"\n", "cwd = \"/work\"\n", 1))); err != nil {
		t.Fatal(err)
	}
}

// A manifest without images keeps the digest it had before images existed.
func TestNoImagesKeepsTheDigest(t *testing.T) {
	v, err := Parse([]byte(sourceManifest))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(v.Normalized), "image") {
		t.Fatalf("normalized mentions images: %s", v.Normalized)
	}
}
