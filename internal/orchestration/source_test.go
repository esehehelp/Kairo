package orchestration

import (
	"strings"
	"testing"
)

const sourceManifest = `schema_version = 1
[project]
name = "neo"
[sources.code]
paths = ["src", "configs/", "pyproject.toml"]
exclude = ["__pycache__", "*.pyc"]
[[tasks]]
name = "train"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["uv", "run", "train"]
source = "code"
`

// A task runs in a declared source, its cwd relative to the snapshot ("."
// when not written); the snapshots an apply sends are not part of the
// digest; a source not declared, a path outside the root or an absolute
// cwd are refused.
func TestATaskRunsInADeclaredSource(t *testing.T) {
	v, err := Parse([]byte(sourceManifest))
	if err != nil {
		t.Fatal(err)
	}
	task := v.Manifest.Tasks[0]
	if task.Execution.Source != "code" || task.Execution.CWD != "." || strings.Join(v.Manifest.Sources["code"].Paths, ",") != "src,configs,pyproject.toml" {
		t.Fatalf("parsed %+v %+v", task.Execution, v.Manifest.Sources)
	}
	m := v.Manifest
	m.Snapshots = map[string]string{"code": "src_" + strings.Repeat("0", 32)}
	again, err := Normalize(m)
	if err != nil || again.Digest != v.Digest || again.TaskDigests["train"] != v.TaskDigests["train"] || again.Manifest.Snapshots["code"] == "" {
		t.Fatalf("snapshots changed the digest: %s %s %v", again.Digest, v.Digest, err)
	}
	m.Snapshots = map[string]string{"code": "obj_1"}
	if _, err := Normalize(m); err == nil {
		t.Fatal("a snapshot that is no snapshot id")
	}
	for _, bad := range []struct{ from, to, want string }{
		{`source = "code"`, `source = "other"`, "which is no [sources.other]"},
		{`paths = ["src", "configs/", "pyproject.toml"]`, `paths = ["../elsewhere"]`, "is not a path under its root"},
		{`source = "code"`, "source = \"code\"\ncwd = \"/work\"", "must be a directory of the snapshot"},
		{`source = "code"`, "source = \"code\"\ncwd = \"../up\"", "must be a directory of the snapshot"},
	} {
		if _, err := Parse([]byte(strings.Replace(sourceManifest, bad.from, bad.to, 1))); err == nil || !strings.Contains(err.Error(), bad.want) {
			t.Fatalf("%s: %v", bad.to, err)
		}
	}
}
