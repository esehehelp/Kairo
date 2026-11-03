package orchestration

import (
	"strings"
	"testing"
)

// scratch_tmpdir is a task execution field like any other: [defaults]
// gives it, a task's own value wins.
func TestParseScratchTmpdir(t *testing.T) {
	source := `schema_version = 1
[project]
name = "p"
[defaults]
queue = "q"
policy = "RunToCompletion@v1"
[defaults.execution]
schema_version = 2
argv = ["run"]
cwd = "/work"
scratch_tmpdir = true

[[tasks]]
name = "a"

[[tasks]]
name = "b"
[tasks.execution]
scratch_tmpdir = false
`
	parsed, err := Parse([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	if a, b := parsed.Manifest.Tasks[0].Execution, parsed.Manifest.Tasks[1].Execution; !a.ScratchTmpdir || b.ScratchTmpdir {
		t.Fatalf("scratch_tmpdir: a %v, b %v", a.ScratchTmpdir, b.ScratchTmpdir)
	}
	if n := strings.Count(string(parsed.Normalized), `"scratch_tmpdir":true`); n != 1 {
		t.Fatalf("scratch_tmpdir %d times in the normalized spec: %s", n, parsed.Normalized)
	}
}
