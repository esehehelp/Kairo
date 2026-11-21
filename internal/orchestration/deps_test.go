package orchestration

import (
	"slices"
	"strings"
	"testing"
)

// A task may depend on, and take as input, a task the file does not declare:
// one of the project's tasks declared by another file, which the daemon
// resolves when applying (issue #12). Depending on itself, or a cycle among
// the file's tasks, is still refused.
func TestATaskDependsOnATaskOfAnotherFile(t *testing.T) {
	spec := func(tasks string) string {
		return `schema_version = 1
[project]
name = "p"
[defaults]
queue = "q"
policy = "RunToCompletion@v1"
[defaults.execution]
schema_version = 2
argv = ["run"]
cwd = "/work"
` + tasks
	}
	parsed, err := Parse([]byte(spec(`
[[tasks]]
name = "judge"
depends_on = ["control-run", "smoke"]
[[tasks.execution.inputs]]
name = "control"
task = "control-run"

[[tasks]]
name = "smoke"
`)))
	if err != nil {
		t.Fatal(err)
	}
	if got := parsed.ExternalDependencies(); !slices.Equal(got, []string{"control-run"}) {
		t.Fatalf("external dependencies %v", got)
	}
	for name, tasks := range map[string]string{
		"itself": "\n[[tasks]]\nname = \"a\"\ndepends_on = [\"a\"]\n",
		"cycle":  "\n[[tasks]]\nname = \"a\"\ndepends_on = [\"b\"]\n\n[[tasks]]\nname = \"b\"\ndepends_on = [\"a\"]\n",
	} {
		if _, err := Parse([]byte(spec(tasks))); err == nil || !strings.Contains(err.Error(), "cycle") && !strings.Contains(err.Error(), "itself") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
