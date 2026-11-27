package orchestration

import (
	"strings"
	"testing"
)

// continue_after_loss is for a checkpointable single execution, and is in
// the normalized spec only when set (existing digests do not change).
func TestParseContinueAfterLoss(t *testing.T) {
	spec := func(execution string) string {
		return `schema_version = 1
[project]
name = "p"
[[tasks]]
name = "train"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["train"]
cwd = "/work"
` + execution
	}
	parsed, err := Parse([]byte(spec("checkpointable = true\ncontinue_after_loss = true\n")))
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Manifest.Tasks[0].Execution.ContinueAfterLoss || !strings.Contains(string(parsed.Normalized), `"continue_after_loss":true`) {
		t.Fatalf("continue_after_loss not kept: %s", parsed.Normalized)
	}
	plain, err := Parse([]byte(spec("checkpointable = true\n")))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain.Normalized), "continue_after_loss") {
		t.Fatalf("continue_after_loss in a spec that does not set it: %s", plain.Normalized)
	}
	for name, execution := range map[string]string{
		"not checkpointable": "continue_after_loss = true\n",
		"a gang":             "checkpointable = true\ncontinue_after_loss = true\n[tasks.execution.gang]\nsize = 2\n",
	} {
		if _, err := Parse([]byte(spec(execution))); err == nil || !strings.Contains(err.Error(), "continue_after_loss") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
