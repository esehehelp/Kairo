package execution

import (
	"strings"
	"testing"
)

func TestParseExecutionDefaults(t *testing.T) {
	parsed, err := Parse([]byte(`schema_version=2
client_request_id="train/one"
project="llm-develop"
queue="training"
task="model"
argv=["python", "train.py"]
cwd="/work"
checkpointable=true
preemptible=true
[[exclusive]]
kind="gpu"
count=2
`))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Manifest.Exclusive[0].OnExternalClaim != "wait" || parsed.Manifest.Capacity.Strength != "admitted" {
		t.Fatalf("defaults missing: %+v", parsed.Manifest)
	}
	if parsed.Digest == "" || len(parsed.Normalized) == 0 {
		t.Fatal("normalization metadata missing")
	}
}

func TestParseExecutionRejectsWorkflowFields(t *testing.T) {
	base := `schema_version=2
client_request_id="one"
project="p"
argv=["run"]
cwd="."
`
	for _, field := range []string{"depends_on", "retry", "condition", "gate", "artifact_dependency", "dynamic_fan_out"} {
		_, err := Parse([]byte(base + field + `="project-owned"` + "\n"))
		if err == nil || !strings.Contains(err.Error(), "unknown execution field") {
			t.Fatalf("workflow field %q was accepted: %v", field, err)
		}
	}
}

func TestParseExecutionRequiresCheckpointablePreemption(t *testing.T) {
	_, err := Parse([]byte(`schema_version=2
client_request_id="one"
project="p"
argv=["run"]
cwd="."
preemptible=true
`))
	if err == nil {
		t.Fatal("preemptible non-checkpointable execution was accepted")
	}
}
