package orchestration

import (
	"strings"
	"testing"
)

func validManifest() string {
	return `schema_version = 1
[project]
name = "neo-ime"

[[tasks]]
name = "prepare"
queue = "data"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["uv", "run", "neo-ime", "prepare"]
cwd = "/work"
checkpointable = false
preemptible = false

[[tasks]]
name = "train"
queue = "training"
depends_on = ["prepare"]
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["uv", "run", "neo-ime", "train"]
cwd = "/work"
checkpointable = true
preemptible = true
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
`
}

func TestParseProjectTaskDAG(t *testing.T) {
	parsed, err := Parse([]byte(validManifest()))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Manifest.Project.Name != "neo-ime" || len(parsed.Manifest.Tasks) != 2 {
		t.Fatalf("unexpected manifest: %+v", parsed.Manifest)
	}
	if parsed.Manifest.Tasks[1].Name != "train" || parsed.Manifest.Tasks[1].DependsOn[0] != "prepare" {
		t.Fatalf("task DAG was not normalized: %+v", parsed.Manifest.Tasks)
	}
	if parsed.Digest == "" || parsed.TaskDigests["train"] == "" {
		t.Fatal("normalized declarations require stable digests")
	}
}

func TestParseRejectsGraphIDAndNestedNodeModel(t *testing.T) {
	for _, field := range []string{"graph_id = \"legacy\"\n", "[[tasks.nodes]]\nname = \"nested\"\n"} {
		source := strings.Replace(validManifest(), "[project]\n", "[project]\n"+field, 1)
		if _, err := Parse([]byte(source)); err == nil {
			t.Fatalf("field was accepted: %s", field)
		}
	}
}

func TestParseRejectsTaskCycles(t *testing.T) {
	source := strings.Replace(validManifest(), "depends_on = []", "depends_on = [\"train\"]", 1)
	if _, err := Parse([]byte(source)); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle error=%v", err)
	}
}

func defaultsManifest() string {
	return `schema_version = 1
[project]
name = "neo-ime"

[defaults]
policy = "RunToCompletion@v1"
[defaults.execution]
schema_version = 2
cwd = "/work"
checkpointable = false
preemptible = false

[[tasks]]
name = "prepare"
queue = "data"
[tasks.execution]
argv = ["uv", "run", "neo-ime", "prepare"]

[[tasks]]
name = "train"
queue = "training"
depends_on = ["prepare"]
[tasks.execution]
argv = ["uv", "run", "neo-ime", "train"]
checkpointable = true
preemptible = true
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
`
}

func TestParseDefaultsMatchFullyWrittenSpec(t *testing.T) {
	full, err := Parse([]byte(validManifest()))
	if err != nil {
		t.Fatal(err)
	}
	short, err := Parse([]byte(defaultsManifest()))
	if err != nil {
		t.Fatal(err)
	}
	if short.Digest != full.Digest {
		t.Fatalf("digest differs:\nfull  %s\nshort %s", full.Normalized, short.Normalized)
	}
	for name, digest := range full.TaskDigests {
		if short.TaskDigests[name] != digest {
			t.Fatalf("task %q digest differs", name)
		}
	}
}

func TestParseDefaultsMergeTablesAndReplaceArrays(t *testing.T) {
	source := `schema_version = 1
[project]
name = "p"
[defaults]
queue = "eval"
policy = "RunToCompletion@v1"
[defaults.execution]
schema_version = 2
argv = ["default"]
cwd = "/work"
[defaults.execution.executor]
labels = { environment = "windows" }
[defaults.execution.capacity]
cpu_millis = 8000
ram_bytes = 4096

[[tasks]]
name = "a"
[tasks.execution]
argv = ["run", "a"]
[tasks.execution.executor]
labels = { site = "pve0" }
[tasks.execution.capacity]
cpu_millis = 0
`
	parsed, err := Parse([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	task := parsed.Manifest.Tasks[0]
	execution := task.Execution
	if task.Queue != "eval" || task.DependsOn == nil || len(task.DependsOn) != 0 {
		t.Fatalf("task fields: %+v", task)
	}
	if strings.Join(execution.Argv, " ") != "run a" {
		t.Fatalf("argv was merged instead of replaced: %v", execution.Argv)
	}
	if execution.Capacity.CPUMillis != 0 || execution.Capacity.RAMBytes != 4096 {
		t.Fatalf("capacity: %+v", execution.Capacity)
	}
	if execution.Executor.Labels["environment"] != "windows" || execution.Executor.Labels["site"] != "pve0" {
		t.Fatalf("labels: %+v", execution.Executor.Labels)
	}
}

func TestParseDefaultsRejectsTaskIdentityAndUnknownFields(t *testing.T) {
	for _, field := range []string{"name = \"x\"\n", "depends_on = []\n"} {
		source := strings.Replace(defaultsManifest(), "[defaults]\n", "[defaults]\n"+field, 1)
		if _, err := Parse([]byte(source)); err == nil || !strings.Contains(err.Error(), "not allowed") {
			t.Fatalf("defaults field %q: err=%v", field, err)
		}
	}
	source := strings.Replace(defaultsManifest(), "[defaults.execution]\n", "[defaults.execution]\ntypo = 1\n", 1)
	if _, err := Parse([]byte(source)); err == nil || !strings.Contains(err.Error(), "unknown project declaration field") {
		t.Fatalf("unknown field in defaults: err=%v", err)
	}
}

func TestParseWithoutDefaultsKeepsOmittedDependsOnAsNull(t *testing.T) {
	source := strings.Replace(validManifest(), "depends_on = []\n", "", 1)
	parsed, err := Parse([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	full, _ := Parse([]byte(validManifest()))
	if parsed.TaskDigests["prepare"] == full.TaskDigests["prepare"] {
		t.Fatal("a spec without [defaults] must keep its existing digests (omitted depends_on is null)")
	}
}
