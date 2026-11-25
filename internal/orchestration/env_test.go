package orchestration

import (
	"strings"
	"testing"
)

// The digests were computed before env existed: a spec without env must keep
// them, or every applied project would conflict with its own declaration.
func TestSpecWithoutEnvKeepsItsDigests(t *testing.T) {
	for _, c := range []struct {
		source, spec string
		tasks        map[string]string
	}{
		{validManifest(), "8ea36af8d36273cd6767b0c7945a21e2a7d7b20229b7629ea8e54a271374060f", map[string]string{
			"prepare": "f6e3762d094af3352ddcfc328ee1a3ed44b8aa40a26bdd3e7956f1cfc0d0758c",
			"train":   "794c8579601a0394b28e12aaf6c723cd3052cb72bad4a73f875d12d2b672b12f",
		}},
	} {
		parsed, err := Parse([]byte(c.source))
		if err != nil {
			t.Fatal(err)
		}
		if parsed.Digest != c.spec {
			t.Fatalf("spec digest changed: %s\n%s", parsed.Digest, parsed.Normalized)
		}
		for name, digest := range c.tasks {
			if parsed.TaskDigests[name] != digest {
				t.Fatalf("task %q digest changed: %s", name, parsed.TaskDigests[name])
			}
		}
	}
}

func TestParseEnvMergesDefaultsKeyByKey(t *testing.T) {
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
[defaults.execution.env]
PYTHONUTF8 = "1"
HF_HOME = "D:/hf"

[[tasks]]
name = "a"
[tasks.execution.env]
HF_HOME = "/srv/hf"
OMP_NUM_THREADS = "4"

[[tasks]]
name = "b"
`
	parsed, err := Parse([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	a, b := parsed.Manifest.Tasks[0].Execution, parsed.Manifest.Tasks[1].Execution
	if len(a.Env) != 3 || a.Env["PYTHONUTF8"] != "1" || a.Env["HF_HOME"] != "/srv/hf" || a.Env["OMP_NUM_THREADS"] != "4" {
		t.Fatalf("task a env: %v", a.Env)
	}
	if len(b.Env) != 2 || b.Env["HF_HOME"] != "D:/hf" {
		t.Fatalf("task b env: %v", b.Env)
	}
}

// There are no gangs any more: a declaration with one is refused, saying so.
func TestAGangIsRefused(t *testing.T) {
	source := `schema_version = 1
[project]
name = "p"
[[tasks]]
name = "ddp"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["train"]
cwd = "/work"
[tasks.execution.gang]
size = 2
`
	if _, err := Parse([]byte(source)); err == nil || !strings.Contains(err.Error(), "gangs are no more") {
		t.Fatalf("a gang: %v", err)
	}
}

func TestParseEnvValidation(t *testing.T) {
	task := func(gpu bool, env string) string {
		source := `schema_version = 1
[project]
name = "p"
[[tasks]]
name = "a"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["run"]
cwd = "/work"
`
		if gpu {
			source += "[[tasks.execution.exclusive]]\nkind = \"gpu\"\ncount = 1\n"
		}
		return source + env
	}
	for _, env := range []string{
		"[tasks.execution.env]\nKAIRO_TOKEN = \"x\"\n",
		"[tasks.execution.env]\nkairo_api_url = \"x\"\n",
		"[tasks.execution.env]\n\"A=B\" = \"x\"\n",
		"[tasks.execution.env]\n\" A\" = \"x\"\n",
		"[tasks.execution.env]\n\"\" = \"x\"\n",
		"[tasks.execution.env]\nA = 1\n",
	} {
		if _, err := Parse([]byte(task(false, env))); err == nil {
			t.Fatalf("accepted env %q", env)
		}
	}
	cuda := "[tasks.execution.env]\nCUDA_VISIBLE_DEVICES = \"-1\"\n"
	if _, err := Parse([]byte(task(true, cuda))); err == nil {
		t.Fatal("accepted CUDA_VISIBLE_DEVICES on a GPU task")
	}
	if _, err := Parse([]byte(task(false, cuda))); err != nil {
		t.Fatalf("CPU task cannot hide GPUs: %v", err)
	}
}
