package orchestration

import (
	"strings"
	"testing"
)

// gangDigestManifest is a gang task without env.
const gangDigestManifest = `schema_version = 1
[project]
name = "gangs"
[[tasks]]
name = "ddp"
queue = "training"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["wsl-train"]
cwd = "/mnt/d/work"
[[tasks.execution.exclusive]]
kind = "gpu"
count = 1
[tasks.execution.gang]
size = 2
[[tasks.execution.gang.ranks]]
rank = 1
argv = ["linux-train"]
cwd = "/srv/work"
executor = { labels = { environment = "linux" } }
`

// The digests were computed before env existed: a spec without env must keep
// them, or every applied project would conflict with its own declaration.
func TestSpecWithoutEnvKeepsItsDigests(t *testing.T) {
	for _, c := range []struct {
		source, spec string
		tasks        map[string]string
	}{
		{validManifest(), "3f95aaffe4564078a89faf70f1fa935ee1b7b25c1aed89df4d2c03f04e2466b8", map[string]string{
			"prepare": "0970cc079430f52b0ef3fcc6cf1a4afe980d3911c6d3248d6efb3f200fe6c0e1",
			"train":   "50adcaa461eeefe47e1d2c99815093a1d6df61088a60bef1a39f1106c141e5bf",
		}},
		{gangDigestManifest, "744ec6b2855b6948ba8f0af2884c5fb6d09743824eea47aeb47f6f42ab9784f1", map[string]string{
			"ddp": "39142e7adf75450e94bd2de8e5253bf4d28509a1fc07112c3ae496c370dbfd26",
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

func TestParseEnvMergesDefaultsKeyByKeyAndRankEnv(t *testing.T) {
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
[tasks.execution.gang]
size = 2
[[tasks.execution.gang.ranks]]
rank = 1
env = { OMP_NUM_THREADS = "8" }

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
	if rank := a.Gang.Ranks[0]; rank.Env["OMP_NUM_THREADS"] != "8" || len(rank.Env) != 1 {
		t.Fatalf("rank env: %v", rank.Env)
	}
	if !strings.Contains(string(parsed.Normalized), `"env":{"OMP_NUM_THREADS":"8"}`) {
		t.Fatalf("rank env missing from the normalized spec: %s", parsed.Normalized)
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
		"[tasks.execution.env]\nWslEnv = \"x\"\n",
		"[tasks.execution.env]\n\"A=B\" = \"x\"\n",
		"[tasks.execution.env]\n\" A\" = \"x\"\n",
		"[tasks.execution.env]\n\"\" = \"x\"\n",
		"[tasks.execution.env]\nA = 1\n",
		"[tasks.execution.gang]\nsize = 2\n[[tasks.execution.gang.ranks]]\nrank = 1\nenv = { KAIRO_GANG_RANK = \"0\" }\n",
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
