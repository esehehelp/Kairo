package execution

import (
	"strings"
	"testing"
)

const envBase = `schema_version=2
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
`

// The digest was computed before env existed.
func TestExecutionWithoutEnvKeepsItsDigest(t *testing.T) {
	parsed, err := Parse([]byte(envBase))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Digest != "aacd8a34bc39ba9a4a49ebb9c9acd588a17577246eee005a2b72d3044bcdacf0" {
		t.Fatalf("digest changed: %s\n%s", parsed.Digest, parsed.Normalized)
	}
}

func TestParseExecutionEnv(t *testing.T) {
	parsed, err := Parse([]byte(envBase + "[env]\nPYTHONUTF8 = \"1\"\nPath = 'C:\\tools'\n"))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Manifest.Env["PYTHONUTF8"] != "1" || parsed.Manifest.Env["Path"] != `C:\tools` {
		t.Fatalf("env: %v", parsed.Manifest.Env)
	}
	if !strings.Contains(string(parsed.Normalized), `"env":{"PYTHONUTF8":"1","Path":"C:\\tools"}`) {
		t.Fatalf("normalized: %s", parsed.Normalized)
	}
	for _, bad := range []string{"KAIRO_TOKEN = \"x\"\n", "WSLENV = \"x\"\n", "CUDA_VISIBLE_DEVICES = \"-1\"\n", "\"A=B\" = \"x\"\n"} {
		if _, err := Parse([]byte(envBase + "[env]\n" + bad)); err == nil {
			t.Fatalf("accepted env %q", bad)
		}
	}
	cpu := strings.Replace(envBase, "[[exclusive]]\nkind=\"gpu\"\ncount=2\n", "", 1)
	if _, err := Parse([]byte(cpu + "[env]\nCUDA_VISIBLE_DEVICES = \"-1\"\n")); err != nil {
		t.Fatalf("CPU execution cannot hide GPUs: %v", err)
	}
}
