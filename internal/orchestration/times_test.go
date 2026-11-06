package orchestration

import (
	"fmt"
	"strings"
	"testing"
)

func timesSpec(fields string) string {
	return `schema_version = 1
[project]
name = "rhllem"

[defaults]
policy = "RunToCompletion@v1"
[defaults.execution]
schema_version = 2
cwd = "/work"
checkpointable = false
preemptible = false

[[tasks]]
name = "predict"
queue = "live"
[tasks.execution]
argv = ["predict"]
` + fields + `
`
}

// not_before, start_by and finish_by take TOML offset date-times or RFC 3339
// strings, keep the offset they were written with, and leave the digest of a
// task without them as it was.
func TestParseTimesKeepTheirOffsetAndOtherDigests(t *testing.T) {
	plain, err := Parse([]byte(timesSpec("")))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain.Normalized), "not_before") {
		t.Fatalf("a task without times names them: %s", plain.Normalized)
	}
	native, err := Parse([]byte(timesSpec("not_before = 2026-11-08T06:50:00+09:00\nstart_by = 2026-11-08T07:30:00.5+09:00")))
	if err != nil {
		t.Fatal(err)
	}
	quoted, err := Parse([]byte(timesSpec(`not_before = "2026-11-08T06:50:00+09:00"` + "\n" + `start_by = "2026-11-08T07:30:00+09:00"`)))
	if err != nil {
		t.Fatal(err)
	}
	e := native.Manifest.Tasks[0].Execution
	if e.NotBefore != "2026-11-08T06:50:00+09:00" || e.StartBy != "2026-11-08T07:30:00+09:00" || e.FinishBy != "" {
		t.Fatalf("times %q %q %q", e.NotBefore, e.StartBy, e.FinishBy)
	}
	if native.Digest != quoted.Digest || native.Digest == plain.Digest {
		t.Fatalf("digests: native %s quoted %s plain %s", native.Digest, quoted.Digest, plain.Digest)
	}
	// [defaults.execution] reaches every task as any other field does.
	withDefault := strings.Replace(timesSpec(""), "preemptible = false\n", "preemptible = false\nfinish_by = 2026-11-08T17:00:00+09:00\n", 1)
	defaulted, err := Parse([]byte(withDefault))
	if err != nil {
		t.Fatal(err)
	}
	if got := defaulted.Manifest.Tasks[0].Execution.FinishBy; got != "2026-11-08T17:00:00+09:00" {
		t.Fatalf("finish_by from defaults: %q", got)
	}
}

func TestParseTimesValidation(t *testing.T) {
	for _, c := range []struct{ fields, want string }{
		{"not_before = 2026-11-08T06:50:00", "no offset"},
		{`not_before = "2026-11-08 06:50"`, "RFC 3339"},
		{"not_before = 2026-11-08", "no offset"},
		{"not_before = 7", "offset date-time"},
		{"not_before = 2026-11-08T08:00:00+09:00\nstart_by = 2026-11-08T07:30:00+09:00", "start_by is before not_before"},
		{"not_before = 2026-11-08T08:00:00+09:00\nfinish_by = 2026-11-07T23:00:00Z", "finish_by is not after not_before"},
		{"start_by = 2026-11-08T08:00:00+09:00\nfinish_by = 2026-11-08T07:00:00+09:00", "finish_by is before start_by"},
	} {
		_, err := Parse([]byte(timesSpec(c.fields)))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%q: got %v, want %q", c.fields, err, c.want)
		}
	}
}

// A task input names a task the declaring task depends on; inputs are named
// once each, a letter then letters, digits and _, with exactly one source.
func TestInputsOfATask(t *testing.T) {
	base := `schema_version = 1
[project]
name = "p"
[[tasks]]
name = "train"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["true"]
cwd = "/work"
[[tasks]]
name = "decay"
queue = "q"
depends_on = %s
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["true"]
cwd = "/work"
%s`
	for _, c := range []struct{ deps, inputs, wantErr string }{
		{`["train"]`, "[[tasks.execution.inputs]]\nname = \"init\"\ntask = \"train\"\n", ""},
		{`["train"]`, "[[tasks.execution.inputs]]\nname = \"data\"\ndata = \"sumire/p600m\"\n", ""},
		{`[]`, "[[tasks.execution.inputs]]\nname = \"init\"\ntask = \"train\"\n", "does not depend on"},
		{`["train"]`, "[[tasks.execution.inputs]]\nname = \"init\"\ntask = \"train\"\ndata = \"x\"\n", "exactly one of"},
		{`["train"]`, "[[tasks.execution.inputs]]\nname = \"1st\"\ntask = \"train\"\n", "is not a letter"},
		{`["train"]`, "[[tasks.execution.inputs]]\nname = \"a\"\ndata = \"x\"\n[[tasks.execution.inputs]]\nname = \"A\"\ndata = \"y\"\n", "named twice"},
	} {
		_, err := Parse([]byte(fmt.Sprintf(base, c.deps, c.inputs)))
		if c.wantErr == "" && err != nil || c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
			t.Fatalf("deps %s inputs %q: %v, want %q", c.deps, c.inputs, err, c.wantErr)
		}
	}
}
