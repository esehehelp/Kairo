package orchestration

import (
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
