package plan

import "testing"

func TestParseDefaultsAndRejectsCycle(t *testing.T) {
	source := []byte(`schema_version=1
project="p"
queue="q"
[[tasks]]
key="a"
argv=["run"]
cwd="."
depends_on=[]
[[tasks.exclusive]]
kind="gpu"
count=1
`)
	v, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	if v.Manifest.Tasks[0].Retry.MaxFailureAttempts != 1 || v.Manifest.Tasks[0].Exclusive[0].OnExternalClaim != "wait" {
		t.Fatalf("defaults missing: %+v", v.Manifest.Tasks[0])
	}
	if v.Digest == "" || len(v.Normalized) == 0 {
		t.Fatal("normalization missing")
	}
	_, err = Parse([]byte(`schema_version=1
project="p"
queue="q"
[[tasks]]
key="a"
argv=["run"]
cwd="."
depends_on=["b"]
[[tasks]]
key="b"
argv=["run"]
cwd="."
depends_on=["a"]
`))
	if err == nil {
		t.Fatal("cycle was accepted")
	}
}
