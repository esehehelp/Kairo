package execution

import (
	"strings"
	"testing"
)

// scratch_tmpdir is read, and part of the digest only when set.
func TestParseExecutionScratchTmpdir(t *testing.T) {
	plain, err := Parse([]byte(envBase))
	if err != nil {
		t.Fatal(err)
	}
	if plain.Manifest.ScratchTmpdir || strings.Contains(string(plain.Normalized), "scratch_tmpdir") {
		t.Fatalf("scratch_tmpdir without being set: %s", plain.Normalized)
	}
	parsed, err := Parse([]byte(strings.Replace(envBase, "preemptible=true\n", "preemptible=true\nscratch_tmpdir=true\n", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Manifest.ScratchTmpdir || !strings.Contains(string(parsed.Normalized), `"scratch_tmpdir":true`) || parsed.Digest == plain.Digest {
		t.Fatalf("scratch_tmpdir: %s", parsed.Normalized)
	}
}
