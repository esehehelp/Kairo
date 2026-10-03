// Package conformance loads the cross-language client contract in
// sdk/conformance for Go tests. Each fixture explains its rules in $comment;
// every Kairo client replays the same files.
package conformance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Dir is the sdk/conformance directory of this checkout.
func Dir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "sdk", "conformance")
}

// Load decodes sdk/conformance/<name> into target, failing the test if it
// cannot.
func Load(t testing.TB, name string, target any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(Dir(), name))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, target); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}
