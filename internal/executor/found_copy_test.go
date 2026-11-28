package executor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"kairo/internal/store"
)

// A copy found on a durable store without a record is adopted only once its
// pack and file list read back as its description says: adopted, it counts
// as one of the object's copies (one that lets another be shed).
func TestACopyFoundIsAdoptedOnlyAsItsDescriptionSays(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "objects", "202611")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	id := "obj_found"
	pack, list := []byte("the pack"), []byte("a\t1\n")
	write := func(name string, body []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(id+packSuffix, pack)
	write(id+packSuffix+manifestSuffix, list)
	sum := func(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
	desc := store.ObjectDescription{ObjectID: id, PackBytes: int64(len(pack)), PackSHA256: sum(pack), ManifestSHA256: sum(list)}
	if err := checkFound(root, "202611", id, desc); err != nil {
		t.Fatalf("a whole copy: %v", err)
	}
	// cut short
	write(id+packSuffix, pack[:3])
	if err := checkFound(root, "202611", id, desc); !errors.Is(err, errCopyMismatch) {
		t.Fatalf("a copy cut short: %v", err)
	}
	// its list damaged
	write(id+packSuffix, pack)
	write(id+packSuffix+manifestSuffix, []byte("b\t1\n"))
	if err := checkFound(root, "202611", id, desc); !errors.Is(err, errCopyMismatch) {
		t.Fatalf("a damaged list: %v", err)
	}
	if err := checkFound(root, "2026-11", id, desc); err == nil {
		t.Fatal("a month outside the layout")
	}
}
