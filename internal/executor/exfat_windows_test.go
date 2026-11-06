//go:build windows

package executor

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"kairo/internal/store"
)

// A copy renamed on exFAT (KAIRO_TEST_EXFAT_DIR, a directory on an exFAT
// volume) is verified at once: there a rename changes the file's id, so the
// final names are read back again rather than refused as written over.
func TestACopyRenamedOnExFATIsVerified(t *testing.T) {
	base := os.Getenv("KAIRO_TEST_EXFAT_DIR")
	if base == "" {
		t.Skip("KAIRO_TEST_EXFAT_DIR (a directory on an exFAT volume) is not set")
	}
	dirPath := filepath.Join(base, "verify-"+t.Name())
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dirPath) })
	dir, err := os.OpenRoot(dirPath)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	pack, list := []byte("pack body"), []byte("path\tbytes\tsha256\tmtime_ns\n")
	hash := func(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
	packName := "obj_x" + packSuffix + partialSuffix
	listName := "obj_x" + packSuffix + manifestSuffix + partialSuffix
	if err := os.WriteFile(filepath.Join(dirPath, packName), pack, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirPath, listName), list, 0o644); err != nil {
		t.Fatal(err)
	}
	o := store.CopyOrder{ObjectID: "obj_x", PackBytes: int64(len(pack)), PackSHA256: hash(pack), ManifestSHA256: hash(list)}
	made, err := verifyCopy(dir, dirPath, packName, listName, o, store.CopyMade{}, true)
	if err != nil || made.SHA256 != o.PackSHA256 {
		t.Fatalf("verify: %+v %v", made, err)
	}
}
