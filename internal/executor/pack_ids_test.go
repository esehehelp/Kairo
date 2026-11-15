package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A file opened is the file listed by its id; where ids are not stable
// (exFAT, FAT) a file of the size and mtime listed is taken for it, and one
// of another size or mtime never is.
func TestAFileOpenedIsTheFileListed(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 11, 16, 1, 2, 3, 0, time.UTC)
	stat := func(name, body string, mtime time.Time) os.FileInfo {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		return fi
	}
	a, b := stat("a", "x", at), stat("b", "y", at)
	longer, later := stat("c", "xy", at), stat("d", "x", at.Add(time.Second))
	switch {
	case !sameListedFile(a, a, true) || !sameListedFile(a, a, false):
		t.Fatal("a file is not itself")
	case sameListedFile(a, b, true):
		t.Fatal("another file of the same size and mtime is taken where ids are stable")
	case !sameListedFile(a, b, false):
		t.Fatal("where ids are not stable, a file of the size and mtime listed is not taken")
	case sameListedFile(a, longer, false) || sameListedFile(a, later, false):
		t.Fatal("a file of another size or mtime is taken")
	}
}

// KAIRO_TEST_EXFAT_DIR (an existing directory on exFAT, read only): it is
// identified as packing reads it, every file taken for the one listed.
func TestADirectoryOnExFATIsRead(t *testing.T) {
	dir := os.Getenv("KAIRO_TEST_EXFAT_DIR")
	if dir == "" {
		t.Skip("KAIRO_TEST_EXFAT_DIR is not set")
	}
	if stableFileIDs(dir) {
		t.Fatalf("%s is taken as having stable file ids", dir)
	}
	res, err := packDir(context.Background(), filepath.Dir(dir), dir, "obj_exfattest", true)
	if err != nil || res.files == 0 {
		t.Fatalf("identify %s: %+v %v", dir, res, err)
	}
	t.Logf("%s: %d files, %d bytes", dir, res.files, res.contentBytes)
}
