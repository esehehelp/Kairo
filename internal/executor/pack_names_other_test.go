//go:build !windows

package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The file list is sorted by path, not by its escaped line (a tab sorts
// before "-", its escape after); a name that is not UTF-8 is refused.
func TestPackListIsSortedByPathAndNamesAreUTF8(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "volatile")
	dir, err := (localStoreFS{}).CreateKind(ctx, root, kindOutput, "exe_1")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a-b", "a\tb"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	res, err := (localStoreFS{}).Pack(ctx, root, kindOutput, "exe_1", "obj_1")
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(res.manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[1], `a\tb`+"\t") || !strings.HasPrefix(lines[2], "a-b\t") {
		t.Fatalf("file list:\n%s", body)
	}
	listed, err := readPackList(root, "obj_1"+packSuffix+manifestSuffix)
	if err != nil || len(listed) != 2 || listed["a\tb"].size != 3 {
		t.Fatalf("read back: %+v %v", listed, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bad\xff"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := (localStoreFS{}).Pack(ctx, root, kindOutput, "exe_1", "obj_2"); !errors.Is(err, errPackName) {
		t.Fatalf("a name that is not UTF-8: %v", err)
	}
}

// A declared filesystem must be a mount point where /proc tells.
func TestTheDeclaredFilesystemIsAMountPoint(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("mountinfo is Linux's")
	}
	if mounted, known, err := isMountPoint("/"); err != nil || !known || !mounted {
		t.Fatalf("/: %v %v %v", mounted, known, err)
	}
	dir := t.TempDir()
	if mounted, known, err := isMountPoint(dir); err != nil || !known || mounted {
		t.Fatalf("%s: %v %v %v", dir, mounted, known, err)
	}
	if on, err := onFilesystem(dir, dir); on || err == nil || !strings.Contains(err.Error(), "is not a mount point") {
		t.Fatalf("a plain directory as the filesystem: %v %v", on, err)
	}
}
