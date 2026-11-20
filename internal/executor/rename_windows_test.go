//go:build windows

package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

// A directory whose file something holds open without FILE_SHARE_DELETE (a
// scanner looking at a file just written) cannot be renamed until it lets
// go: renameSettled tries again until then (issue #2).
func TestRenameSettledWaitsForAHeldFile(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "tree.partial"), 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "tree.partial", "a.txt")
	if err := os.WriteFile(file, []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	name, err := windows.UTF16PtrFromString(file)
	if err != nil {
		t.Fatal(err)
	}
	held, err := windows.CreateFile(name, windows.GENERIC_READ, windows.FILE_SHARE_READ, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.OpenRoot(root)
	if err != nil {
		windows.CloseHandle(held)
		t.Fatal(err)
	}
	defer dir.Close()
	if err := dir.Rename("tree.partial", "tree"); err == nil || !renameTransient(err) {
		windows.CloseHandle(held)
		t.Fatalf("a held tree was renamed, or failed otherwise: %v", err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		windows.CloseHandle(held)
	}()
	if err := renameSettled(context.Background(), dir, "tree.partial", "tree", nil); err != nil {
		t.Fatalf("not renamed once let go: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "tree", "a.txt")); err != nil {
		t.Fatal(err)
	}
}
