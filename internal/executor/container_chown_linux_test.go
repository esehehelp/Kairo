package executor

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// chownTree hands the container what an id outside its map owns, leaves
// what one of its ids owns, and follows no link out of the tree.
func TestChownTreeHandsOverOnlyWhatIsNotTheContainers(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to change owners")
	}
	base := t.TempDir()
	root, outside := filepath.Join(base, "cache"), filepath.Join(base, "outside")
	for _, d := range []string{filepath.Join(root, "a", "b"), outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(p string, uid int) {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(p, uid, uid); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "a", "host"), 0)
	write(filepath.Join(root, "a", "b", "mine"), 1500) // a user of the container's
	write(filepath.Join(outside, "secret"), 0)
	if err := os.Symlink(outside, filepath.Join(root, "a", "link")); err != nil {
		t.Fatal(err)
	}
	// a map the test's own namespace has ids for
	m := idMap{uid: 1000, gid: 1000, uidLen: 1000, gidLen: 1000}
	if err := chownTree(root, m); err != nil {
		t.Fatal(err)
	}
	owner := func(p string) uint32 {
		var st syscall.Stat_t
		if err := syscall.Lstat(p, &st); err != nil {
			t.Fatal(err)
		}
		return st.Uid
	}
	for p, want := range map[string]uint32{
		root: 1000, filepath.Join(root, "a"): 1000, filepath.Join(root, "a", "host"): 1000,
		filepath.Join(root, "a", "b", "mine"): 1500, filepath.Join(root, "a", "link"): 1000,
		outside: 0, filepath.Join(outside, "secret"): 0,
	} {
		if got := owner(p); got != want {
			t.Errorf("%s: uid %d, want %d", p, got, want)
		}
	}
	if err := chownTree(filepath.Join(base, "missing"), m); err != nil {
		t.Fatalf("a missing tree: %v", err)
	}
}
