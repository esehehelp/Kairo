package executor

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// chownTree hands the container's attempt user all of the tree: what an id
// outside its map owns, what the container's root or another of its users
// owns (written by attempts that ran as root or switched user); it follows
// no link out of the tree.
func TestChownTreeHandsTheAttemptUserAllOfTheTree(t *testing.T) {
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
	// a map the test's own namespace has ids for: the container's root is
	// 100000, its attempt user 101000
	m := idMap{uid: 100000, gid: 100000, uidLen: 65536, gidLen: 65536}
	write := func(p string, uid int) {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(p, uid, uid); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "a", "host"), 0)
	write(filepath.Join(root, "a", "root"), 100000)      // an attempt that ran as root
	write(filepath.Join(root, "a", "b", "mine"), 165534) // a user an attempt switched to (nobody)
	write(filepath.Join(outside, "secret"), 0)
	if err := os.Symlink(outside, filepath.Join(root, "a", "link")); err != nil {
		t.Fatal(err)
	}
	if err := chownTree(root, m.toAttemptUser); err != nil {
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
		root: 101000, filepath.Join(root, "a"): 101000, filepath.Join(root, "a", "host"): 101000, filepath.Join(root, "a", "root"): 101000,
		filepath.Join(root, "a", "b", "mine"): 101000, filepath.Join(root, "a", "link"): 101000,
		outside: 0, filepath.Join(outside, "secret"): 0,
	} {
		if got := owner(p); got != want {
			t.Errorf("%s: uid %d, want %d", p, got, want)
		}
	}
	if err := chownTree(filepath.Join(base, "missing"), m.toAttemptUser); err != nil {
		t.Fatalf("a missing tree: %v", err)
	}
	// a file (an attempt's log, mounted writable) is handed over itself
	log := filepath.Join(base, "att.stdout.log")
	write(log, 0)
	if err := chownTree(log, m.toAttemptUser); err != nil {
		t.Fatalf("a file: %v", err)
	}
	if got := owner(log); got != 101000 {
		t.Fatalf("the log: uid %d", got)
	}
	// in a container whose runner hands it nothing, all of it
	if err := chownTree(filepath.Join(root, "a", "b"), toAttemptUserHere); err != nil {
		t.Fatal(err)
	}
	if got := owner(filepath.Join(root, "a", "b", "mine")); got != containerUID {
		t.Fatalf("handed here: uid %d", got)
	}
}
