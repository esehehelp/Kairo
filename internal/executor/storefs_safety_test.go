package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// The marker is created, never opened: a file put where it goes (here a hard
// link to a file outside) is not written through, and the scratch is not
// made.
func TestMarkerIsNeverWrittenThroughWhatIsAlreadyThere(t *testing.T) {
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Link(outside, filepath.Join(dir, scratchMarkerName)); err != nil {
		t.Skipf("no hard links here: %v", err)
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = writeMarker(r, "att_1"); err == nil {
		t.Fatal("a marker was written over a hard link")
	}
	if body, err := os.ReadFile(outside); err != nil || string(body) != "keep" {
		t.Fatalf("the file outside was changed: %q %v", body, err)
	}
}

// A hard link in a scratch to a read-only file outside is unlinked: the file
// outside keeps its content and its mode (nothing made writable).
func TestRemoveScratchLeavesAHardLinkedFileAsItWas(t *testing.T) {
	ctx := context.Background()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(outside, 0o444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(outside, 0o644) })
	before, err := os.Stat(outside)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	dir := makeTestScratch(t, root, "att_1", 10)
	if err := os.Link(outside, filepath.Join(dir, "linked")); err != nil {
		t.Skipf("no hard links here: %v", err)
	}
	if err := (localStoreFS{}).Remove(ctx, root, dir, "att_1", false); err != nil || exists(dir) {
		t.Fatalf("remove: %v", err)
	}
	after, err := os.Stat(outside)
	if err != nil {
		t.Fatalf("the file outside was deleted: %v", err)
	}
	if after.Mode() != before.Mode() {
		t.Fatalf("the file outside changed mode: %v -> %v", before.Mode(), after.Mode())
	}
	if body, err := os.ReadFile(outside); err != nil || string(body) != "keep" {
		t.Fatalf("the file outside was changed: %q %v", body, err)
	}
}

// A path is opened one directory at a time from its volume's root: the
// directory reached is the one at the path, a missing one is made only when
// asked, and a relative path is refused.
func TestOpenDirPathWalksFromTheVolume(t *testing.T) {
	base := t.TempDir()
	want := filepath.Join(base, "a", "b")
	if _, err := openDirPath(want, false); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a missing path opened: %v", err)
	}
	r, err := openDirPath(want, true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	opened, err := r.Stat(".")
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(want); err != nil || !os.SameFile(fi, opened) {
		t.Fatalf("opened another directory than %s: %v", want, err)
	}
	if _, err := openDirPath(filepath.Join("a", "b"), true); !errors.Is(err, errOutsideRoot) {
		t.Fatalf("a relative path: %v", err)
	}
	if err := removeScratch(context.Background(), filepath.Join("a", "b"), filepath.Join("a", "b", scratchDirName, "att_1"), "att_1", false); !errors.Is(err, errOutsideRoot) {
		t.Fatalf("removal under a relative root: %v", err)
	}
}

// A scratch directory its task made unwritable is deleted all the same, as
// are its unwritable subdirectories (Linux: on Windows a mode is only the
// read-only attribute, which a deletion ignores).
func TestRemoveScratchThatItsTaskLockedUp(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no directory modes here")
	}
	root := t.TempDir()
	dir := makeTestScratch(t, root, "att_1", 10)
	sub := filepath.Join(dir, "sub")
	if err := os.MkdirAll(filepath.Join(sub, "deeper"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "deeper", "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{filepath.Join(sub, "deeper"), sub, dir} {
		if err := os.Chmod(d, 0o500); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, d := range []string{dir, sub, filepath.Join(sub, "deeper")} {
			_ = os.Chmod(d, 0o755)
		}
	})
	if err := (localStoreFS{}).Remove(context.Background(), root, dir, "att_1", false); err != nil || exists(dir) {
		t.Fatalf("a locked-up scratch: %v", err)
	}
}

// List tells a directory whose marker cannot be read now (not known to be
// Kairo's, nor to be gone) from one that is not Kairo's.
func TestListTellsUnreadFromNotKairos(t *testing.T) {
	if runtime.GOOS == "windows" || os.Getuid() == 0 {
		t.Skip("a mode does not stop reading here")
	}
	root := t.TempDir()
	makeTestScratch(t, root, "att_1", 10)
	locked := makeTestScratch(t, root, "att_2", 10)
	if err := os.WriteFile(filepath.Join(root, scratchDirName, "att_3"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	ids, unread, err := localStoreFS{}.List(context.Background(), root)
	if err != nil || len(ids) != 1 || ids[0] != "att_1" || len(unread) != 1 || unread[0] != "att_2" {
		t.Fatalf("list: %v %v %v", ids, unread, err)
	}
}

// A root with a link on its path (here a symbolic link as one of its
// directories) is not made, used, listed or deleted in.
func TestARootWithALinkOnItsPathIsRefused(t *testing.T) {
	ctx := context.Background()
	fs := localStoreFS{}
	outside, files := outsideTree(t)
	makeTestScratch(t, outside, "att_1", 10)
	base := t.TempDir()
	symlinkOrSkip(t, outside, filepath.Join(base, "linked"))
	root := filepath.Join(base, "linked")
	if err := fs.Ensure(ctx, root, testFilesystem(base)); !errors.Is(err, errScratchLink) {
		t.Fatalf("ensure: %v", err)
	}
	if err := fs.Ensure(ctx, filepath.Join(root, "store"), testFilesystem(base)); !errors.Is(err, errScratchLink) {
		t.Fatalf("ensure below the link: %v", err)
	}
	if _, _, err := fs.List(ctx, root); !errors.Is(err, errScratchLink) {
		t.Fatalf("list: %v", err)
	}
	if _, err := fs.Create(ctx, root, "att_2"); !errors.Is(err, errScratchLink) {
		t.Fatalf("create: %v", err)
	}
	dir := filepath.Join(root, scratchDirName, "att_1")
	if err := fs.Remove(ctx, root, dir, "att_1", false); !errors.Is(err, errScratchLink) {
		t.Fatalf("remove: %v", err)
	}
	if !exists(filepath.Join(outside, scratchDirName, "att_1", "data")) {
		t.Fatal("a scratch reached through a linked root was deleted")
	}
	checkKept(t, files)
}
