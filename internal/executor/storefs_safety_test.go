package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

// A directory opened after it was checked must be the one checked: checkOpened
// refuses another (a directory swapped meanwhile).
func TestCheckOpenedRefusesAnotherDirectory(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	fa, err := os.Lstat(a)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := os.OpenRoot(b)
	if err != nil {
		t.Fatal(err)
	}
	defer rb.Close()
	if err = checkOpened(a, fa, rb); !errors.Is(err, errScratchLink) {
		t.Fatalf("another directory taken for the one checked: %v", err)
	}
	ra, err := os.OpenRoot(a)
	if err != nil {
		t.Fatal(err)
	}
	defer ra.Close()
	if err = checkOpened(a, fa, ra); err != nil {
		t.Fatalf("the directory checked refused: %v", err)
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
	if _, err := fs.List(ctx, root); !errors.Is(err, errScratchLink) {
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
