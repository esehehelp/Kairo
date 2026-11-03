//go:build windows

package executor

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// mklinkJunction makes link a junction to target (no privilege needed, as
// any user can).
func mklinkJunction(t *testing.T, link, target string) {
	t.Helper()
	if out, err := exec.Command("cmd", "/c", "mklink", "/J", link, target).CombinedOutput(); err != nil {
		t.Fatalf("mklink /J %s %s: %v: %s", link, target, err, out)
	}
	fi, err := os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	if !isLink(fi) {
		t.Fatalf("junction %s not seen as a link (mode %v)", link, fi.Mode())
	}
}

// A junction in a scratch (at any depth) is removed itself, never what it
// points at, and is not counted; a scratch directory or scratch parent that
// is a junction is not removed, made in or scanned.
func TestRemoveScratchNeverFollowsAJunction(t *testing.T) {
	ctx := context.Background()
	fs := localStoreFS{}
	outside, files := outsideTree(t)
	root := t.TempDir()
	dir := makeTestScratch(t, root, "att_1", 10)
	mklinkJunction(t, filepath.Join(dir, "junction"), outside)
	if err := os.MkdirAll(filepath.Join(dir, "a", "b"), 0o755); err != nil {
		t.Fatal(err)
	}
	mklinkJunction(t, filepath.Join(dir, "a", "b", "junction"), outside)
	if size, err := fs.Size(ctx, dir); err != nil || size != 10+int64(len("att_1\n")) {
		t.Fatalf("size through junctions: %d %v", size, err)
	}
	if sizes, err := fs.Scan(ctx, root); err != nil || sizes["att_1"] != 10+int64(len("att_1\n")) {
		t.Fatalf("scan through junctions: %v %v", sizes, err)
	}
	if err := fs.Remove(ctx, root, dir, "att_1", false); err != nil || exists(dir) {
		t.Fatalf("remove: %v", err)
	}
	checkKept(t, files)

	// the scratch directory itself a junction to a directory with a marker
	if err := os.WriteFile(filepath.Join(outside, scratchMarkerName), []byte("att_2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, scratchDirName, "att_2")
	mklinkJunction(t, link, outside)
	if err := fs.Remove(ctx, root, link, "att_2", false); !errors.Is(err, errScratchLink) || !exists(link) {
		t.Fatalf("a junction as scratch: %v", err)
	}
	if err := fs.Remove(ctx, root, link, "att_2", true); !errors.Is(err, errScratchLink) {
		t.Fatalf("a junction as scratch, only if empty: %v", err)
	}
	if sizes, err := fs.Scan(ctx, root); err != nil || len(sizes) != 0 {
		t.Fatalf("a junction scanned as scratch: %v %v", sizes, err)
	}
	checkKept(t, files)

	// the scratch parent a junction
	elsewhere := t.TempDir()
	makeTestScratch(t, elsewhere, "att_3", 10)
	linked := t.TempDir()
	mklinkJunction(t, filepath.Join(linked, scratchDirName), filepath.Join(elsewhere, scratchDirName))
	if err := fs.Remove(ctx, linked, filepath.Join(linked, scratchDirName, "att_3"), "att_3", false); !errors.Is(err, errScratchLink) || !exists(filepath.Join(elsewhere, scratchDirName, "att_3", "data")) {
		t.Fatalf("a junction as scratch parent: %v", err)
	}
	if _, err := fs.Create(ctx, linked, "att_4"); !errors.Is(err, errScratchLink) {
		t.Fatalf("made under a junction: %v", err)
	}
	if _, err := fs.Scan(ctx, linked); !errors.Is(err, errScratchLink) {
		t.Fatalf("scanned through a junction: %v", err)
	}
}
