package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// An imported regular file (a published checkpoint) is packed alone, by its
// name; a path gone from a volume that is there is errSourceGone.
func TestAnImportedFileIsPackedAloneAndAGonePathIsSaidSo(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	root, data := filepath.Join(base, "volatile"), filepath.Join(base, "ckpt")
	for _, d := range []string{root, data} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	file := filepath.Join(data, "step_200.pt")
	if err := os.WriteFile(file, []byte("weights"), 0o644); err != nil {
		t.Fatal(err)
	}
	// a neighbour that is not part of the checkpoint
	if err := os.WriteFile(filepath.Join(data, "step_100.pt"), []byte("older"), 0o644); err != nil {
		t.Fatal(err)
	}
	if size, err := (localStoreFS{}).Size(ctx, file); err != nil || size != int64(len("weights")) {
		t.Fatalf("size of the file: %d %v", size, err)
	}
	res, err := (localStoreFS{}).PackDir(ctx, root, file, "obj_f")
	if err != nil {
		t.Fatal(err)
	}
	if res.files != 1 || res.contentBytes != int64(len("weights")) || !strings.Contains(res.listBody, "step_200.pt") || strings.Contains(res.listBody, "step_100.pt") {
		t.Fatalf("pack of one file: %+v", res)
	}
	for _, gone := range []string{filepath.Join(data, "step_300.pt"), filepath.Join(base, "nowhere", "step_1.pt"), filepath.Join(base, "nowhere")} {
		if _, err = (localStoreFS{}).PackDir(ctx, root, gone, "obj_g"); !errors.Is(err, errSourceGone) {
			t.Fatalf("%s: %v, want the source gone", gone, err)
		}
	}
	if exists(filepath.Join(root, packsDirName, "obj_g"+packSuffix)) {
		t.Fatal("a pack was left of a gone path")
	}
}
