package executor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A durable store's listing names each copy with its file list beside it
// (partial files and a copy without its list left out); a description is read
// only for the copy it describes; a root not there is not an empty store.
func TestADurableStoreIsListedAndItsDescriptionsRead(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "_kairo-durable")
	month := filepath.Join(root, "objects", "202611")
	if err := os.MkdirAll(month, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(month, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("obj_a.tar.zst", "pack")
	write("obj_a.tar.zst.files.tsv", "list")
	write("obj_a.tar.zst.object.json", `{"format":"kairo object 1","object_id":"obj_a","kind":"output","lifecycle":"durable","pack_bytes":4}`)
	write("obj_b.tar.zst", "pack without its list")
	write("obj_c.tar.zst.partial", "being written")
	write("obj_d.tar.zst", "pack")
	write("obj_d.tar.zst.files.tsv", "list")
	write("obj_d.tar.zst.object.json", `{"format":"kairo object 1","object_id":"obj_x"}`)
	if err := os.MkdirAll(filepath.Join(root, "objects", "notamonth"), 0o755); err != nil {
		t.Fatal(err)
	}
	copies, err := listDurableCopies(ctx, root)
	if err != nil || len(copies) != 2 || copies["obj_a"] != "202611" || copies["obj_d"] != "202611" {
		t.Fatalf("listing %v %v", copies, err)
	}
	d, err := readDescription(root, "202611", "obj_a")
	if err != nil || d.ObjectID != "obj_a" || d.Kind != "output" || d.PackBytes != 4 {
		t.Fatalf("description %+v %v", d, err)
	}
	if _, err = readDescription(root, "202611", "obj_d"); err == nil || !strings.Contains(err.Error(), "describes obj_x") {
		t.Fatalf("a description of another object: %v", err)
	}
	if _, err = readDescription(root, "202611", "obj_b"); err == nil {
		t.Fatal("a copy without a description was described")
	}
	if _, err = listDurableCopies(ctx, filepath.Join(t.TempDir(), "unmounted")); err == nil {
		t.Fatal("a root not there was listed as an empty store")
	}
	empty := filepath.Join(t.TempDir(), "new")
	if err = os.MkdirAll(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	if copies, err = listDurableCopies(ctx, empty); err != nil || len(copies) != 0 {
		t.Fatalf("a store with no objects yet: %v %v", copies, err)
	}
}

// A shed copy's files go: the pack, its file list and its description;
// nothing else of the month's directory.
func TestAShedCopysFilesAreRemoved(t *testing.T) {
	root := filepath.Join(t.TempDir(), "_kairo-durable")
	month := filepath.Join(root, "objects", "202611")
	if err := os.MkdirAll(month, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"obj_a.tar.zst", "obj_a.tar.zst.files.tsv", "obj_a.tar.zst.object.json", "obj_b.tar.zst"} {
		if err := os.WriteFile(filepath.Join(month, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeObjectCopy(root, "202611", "obj_a"); err != nil {
		t.Fatal(err)
	}
	if err := removeObjectCopy(root, "202611", "obj_a"); err != nil {
		t.Fatalf("removed again: %v", err)
	}
	entries, _ := os.ReadDir(month)
	if len(entries) != 1 || entries[0].Name() != "obj_b.tar.zst" {
		t.Fatalf("left %v", entries)
	}
	if err := removeObjectCopy(root, "../x", "obj_a"); err == nil {
		t.Fatal("a month outside the objects was taken")
	}
}
