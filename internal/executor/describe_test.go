package executor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// A copy's description is written beside it, readable without the daemon;
// nothing is described where the copy is not.
func TestACopyIsDescribedBesideItself(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, objectsDirName, "202611")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "obj_a"+packSuffix), []byte("pack"), 0o644); err != nil {
		t.Fatal(err)
	}
	name, project := "local/hand", "neo-ime-dict"
	s := servedStore{decl: machine.Store{ID: "e", Class: machine.StoreDurable, Root: root}, fs: localStoreFS{}, usable: true}
	o := store.CopyOrder{ObjectID: "obj_a", StoreID: "e", Root: root, Month: "202611",
		Description: &store.ObjectDescription{Format: store.DescriptionFormat, ObjectID: "obj_a", Kind: "import", Project: &project, Name: &name}}
	if err := describeCopy(s, o); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(dir, "obj_a"+packSuffix+descriptionSuffix))
	if err != nil {
		t.Fatal(err)
	}
	var got store.ObjectDescription
	if err = json.Unmarshal(body, &got); err != nil || got.ObjectID != "obj_a" || got.Name == nil || *got.Name != name || got.Project == nil || *got.Project != project {
		t.Fatalf("description %s %v", body, err)
	}
	if _, err = os.Stat(filepath.Join(dir, "obj_a"+packSuffix+descriptionSuffix+partialSuffix)); !os.IsNotExist(err) {
		t.Fatalf("partial left: %v", err)
	}
	o.ObjectID = "obj_b"
	if err = describeCopy(s, o); err == nil {
		t.Fatal("described a copy that is not there")
	}
}
