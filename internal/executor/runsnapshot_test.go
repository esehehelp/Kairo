package executor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A running run's snapshot holds the files of its output directory changed
// since the previous one; the first, every file; the directory is left as
// it is.
func TestARunSnapshotHoldsTheFilesChangedSinceThePreviousOne(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	fs := localStoreFS{}
	dir, err := fs.CreateKind(ctx, root, kindOutput, "exe_run")
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, body string, at time.Time) {
		t.Helper()
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-3 * time.Hour)
	write("ckpt/step-100.pt", "old weights", old)
	write("metrics.jsonl", "{}\n", old)
	first, err := fs.PackSince(ctx, root, kindOutput, "exe_run", "obj_s1", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if first.files != 2 {
		t.Fatalf("first snapshot %+v", first)
	}
	previous := time.Now().Add(-time.Hour)
	write("ckpt/step-200.pt", "new weights", time.Now())
	second, err := fs.PackSince(ctx, root, kindOutput, "exe_run", "obj_s2", previous)
	if err != nil {
		t.Fatal(err)
	}
	if second.files != 1 || !strings.Contains(second.listBody, "ckpt/step-200.pt") || strings.Contains(second.listBody, "step-100") {
		t.Fatalf("second snapshot %+v", second)
	}
	for _, name := range []string{"ckpt/step-100.pt", "ckpt/step-200.pt", "metrics.jsonl"} {
		if !exists(filepath.Join(dir, filepath.FromSlash(name))) {
			t.Fatalf("%s left the run's directory", name)
		}
	}
}
