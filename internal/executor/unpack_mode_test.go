//go:build !windows

package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// A data object's file packed executable stays executable where it is
// unpacked (an input brought to another node, a continuation's output moved),
// for whoever the attempt runs as; the rest stays as written. Only a code
// snapshot takes a "#!" file packed without execute bits for executable.
func TestAnUnpackedFileKeepsItsExecuteBits(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	fs := localStoreFS{}
	dir, err := fs.CreateKind(ctx, root, kindOutput, "exe_1")
	if err != nil {
		t.Fatal(err)
	}
	for name, mode := range map[string]os.FileMode{"bin/tool": 0o700, "data.txt": 0o644, "run.sh": 0o644} {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
	}
	res, err := fs.Pack(ctx, root, kindOutput, "exe_1", "obj_1")
	if err != nil {
		t.Fatal(err)
	}
	packs, err := os.OpenRoot(filepath.Dir(res.packPath))
	if err != nil {
		t.Fatal(err)
	}
	defer packs.Close()
	for _, tc := range []struct {
		execBits bool
		want     map[string]os.FileMode
	}{
		{false, map[string]os.FileMode{"bin/tool": 0o755, "data.txt": 0o644, "run.sh": 0o644}},
		{true, map[string]os.FileMode{"bin/tool": 0o755, "data.txt": 0o755, "run.sh": 0o755}},
	} {
		dest := t.TempDir()
		into, err := os.OpenRoot(dest)
		if err != nil {
			t.Fatal(err)
		}
		var skipped []string
		if _, err := unpackChecked(ctx, packs, filepath.Base(res.packPath), filepath.Base(res.manifestPath), into, tc.execBits, &skipped); err != nil {
			t.Fatal(err)
		}
		into.Close()
		for name, want := range tc.want {
			fi, err := os.Stat(filepath.Join(dest, filepath.FromSlash(name)))
			if err != nil {
				t.Fatal(err)
			}
			if got := fi.Mode().Perm(); got != want {
				t.Errorf("execBits %v: %s is %o, want %o", tc.execBits, name, got, want)
			}
		}
	}
}
