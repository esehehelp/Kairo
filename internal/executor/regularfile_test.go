package executor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// What an attempt can write is read only as the regular file it is, and
// only so much of it: a link it put there (to /dev/zero, to another's file)
// is not followed, nor is a file bigger than the limit read whole. Its
// exit is written to a file made new and renamed over the exit, so a link
// the attempt left at either name is replaced, not written through.
func TestWhatAnAttemptCanWriteIsReadAsTheFileItIs(t *testing.T) {
	dir := t.TempDir()
	exit := filepath.Join(dir, "exit")
	if code := writeShimExit(exit, 3); code != 3 {
		t.Fatalf("returned %d", code)
	}
	if code, ok := readExitFile(exit); !ok || code != 3 {
		t.Fatalf("read back %d %v", code, ok)
	}
	if err := os.WriteFile(exit, []byte(`{"exit_code":0}`+strings.Repeat(" ", exitLimit)), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := readExitFile(exit); ok {
		t.Fatal("an exit over the limit was read")
	}

	elsewhere := filepath.Join(dir, "elsewhere")
	if err := os.WriteFile(elsewhere, []byte(`{"exit_code":0}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(exit); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, exit); err != nil {
		t.Skipf("no symbolic links here: %v", err)
	}
	if _, ok := readExitFile(exit); ok {
		t.Fatal("a link was followed")
	}
	if err := os.Symlink(elsewhere, exit+".tmp"); err != nil {
		t.Fatal(err)
	}
	writeShimExit(exit, 7)
	if body, _ := os.ReadFile(elsewhere); string(body) != `{"exit_code":0}` {
		t.Fatalf("written through a link: %s", body)
	}
	if code, ok := readExitFile(exit); !ok || code != 7 {
		t.Fatalf("the exit after links at both names: %d %v", code, ok)
	}
}
