//go:build windows

package secfile

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestRestrictLeavesOnlyTheUserAndSystem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("s3cret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Restrict(path); err != nil {
		t.Fatal(err)
	}
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	control, _, err := sd.Control()
	if err != nil {
		t.Fatal(err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		t.Fatal("DACL still inherits from the directory")
	}
	acl, _, err := sd.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if acl.AceCount != 2 {
		t.Fatalf("DACL has %d entries, want the user and SYSTEM", acl.AceCount)
	}
	if body, err := os.ReadFile(path); err != nil || string(body) != "s3cret" {
		t.Fatalf("owner can no longer read it: %q %v", body, err)
	}
	if err := RestrictExisting(path, filepath.Join(t.TempDir(), "missing")); err != nil {
		t.Fatal(err)
	}
}
