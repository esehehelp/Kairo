//go:build !windows

package executor

import (
	"net"
	"path/filepath"
	"syscall"
	"testing"

	"kairo/internal/store"
)

// A socket or a pipe holds no data: left out of the pack, it keeps no source
// from being deleted once copied (a database's socket in a failed test's
// scratch, say).
func TestASocketOrPipeKeepsNoSourceFromBeingDeleted(t *testing.T) {
	settled, dir := retireWith(t, func(t *testing.T, dir string) {
		t.Helper()
		if err := syscall.Mkfifo(filepath.Join(dir, "pipe"), 0o600); err != nil {
			t.Fatal(err)
		}
		// a socket path must be short: listen from the directory
		l, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(dir, ".s.PGSQL.5432"), Net: "unix"})
		if err != nil {
			t.Skipf("no unix socket here: %v", err)
		}
		l.SetUnlinkOnClose(false)
		t.Cleanup(func() { l.Close() })
	}, func(*testing.T, string) {})
	if len(settled) != 2 || settled[1].State != store.SourceDeleted || exists(dir) {
		t.Fatalf("settled %+v (source there: %v)", settled, exists(dir))
	}
}
