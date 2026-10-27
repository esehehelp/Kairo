//go:build !windows && !linux

package bench

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// Other systems run the workloads without device identity, pinning or
// cache bypass.

func cpuModel() string { return "" }

func osVersion() string { return "" }

func memory() (total, available int64, err error) { return 0, 0, errors.ErrUnsupported }

func pinThread(int) error { return errors.ErrUnsupported }

func diskFree(dir string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(uint64(st.Bavail) * uint64(st.Bsize)), nil
}

func volumeInfo(string) map[string]any { return map[string]any{} }

type testFile struct{ f *os.File }

func openTestFile(path string, create, _ bool) (*testFile, bool, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE | os.O_EXCL
	}
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, false, err
	}
	return &testFile{f}, false, nil
}

func (f *testFile) ReadAt(p []byte, off int64) (int, error)  { return f.f.ReadAt(p, off) }
func (f *testFile) WriteAt(p []byte, off int64) (int, error) { return f.f.WriteAt(p, off) }
func (f *testFile) sync() error                              { return f.f.Sync() }
func (f *testFile) close() error                             { return f.f.Close() }
func (f *testFile) dropDirect() error                        { return errors.ErrUnsupported }
func isDirectIOError(error) bool                             { return false }
func earlyUnlink(path string) bool                           { return os.Remove(path) == nil }
func linkSpeed(int, string) map[string]any                   { return nil }
