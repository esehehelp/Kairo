//go:build !windows

package executor

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// The mount points at or under a scratch directory are read from mountinfo
// (bind mounts included, escaped names undone), and nothing else is.
func TestMountsUnderAScratchAreFound(t *testing.T) {
	info := `22 1 8:1 / / rw,relatime - ext4 /dev/sda1 rw
40 22 8:1 /data /srv/kairo/scratch/att_1/bound rw,relatime - ext4 /dev/sda1 rw
41 22 0:5 / /srv/kairo/scratch/att_1/with\040space rw - tmpfs tmpfs rw
42 22 0:6 / /srv/kairo/scratch/att_10 rw - tmpfs tmpfs rw
43 22 0:7 / /srv/kairo/scratch/att_1 rw - tmpfs tmpfs rw
`
	got, err := parseMountsUnder(bufio.NewScanner(strings.NewReader(info)), "/srv/kairo/scratch/att_1")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/srv/kairo/scratch/att_1/bound", "/srv/kairo/scratch/att_1/with space", "/srv/kairo/scratch/att_1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mounts under the scratch: %q, want %q", got, want)
	}
	if got, _ := parseMountsUnder(bufio.NewScanner(strings.NewReader(info)), "/srv/kairo/scratch/att_2"); len(got) != 0 {
		t.Fatalf("mounts under another scratch: %q", got)
	}
}

// A path's place is the device and the path within its filesystem of the
// mount covering it, bind mounts undone; places overlap when one holds the
// other on one device.
func TestPlaceUndoesBindMounts(t *testing.T) {
	info := `22 1 8:1 / / rw - ext4 /dev/sda1 rw
30 22 8:2 / /data rw - ext4 /dev/sdb1 rw
31 22 8:2 /d /data/v rw - ext4 /dev/sdb1 rw
32 31 0:9 / /data/v/tmp rw - tmpfs tmpfs rw
33 22 8:11 / /srv rw - ext4 /dev/sdc1 rw
`
	for p, want := range map[string]string{
		"/":             "8:1:/",
		"/var/k":        "8:1:/var/k",
		"/data/d/x":     "8:2:/d/x",
		"/data/v":       "8:2:/d",
		"/data/v/sub":   "8:2:/d/sub",
		"/data/v/tmp/a": "0:9:/a",
		"/srv/k":        "8:11:/k",
		"/srvx":         "8:1:/srvx",
	} {
		if got, err := parsePlace(bufio.NewScanner(strings.NewReader(info)), p); err != nil || got != want {
			t.Fatalf("place of %s: %q %v, want %q", p, got, err, want)
		}
	}
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"8:2:/d", "8:2:/d", true},
		{"8:2:/d", "8:2:/d/x", true},
		{"8:2:/d/x", "8:2:/d", true},
		{"8:2:/d", "8:2:/dx", false},
		{"8:1:/", "8:1:/x", true},
		{"8:1:/", "8:11:/x", false},
		{"", "8:1:/", false},
	} {
		if got := placesOverlap(c.a, c.b); got != c.want {
			t.Fatalf("overlap %q %q: %v", c.a, c.b, got)
		}
	}
}

// chmodDir changes only a directory: a file in its place (a hard link to
// one outside, say) is refused and keeps its mode.
func TestChmodDirChangesOnlyADirectory(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux only")
	}
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "d"), 0o500); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f"), nil, 0o400); err != nil {
		t.Fatal(err)
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = chmodDir(r, "d", 0o700); err != nil {
		t.Fatal(err)
	}
	if err = chmodDir(r, "f", 0o700); err == nil {
		t.Fatal("a file's mode changed")
	}
	for name, want := range map[string]os.FileMode{"d": 0o700, "f": 0o400} {
		if fi, err := os.Lstat(filepath.Join(dir, name)); err != nil || fi.Mode().Perm() != want {
			t.Fatalf("%s: %v %v, want %v", name, fi.Mode(), err, want)
		}
	}
}

// A scratch with a mount point under it is not deleted (os.Root would cross
// it, a bind mount of the same filesystem included), nor one whose scratch
// parent is a mount point; nothing is made under such a parent.
func TestRemoveScratchRefusesAMountUnderIt(t *testing.T) {
	root := t.TempDir()
	dir := makeTestScratch(t, root, "att_1", 10)
	saved := mountsUnderFn
	t.Cleanup(func() { mountsUnderFn = saved })
	mountsUnderFn = func(string) ([]string, error) { return []string{dir + "/bound"}, nil }
	if err := (localStoreFS{}).Remove(context.Background(), root, dir, "att_1", false); !errors.Is(err, errScratchLink) || !exists(dir+"/data") {
		t.Fatalf("a scratch with a mount under it: %v", err)
	}
	parent := filepath.Join(root, scratchDirName)
	mountsUnderFn = func(string) ([]string, error) { return []string{parent}, nil }
	if err := (localStoreFS{}).Remove(context.Background(), root, dir, "att_1", false); !errors.Is(err, errScratchLink) || !exists(dir+"/data") {
		t.Fatalf("a scratch under a mounted parent: %v", err)
	}
	if _, err := (localStoreFS{}).Create(context.Background(), root, "att_2"); !errors.Is(err, errScratchLink) || exists(filepath.Join(parent, "att_2")) {
		t.Fatalf("made under a mounted parent: %v", err)
	}
}
