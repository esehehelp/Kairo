//go:build !windows

package executor

import (
	"bufio"
	"context"
	"errors"
	"reflect"
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

// A scratch with a mount point under it is not deleted (os.Root would cross
// it, a bind mount of the same filesystem included).
func TestRemoveScratchRefusesAMountUnderIt(t *testing.T) {
	root := t.TempDir()
	dir := makeTestScratch(t, root, "att_1", 10)
	saved := mountsUnderFn
	t.Cleanup(func() { mountsUnderFn = saved })
	mountsUnderFn = func(string) ([]string, error) { return []string{dir + "/bound"}, nil }
	if err := (localStoreFS{}).Remove(context.Background(), root, dir, "att_1", false); !errors.Is(err, errScratchLink) || !exists(dir+"/data") {
		t.Fatalf("a scratch with a mount under it: %v", err)
	}
}
