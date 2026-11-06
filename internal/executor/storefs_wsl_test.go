//go:build windows

package executor

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// The WSL store script in the default distro (host-specific: KAIRO_TEST_WSL=1
// runs it). A root spelled with "." and repeated slashes is that root; a
// scratch is made, listed, sized and removed; one that cannot be read is
// listed unread, not dropped; a root with a symlink on its path is refused
// before anything is made through it; a relative root is refused.
func TestWSLStoreScript(t *testing.T) {
	if os.Getenv("KAIRO_TEST_WSL") != "1" {
		t.Skip("set KAIRO_TEST_WSL=1 to run against WSL")
	}
	ctx := context.Background()
	w := wslStoreFS{}
	base := "/tmp/kairo-store-test-" + strconv.Itoa(os.Getpid())
	sh := func(script string) (string, error) {
		out, err := exec.Command("wsl.exe", "--exec", "sh", "-c", script).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	t.Cleanup(func() { _, _ = sh("chmod -R u+rwx " + base + " 2>/dev/null; rm -rf " + base) })
	if out, err := sh("mkdir -p " + base + "/real && ln -s " + base + "/real " + base + "/link"); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	root := base + "/./r//"
	if err := w.Ensure(ctx, root, "D:"); err != nil {
		t.Fatalf("ensure %s: %v", root, err)
	}
	dir, err := w.Create(ctx, root, "att_1")
	if err != nil || dir != base+"/r/scratch/att_1" {
		t.Fatalf("create: %q %v", dir, err)
	}
	if out, err := sh("printf 12345 > " + dir + "/data"); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if ids, unread, err := w.List(ctx, root); err != nil || !reflect.DeepEqual(ids, []string{"att_1"}) || len(unread) != 0 {
		t.Fatalf("list: %v %v %v", ids, unread, err)
	}
	if size, err := w.Size(ctx, dir); err != nil || size != 5+int64(len("att_1\n")) {
		t.Fatalf("size: %d %v", size, err)
	}
	if uid, _ := sh("id -u"); uid != "0" {
		locked, err := w.Create(ctx, root, "att_2")
		if err != nil {
			t.Fatal(err)
		}
		if out, err := sh("chmod 000 " + locked); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if ids, unread, err := w.List(ctx, root); err != nil || !reflect.DeepEqual(ids, []string{"att_1"}) || !reflect.DeepEqual(unread, []string{"att_2"}) {
			t.Fatalf("list with an unreadable scratch: %v %v %v", ids, unread, err)
		}
		if out, err := sh("chmod 755 " + locked); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
	}
	// a run's output directory: made (found again), listed apart from
	// scratch, never removed as a scratch or as another run's
	out, err := w.CreateKind(ctx, root, kindOutput, "exe_1")
	if err != nil || out != base+"/r/outputs/exe_1" {
		t.Fatalf("create output: %q %v", out, err)
	}
	if o, err := sh("printf ckpt > " + out + "/step.pt"); err != nil {
		t.Fatalf("%v: %s", err, o)
	}
	if again, err := w.CreateKind(ctx, root, kindOutput, "exe_1"); err != nil || again != out {
		t.Fatalf("create output again: %q %v", again, err)
	}
	if runs, _, err := w.ListKind(ctx, root, kindOutput); err != nil || !reflect.DeepEqual(runs, []string{"exe_1"}) {
		t.Fatalf("list outputs: %v %v", runs, err)
	}
	if err := w.Remove(ctx, root, out, "exe_1", false); !errors.Is(err, errOutsideRoot) {
		t.Fatalf("an output removed as a scratch: %v", err)
	}
	if err := w.RemoveKind(ctx, root, kindOutput, out, "exe_2", false); !errors.Is(err, errOutsideRoot) {
		t.Fatalf("an output removed as another run's: %v", err)
	}
	if _, err := sh("test -e " + out + "/step.pt"); err != nil {
		t.Fatal("the output directory was touched")
	}
	if err := w.RemoveKind(ctx, root, kindOutput, out, "exe_1", false); err != nil {
		t.Fatalf("remove output: %v", err)
	}

	if err := w.Remove(ctx, root, dir, "att_1", false); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := sh("test -e " + dir); err == nil {
		t.Fatal("scratch not removed")
	}

	if err := w.Ensure(ctx, base+"/link/sub", "D:"); !errors.Is(err, errScratchLink) {
		t.Fatalf("a root through a symlink: %v", err)
	}
	if _, err := sh("test -e " + base + "/real/sub"); err == nil {
		t.Fatal("a directory was made through the symlink")
	}
	if err := w.Remove(ctx, "tmp/r", "tmp/r/scratch/att_1", "att_1", false); !errors.Is(err, errOutsideRoot) {
		t.Fatalf("a relative root: %v", err)
	}
}
