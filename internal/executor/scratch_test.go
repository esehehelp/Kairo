package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// Test attempts that write a file into their KAIRO_SCRATCH, then exit 0 or
// 1 (exit 4: they were given no scratch).
const (
	helperScratchOK   = "__test-scratch-ok"
	helperScratchFail = "__test-scratch-fail"
)

func runScratchHelper(args []string) {
	if len(args) != 2 || args[1] != helperScratchOK && args[1] != helperScratchFail {
		return
	}
	dir := os.Getenv("KAIRO_SCRATCH")
	if dir == "" {
		os.Exit(4)
	}
	if err := os.WriteFile(filepath.Join(dir, "out"), []byte("scratch"), 0o644); err != nil {
		os.Exit(3)
	}
	if args[1] == helperScratchFail {
		os.Exit(1)
	}
	os.Exit(0)
}

// makeTestScratch makes the scratch of attemptID under root, with a file
// data of size bytes unless size is negative.
func makeTestScratch(t *testing.T, root, attemptID string, size int) string {
	t.Helper()
	dir, err := localStoreFS{}.Create(context.Background(), root, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(root, scratchDirName, attemptID); dir != want {
		t.Fatalf("scratch at %s, not %s", dir, want)
	}
	if size >= 0 {
		if err = os.WriteFile(filepath.Join(dir, "data"), make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func exists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// Remove deletes only <root>/scratch/<attempt id> whose marker names the
// attempt; anything else is refused and left as it is.
func TestRemoveScratchRefusesWhatIsNotItsOwn(t *testing.T) {
	ctx := context.Background()
	fs := localStoreFS{}
	root := t.TempDir()
	dir := makeTestScratch(t, root, "att_1", 10)
	data := filepath.Join(dir, "data")

	for _, c := range []struct{ dir, id string }{
		{root, "att_1"},
		{filepath.Join(root, scratchDirName), "att_1"},
		{filepath.Join(dir, ".."), "att_1"},
		{dir, "att_2"},
		{filepath.Join(t.TempDir(), scratchDirName, "att_1"), "att_1"},
		{filepath.Join(root, scratchDirName, ".."), ".."},
		{filepath.Join(root, scratchDirName, "a", "b"), "a/b"},
		{"", ""},
	} {
		if err := fs.Remove(ctx, root, c.dir, c.id, false); !errors.Is(err, errOutsideRoot) {
			t.Fatalf("remove %q of %q: %v", c.dir, c.id, err)
		}
	}
	if !exists(data) {
		t.Fatal("a refused removal deleted the scratch")
	}

	// a marker naming another attempt, or none while it holds files
	if err := os.WriteFile(filepath.Join(dir, scratchMarkerName), []byte("att_9\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove(ctx, root, dir, "att_1", false); !errors.Is(err, errScratchMarker) || !exists(data) {
		t.Fatalf("a foreign marker: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, scratchMarkerName)); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove(ctx, root, dir, "att_1", false); !errors.Is(err, errScratchMarker) || !exists(data) {
		t.Fatalf("no marker: %v", err)
	}
	// an empty directory without its marker is one whose deletion stopped
	if err := os.Remove(data); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove(ctx, root, dir, "att_1", false); err != nil || exists(dir) {
		t.Fatalf("an emptied scratch: %v", err)
	}
	if err := fs.Remove(ctx, root, dir, "att_1", false); err != nil {
		t.Fatalf("a scratch already gone: %v", err)
	}

	// only empty: refused while it holds anything but its marker
	dir = makeTestScratch(t, root, "att_2", 0)
	if err := fs.Remove(ctx, root, dir, "att_2", true); !errors.Is(err, errScratchNotEmpty) || !exists(filepath.Join(dir, "data")) {
		t.Fatalf("only empty, not empty: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "data")); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove(ctx, root, dir, "att_2", true); err != nil || exists(dir) {
		t.Fatalf("only empty, empty: %v", err)
	}

	// read-only files and directories are deleted too (Windows refuses them
	// as they are)
	dir = makeTestScratch(t, root, "att_3", 5)
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "ro"), []byte("x"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(dir, "data"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove(ctx, root, dir, "att_3", false); err != nil || exists(dir) {
		t.Fatalf("read-only files: %v", err)
	}
	if !exists(filepath.Join(root, scratchDirName)) {
		t.Fatal("the scratch parent went too")
	}

	// made again for the same attempt, the scratch is taken as it is
	dir = makeTestScratch(t, root, "att_4", 7)
	if again, err := fs.Create(ctx, root, "att_4"); err != nil || again != dir || !exists(filepath.Join(dir, "data")) {
		t.Fatalf("made again: %q %v", again, err)
	}
	if err := os.WriteFile(filepath.Join(dir, scratchMarkerName), []byte("att_9\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Create(ctx, root, "att_4"); err == nil {
		t.Fatal("a directory with another attempt's marker was taken")
	}
}

// symlinkOrSkip makes a symbolic link, or skips the test where that needs a
// privilege the test does not have (Windows without developer mode).
func symlinkOrSkip(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("no symbolic links here: %v", err)
	}
}

// outsideTree is a directory outside any store, holding files that must
// survive every removal.
func outsideTree(t *testing.T) (dir string, files []string) {
	t.Helper()
	dir = t.TempDir()
	files = []string{filepath.Join(dir, "keep"), filepath.Join(dir, "sub", "keep")}
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if err := os.WriteFile(f, make([]byte, 1000), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir, files
}

func checkKept(t *testing.T, files []string) {
	t.Helper()
	for _, f := range files {
		if !exists(f) {
			t.Fatalf("%s, outside the scratch, was deleted", f)
		}
	}
}

// A symbolic link in a scratch is removed itself, never what it points at;
// a scratch directory (or scratch parent) that is a link is not removed.
func TestRemoveScratchNeverFollowsASymlink(t *testing.T) {
	ctx := context.Background()
	fs := localStoreFS{}
	outside, files := outsideTree(t)
	root := t.TempDir()
	dir := makeTestScratch(t, root, "att_1", 10)
	symlinkOrSkip(t, outside, filepath.Join(dir, "to-dir"))
	symlinkOrSkip(t, files[0], filepath.Join(dir, "to-file"))
	if err := os.Mkdir(filepath.Join(dir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	symlinkOrSkip(t, outside, filepath.Join(dir, "nested", "to-dir"))
	symlinkOrSkip(t, filepath.Join(outside, "missing"), filepath.Join(dir, "dangling"))
	if size, err := fs.Size(ctx, dir); err != nil || size != 10+int64(len("att_1\n")) {
		t.Fatalf("size through links: %d %v", size, err)
	}
	if err := fs.Remove(ctx, root, dir, "att_1", false); err != nil || exists(dir) {
		t.Fatalf("remove: %v", err)
	}
	checkKept(t, files)

	// the scratch directory itself a link to a directory holding a marker
	if err := os.WriteFile(filepath.Join(outside, scratchMarkerName), []byte("att_2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, scratchDirName, "att_2")
	symlinkOrSkip(t, outside, link)
	if err := fs.Remove(ctx, root, link, "att_2", false); !errors.Is(err, errScratchLink) || !exists(link) {
		t.Fatalf("a linked scratch: %v", err)
	}
	checkKept(t, files)

	// the scratch parent a link
	elsewhere := t.TempDir()
	makeTestScratch(t, elsewhere, "att_3", 10)
	linked := t.TempDir()
	symlinkOrSkip(t, filepath.Join(elsewhere, scratchDirName), filepath.Join(linked, scratchDirName))
	if err := fs.Remove(ctx, linked, filepath.Join(linked, scratchDirName, "att_3"), "att_3", false); !errors.Is(err, errScratchLink) || !exists(filepath.Join(elsewhere, scratchDirName, "att_3", "data")) {
		t.Fatalf("a linked scratch parent: %v", err)
	}
	if _, err := fs.Create(ctx, linked, "att_4"); !errors.Is(err, errScratchLink) {
		t.Fatalf("made under a linked parent: %v", err)
	}
	if _, err := fs.Scan(ctx, linked); !errors.Is(err, errScratchLink) {
		t.Fatalf("scanned through a linked parent: %v", err)
	}
}

// fakeScratch serves stores (fakeStores) and the scratch operations.
type fakeScratch struct {
	*fakeStores
	mu        sync.Mutex
	recordErr error
	settleErr error
	recorded  []store.AttemptScratch
	due       []store.ScratchSettlement
	settled   []store.ScratchSettled
	asked     int
}

func (f *fakeScratch) RecordAttemptScratch(_ context.Context, scratch store.AttemptScratch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recordErr != nil {
		return f.recordErr
	}
	f.recorded = append(f.recorded, scratch)
	return nil
}

func (f *fakeScratch) ScratchToSettle(context.Context, string) ([]store.ScratchSettlement, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked++
	return append([]store.ScratchSettlement(nil), f.due...), nil
}

func (f *fakeScratch) SettleScratch(_ context.Context, settled store.ScratchSettled) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.settleErr != nil {
		return f.settleErr
	}
	f.settled = append(f.settled, settled)
	return nil
}

// An attempt launched by an executor serving a usable volatile store gets
// its scratch there, recorded, and named in its environment; the temporary
// directory follows it only when the execution asks.
func TestLaunchGetsScratchOnAVolatileStore(t *testing.T) {
	root := t.TempDir()
	fake := &fakeScratch{fakeStores: &fakeStores{}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.stores = []servedStore{
		{decl: machine.Store{ID: "a-durable", Class: machine.StoreDurable, Root: filepath.Join(root, "durable")}, fs: localStoreFS{}, usable: true},
		{decl: machine.Store{ID: "b-unusable", Class: machine.StoreVolatile, Root: filepath.Join(root, "unusable")}, fs: localStoreFS{}},
		{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: filepath.Join(root, "volatile")}, fs: localStoreFS{}, usable: true},
	}
	launch := testLaunch()
	e.makeScratch(launch)
	want := filepath.Join(root, "volatile", scratchDirName, "att_1")
	if marker, err := os.ReadFile(filepath.Join(want, scratchMarkerName)); err != nil || string(marker) != "att_1\n" {
		t.Fatalf("marker: %q %v", marker, err)
	}
	if exists(filepath.Join(root, "durable")) || exists(filepath.Join(root, "unusable")) {
		t.Fatal("scratch made on a durable or unusable store")
	}
	if len(fake.recorded) != 1 || fake.recorded[0] != (store.AttemptScratch{AttemptID: "att_1", LeaseID: "lease_1", Epoch: 3, StoreID: "c-scratch", ExecutorID: "exec", Root: filepath.Join(root, "volatile"), Path: want}) {
		t.Fatalf("recorded: %+v", fake.recorded)
	}
	env := e.launchEnv(launch)
	if got, _ := envValue(env, "KAIRO_SCRATCH"); got != want {
		t.Fatalf("KAIRO_SCRATCH = %q", got)
	}
	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		if _, ok := envValue(env, name); ok {
			t.Fatalf("%s set without scratch_tmpdir", name)
		}
	}
	launch.ScratchTmpdir = true
	t.Setenv("TMP", filepath.Join(root, "parent-tmp"))
	cmd := e.command(launch)
	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		var values []string
		for _, kv := range cmd.Env {
			if k, v, _ := strings.Cut(kv, "="); strings.EqualFold(k, name) {
				values = append(values, v)
			}
		}
		if len(values) != 1 || values[0] != want {
			t.Fatalf("%s = %q with scratch_tmpdir", name, values)
		}
	}
	// A WSL attempt's TMP and TEMP would reach wsl.exe as well: only TMPDIR.
	e.Kind = "wsl2"
	env = e.launchEnv(launch)
	if got, _ := envValue(env, "TMPDIR"); got != want {
		t.Fatalf("WSL TMPDIR = %q", got)
	}
	if _, ok := envValue(env, "TMP"); ok {
		t.Fatal("WSL attempt given TMP")
	}
	e.Kind = "windows"
	e.forgetScratch("att_1")
	if _, ok := envValue(e.launchEnv(launch), "KAIRO_SCRATCH"); ok {
		t.Fatal("scratch named after it was forgotten")
	}

	// A scratch the daemon did not record is not used and goes at once.
	fake.recordErr = errors.New("refused")
	launch = testLaunch()
	launch.Attempt.ID = "att_2"
	e.makeScratch(launch)
	if exists(filepath.Join(root, "volatile", scratchDirName, "att_2")) {
		t.Fatal("unrecorded scratch left")
	}
	if _, ok := envValue(e.launchEnv(launch), "KAIRO_SCRATCH"); ok || !strings.Contains(logs.String(), "attempt starts without scratch: not recorded") {
		t.Fatalf("unrecorded scratch named:\n%s", logs.String())
	}

	// Without a volatile store there is none.
	e.stores = e.stores[:2]
	launch.Attempt.ID = "att_3"
	fake.recordErr = nil
	e.makeScratch(launch)
	if len(fake.recorded) != 1 {
		t.Fatalf("scratch without a volatile store: %+v", fake.recorded)
	}
}

// Due scratch is settled on disk as the daemon says: deleted after a
// success; after a failure kept, unless it holds nothing but its marker;
// a lost attempt's kept as it is; a deletion refused for safety keeps it.
func TestDueScratchIsSettled(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	ok := makeTestScratch(t, root, "att_ok", 100)
	failed := makeTestScratch(t, root, "att_failed", 100)
	empty := makeTestScratch(t, root, "att_empty", -1)
	lost := makeTestScratch(t, root, "att_lost", -1)
	foreign := makeTestScratch(t, root, "att_foreign", 100)
	if err := os.WriteFile(filepath.Join(foreign, scratchMarkerName), []byte("att_other\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	outside, files := outsideTree(t)
	settlement := func(id, path string, del, isLost bool, reason string) store.ScratchSettlement {
		return store.ScratchSettlement{AttemptID: id, StoreID: "scratch", Root: root, Path: path, Delete: del, Lost: isLost, Reason: reason}
	}
	fake := &fakeScratch{fakeStores: &fakeStores{}, due: []store.ScratchSettlement{
		settlement("att_ok", ok, true, false, "exit 0"),
		settlement("att_failed", failed, false, false, "exit 1"),
		settlement("att_empty", empty, false, false, "exit 1"),
		settlement("att_lost", lost, false, true, "attempt lost"),
		settlement("att_foreign", foreign, true, false, "exit 0"),
		settlement("att_outside", outside, true, false, "exit 0"),
	}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.settleDueScratch(ctx)

	if exists(ok) || exists(empty) {
		t.Fatal("a successful or empty scratch was not deleted")
	}
	if !exists(filepath.Join(failed, "data")) || !exists(lost) || !exists(filepath.Join(foreign, "data")) {
		t.Fatal("a kept scratch was deleted")
	}
	checkKept(t, files)
	got := map[string]store.ScratchSettled{}
	for _, s := range fake.settled {
		got[s.AttemptID] = s
	}
	if len(got) != 6 {
		t.Fatalf("settled: %+v", fake.settled)
	}
	failedSize := 100 + int64(len("att_failed\n"))
	lostSize := int64(len("att_lost\n"))
	for id, want := range map[string]store.ScratchSettled{
		"att_ok":     {AttemptID: "att_ok", State: "deleted"},
		"att_failed": {AttemptID: "att_failed", State: "kept", Bytes: &failedSize},
		"att_empty":  {AttemptID: "att_empty", State: "deleted", Empty: true},
		"att_lost":   {AttemptID: "att_lost", State: "kept", Bytes: &lostSize},
	} {
		s := got[id]
		if s.State != want.State || s.Empty != want.Empty || s.Reason != "" || (want.Bytes == nil) != (s.Bytes == nil) || want.Bytes != nil && *s.Bytes != *want.Bytes {
			t.Fatalf("%s settled %+v, want %+v", id, s, want)
		}
	}
	if s := got["att_foreign"]; s.State != "kept" || !strings.HasPrefix(s.Reason, "exit 0; not deleted: ") || !strings.Contains(s.Reason, "scratch marker does not name the attempt") {
		t.Fatalf("foreign marker settled %+v", s)
	}
	if s := got["att_outside"]; s.State != "kept" || !strings.Contains(s.Reason, "not a scratch directory of the store's root") {
		t.Fatalf("outside the root settled %+v", s)
	}

	// a settlement the daemon does not take is tried again, warned once
	fake.settled, fake.settleErr = nil, errors.New("daemon unreachable")
	fake.due = []store.ScratchSettlement{settlement("att_failed", failed, false, false, "exit 1")}
	e.settleDueScratch(ctx)
	e.settleDueScratch(ctx)
	if n := strings.Count(logs.String(), "scratch not settled"); n != 1 || !exists(filepath.Join(failed, "data")) {
		t.Fatalf("warned %d times:\n%s", n, logs.String())
	}
}

// With a daemon older than stores (404) the executor makes no scratch and
// never asks for scratch to settle; once the daemon answers for stores it
// does.
func TestScratchWaitsForADaemonThatKnowsStores(t *testing.T) {
	fake := &fakeScratch{fakeStores: &fakeStores{err: errors.New("agent API 404: 404 page not found")}}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.refreshStores(context.Background())
	e.settleScratch()
	e.makeScratch(testLaunch())
	fake.mu.Lock()
	asked, recorded := fake.asked, len(fake.recorded)
	fake.mu.Unlock()
	if asked != 0 || recorded != 0 || e.storesKnown {
		t.Fatalf("asked %d, recorded %d, stores known %v", asked, recorded, e.storesKnown)
	}

	fake.fakeStores.mu.Lock()
	fake.fakeStores.err = nil
	fake.fakeStores.mu.Unlock()
	e.storesAt = e.storesAt.AddDate(-1, 0, 0)
	e.refreshStores(context.Background())
	e.settleScratch()
	waitFor(t, "scratch to settle asked for", func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.asked == 1
	})
}

// End to end on a real store: the attempt finds its scratch in
// KAIRO_SCRATCH; once it has quiesced, its scratch is deleted after exit 0
// and kept after exit 1.
func TestAttemptScratchEndToEnd(t *testing.T) {
	for _, c := range []struct {
		mode, state string
	}{{helperScratchOK, "deleted"}, {helperScratchFail, "kept"}} {
		t.Run(c.state, func(t *testing.T) {
			ctx := context.Background()
			st := logSpaceStore(t, c.mode)
			root := t.TempDir()
			if err := st.UpsertProvider(ctx, "fs-provider", "n", "filesystem", nil); err != nil {
				t.Fatal(err)
			}
			if err := st.UpsertResourceInstance(ctx, store.ResourceInstance{ID: "n-disk", NodeID: "n", ProviderID: "fs-provider", Kind: "disk", StableIdentity: testFilesystem(root), Binding: json.RawMessage(`{}`), AdminState: "enabled"}); err != nil {
				t.Fatal(err)
			}
			spec := machine.Spec{Nodes: []machine.Node{{ID: "n"}}, Stores: []machine.Store{{ID: "scratch", Class: machine.StoreVolatile, Node: "n", Root: root, Filesystem: testFilesystem(root)}}}
			if _, err := st.ApplyMachines(ctx, spec, false); err != nil {
				t.Fatal(err)
			}
			e := logSpaceExecutor(st, t.TempDir(), func(string) (int64, error) { return 2 << 30, nil })
			if err := e.tick(ctx); err != nil {
				t.Fatal(err)
			}
			var attempt store.AttemptView
			waitFor(t, "the attempt's quiescence", func() bool {
				if err := e.reconcileQuiescence(ctx); err != nil {
					t.Fatal(err)
				}
				attempts, err := st.ListAttempts(ctx, store.AttemptFilter{})
				if err != nil || len(attempts) != 1 {
					return false
				}
				attempt = attempts[0]
				return attempt.State == "quiesced"
			})
			want := 0
			if c.mode == helperScratchFail {
				want = 1
			}
			if attempt.ExitCode == nil || *attempt.ExitCode != want {
				t.Fatalf("exit %v (4: no KAIRO_SCRATCH)", attempt.ExitCode)
			}
			dir := filepath.Join(root, scratchDirName, attempt.ID)
			if body, err := os.ReadFile(filepath.Join(dir, "out")); err != nil || string(body) != "scratch" {
				t.Fatalf("the attempt's file: %q %v", body, err)
			}
			e.settleDueScratch(ctx)
			status, err := st.ListStoreStatus(ctx)
			if err != nil || len(status) != 1 {
				t.Fatalf("status: %+v %v", status, err)
			}
			if c.state == "deleted" {
				if exists(dir) || len(status[0].Kept) != 0 {
					t.Fatalf("a successful attempt's scratch: exists %v, kept %+v", exists(dir), status[0].Kept)
				}
				return
			}
			kept := status[0].Kept
			if !exists(filepath.Join(dir, "out")) || len(kept) != 1 || kept[0].AttemptID != attempt.ID || kept[0].Reason == nil || *kept[0].Reason != "exit 1" ||
				kept[0].Bytes == nil || *kept[0].Bytes != int64(len("scratch")+len(attempt.ID)+1) {
				t.Fatalf("a failed attempt's scratch: %+v", kept)
			}
		})
	}
}
