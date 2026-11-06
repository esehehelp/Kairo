package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// An object that keeps failing is tried again at the next round, then after
// twice as long each time (from the round's interval), at most an hour; one
// done clears it.
func TestFailuresWaitLongerEachTime(t *testing.T) {
	e := &Local{ID: "e", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	r := &round{interval: time.Minute}
	fail := func() time.Duration {
		e.roundProblem(r, "obj", "not copied", errors.New("no"))
		at, ok := r.retryAt["obj"]
		if !ok {
			return 0
		}
		return time.Until(at).Round(time.Minute)
	}
	var waits []time.Duration
	for i := 0; i < 9; i++ {
		waits = append(waits, fail())
	}
	if fmt.Sprint(waits) != "[0s 1m0s 2m0s 4m0s 8m0s 16m0s 32m0s 1h0m0s 1h0m0s]" {
		t.Fatalf("waits %v", waits)
	}
	if e.due(r, "obj") || !e.due(r, "other") {
		t.Fatal("due while waiting, or another object waits")
	}
	e.roundProblem(r, "obj", "", nil)
	if !e.due(r, "obj") || r.failures["obj"] != 0 {
		t.Fatal("still waiting once done")
	}
}

// A partial as long as its source is whole already and not fetched again;
// one longer starts over; a source yielding more than its size fails.
func TestWriteFromTakesAWholePartialAndBoundsTheSource(t *testing.T) {
	dir := t.TempDir()
	r, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := os.WriteFile(filepath.Join(dir, "p.partial"), []byte("0123456789"), 0o644); err != nil {
		t.Fatal(err)
	}
	var asked []int64
	source := func(body string) func(string, int64) (io.ReadCloser, int64, error) {
		return func(_ string, offset int64) (io.ReadCloser, int64, error) {
			asked = append(asked, offset)
			return io.NopCloser(strings.NewReader(body[offset:])), offset, nil
		}
	}
	ctx := context.Background()
	if err := writeFrom(ctx, r, "p.partial", source("0123456789"), "p", true, 10); err != nil || len(asked) != 0 {
		t.Fatalf("a whole partial: %v, asked %v", err, asked)
	}
	if err := writeFrom(ctx, r, "p.partial", source("01234"), "p", true, 5); err != nil || fmt.Sprint(asked) != "[0]" {
		t.Fatalf("a longer partial: %v, asked %v", err, asked)
	}
	if body, _ := os.ReadFile(filepath.Join(dir, "p.partial")); string(body) != "01234" {
		t.Fatalf("partial %q", body)
	}
	if err := writeFrom(ctx, r, "q.partial", source("0123456789abc"), "q", true, 10); err == nil || !strings.Contains(err.Error(), "yields more than its 10 bytes") {
		t.Fatalf("a source yielding more: %v", err)
	}
}

// A fetch whose body stops coming is given up once nothing arrives for
// stallTimeout.
func TestAStalledFetchIsGivenUp(t *testing.T) {
	saved := stallTimeout
	stallTimeout = 300 * time.Millisecond
	t.Cleanup(func() { stallTimeout = saved })
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partly"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	e := &Local{ID: "e", PackClient: server.Client(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	order := store.CopyOrder{ObjectID: "obj_1", Source: store.CopySource{Addr: server.Listener.Addr().String(), Token: "t"}}
	body, from, err := e.fetchRemote(context.Background(), order, "obj_1.tar.zst", 0)
	if err != nil || from != 0 {
		t.Fatalf("fetch: %v %d", err, from)
	}
	started := time.Now()
	got, err := io.ReadAll(body)
	body.Close()
	if err == nil || string(got) != "partly" || time.Since(started) > 10*time.Second {
		t.Fatalf("read %q %v after %v", got, err, time.Since(started))
	}
}

// A copy renamed but not recorded (an order is only for a copy not recorded)
// that no longer matches is removed, and copied again at the next round.
func TestAnUnrecordedCopyThatDoesNotMatchIsMadeAgain(t *testing.T) {
	base := t.TempDir()
	volatile, durable := filepath.Join(base, "volatile"), filepath.Join(base, "durable")
	res := packed(t, volatile)
	if err := os.MkdirAll(durable, 0o755); err != nil {
		t.Fatal(err)
	}
	fake := newFakeCopies()
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	e.stores = []servedStore{
		{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true},
		{decl: machine.Store{ID: "e-durable", Class: machine.StoreDurable, Root: durable}, fs: localStoreFS{}, usable: true},
	}
	order := orderFor(res, durable, store.CopySource{Local: true, StoreID: "c-scratch", Root: volatile})
	fake.copies = []store.CopyOrder{order}
	e.copyDue(context.Background())
	if len(fake.copied) != 1 {
		t.Fatalf("not copied: %s", logs.String())
	}
	final := fake.copied[0].Path
	body, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	body[len(body)/2] ^= 0xff
	if err := os.WriteFile(final, body, 0o644); err != nil {
		t.Fatal(err)
	}
	e.copyDue(context.Background())
	if len(fake.copied) != 1 || exists(final) || exists(final+manifestSuffix) || !strings.Contains(logs.String(), "does not match the pack") {
		t.Fatalf("a copy that does not match was kept or recorded: %+v (there: %v)\n%s", fake.copied, exists(final), logs.String())
	}
	e.copyDue(context.Background())
	if len(fake.copied) != 2 || sha256File(t, final) != res.packSHA256 {
		t.Fatalf("not copied again: %+v\n%s", fake.copied, logs.String())
	}
}

// An output directory the daemon refuses for good (the run's directory is
// packed already) is removed again when it holds only its marker.
func TestARefusedOutputDirectoryIsRemovedAgain(t *testing.T) {
	root := t.TempDir()
	fake := &fakeOutputs{fakeScratch: &fakeScratch{fakeStores: &fakeStores{}}, outputErr: fmt.Errorf("%w: packed already", store.ErrStaleEpoch)}
	var logs bytes.Buffer
	e := storesExecutor(t, fake, &logs)
	volatile := filepath.Join(root, "volatile")
	e.stores = []servedStore{{decl: machine.Store{ID: "c-scratch", Class: machine.StoreVolatile, Root: volatile}, fs: localStoreFS{}, usable: true}}
	launch := testLaunch()
	launch.RunID = "exe_1"
	e.makeOutput(launch)
	if exists(filepath.Join(volatile, outputDirName, "exe_1")) {
		t.Fatal("a refused output directory was left")
	}
	if _, ok := envValue(e.launchEnv(launch), "KAIRO_OUTPUT_DIR"); ok {
		t.Fatal("named to the attempt")
	}
	// one that holds what a run wrote stays
	dir, err := (localStoreFS{}).CreateKind(context.Background(), volatile, kindOutput, "exe_1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.txt"), []byte("kept"), 0o644); err != nil {
		t.Fatal(err)
	}
	e.makeOutput(launch)
	if !exists(filepath.Join(dir, "result.txt")) {
		t.Fatal("what a run wrote was removed")
	}
}
