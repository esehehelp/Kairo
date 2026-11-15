package executor

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// An imported directory still being written is packed: a file written lately
// is packed from a copy, as last read when it never stops changing, so the
// pack holds a prefix of what the file becomes; a file gone before it is read
// is left out.
func TestAnImportBeingWrittenIsPackedAsRead(t *testing.T) {
	tries, delay := liveTries, liveRetryDelay
	liveTries, liveRetryDelay = 2, time.Millisecond
	t.Cleanup(func() { liveTries, liveRetryDelay = tries, delay })
	ctx := context.Background()
	base := t.TempDir()
	root, data := filepath.Join(base, "volatile"), filepath.Join(base, "collector")
	for _, d := range []string{root, data} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	old := filepath.Join(data, "done.jsonl")
	if err := os.WriteFile(old, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	long := time.Now().Add(-2 * liveWindow)
	if err := os.Chtimes(old, long, long); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(data, "live.jsonl")
	if err := os.WriteFile(live, bytes.Repeat([]byte("x"), 4<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f, err := os.OpenFile(live, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			return
		}
		defer f.Close()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = f.Write([]byte("y"))
			time.Sleep(50 * time.Microsecond)
		}
	}()
	res, err := (localStoreFS{}).PackDir(ctx, root, data, "obj_live")
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("an import being written was not packed: %v", err)
	}
	entries := unpack(t, res.packPath)
	final, _ := os.ReadFile(live)
	if entries["done.jsonl"] != "{}\n" || len(entries["live.jsonl"]) < 4<<20 || !bytes.HasPrefix(final, []byte(entries["live.jsonl"])) {
		t.Fatalf("packed %d bytes of live.jsonl (%d now), done.jsonl %q", len(entries["live.jsonl"]), len(final), entries["done.jsonl"])
	}
	if exists(filepath.Join(root, packsDirName, "obj_live"+packSuffix+stageSuffix)) {
		t.Fatal("the copy of the live file was left")
	}

	r, err := os.OpenRoot(data)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	packs, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer packs.Close()
	if _, _, err = packLiveFile(ctx, r, packs, "gone.stage", nil, "gone.jsonl", &packResult{}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a gone file: %v", err)
	}
}
