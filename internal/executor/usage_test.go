package executor

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"
)

// fakeProc is a /proc with processes given as (pid, parent, state,
// KAIRO_ATTEMPT_ID, PSS kB, write_bytes, cancelled_write_bytes); a negative
// PSS or write_bytes leaves that file out (the process exited meanwhile).
type fakeProcess struct {
	pid, parent             int
	state, attempt          string
	pssKB, written, cancels int64
}

func fakeProc(processes ...fakeProcess) fstest.MapFS {
	fsys := fstest.MapFS{}
	for _, p := range processes {
		dir := fmt.Sprint(p.pid)
		fsys[dir+"/stat"] = &fstest.MapFile{Data: fmt.Appendf(nil, "%d (proc %d) %s %d 0 0 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 %d 0 0", p.pid, p.pid, p.state, p.parent, 500+p.pid)}
		environ := "PATH=/bin\x00"
		if p.attempt != "" {
			environ += "KAIRO_ATTEMPT_ID=" + p.attempt + "\x00"
		}
		fsys[dir+"/environ"] = &fstest.MapFile{Data: []byte(environ)}
		if p.pssKB >= 0 {
			fsys[dir+"/smaps_rollup"] = &fstest.MapFile{Data: fmt.Appendf(nil, "00400000-7fff [rollup]\nRss:  %d kB\nPss:  %d kB\nPss_Anon:  1 kB\n", 2*p.pssKB, p.pssKB)}
		}
		if p.written >= 0 {
			fsys[dir+"/io"] = &fstest.MapFile{Data: fmt.Appendf(nil, "rchar: 1\nwchar: 2\nread_bytes: 3\nwrite_bytes: %d\ncancelled_write_bytes: %d\n", p.written, p.cancels)}
		}
	}
	return fsys
}

// deniedFS refuses to open the files named.
type deniedFS struct {
	fs.FS
	denied map[string]bool
}

func (d deniedFS) Open(name string) (fs.File, error) {
	if d.denied[name] {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
	}
	return d.FS.Open(name)
}

func value(p *int64) string {
	if p == nil {
		return "unknown"
	}
	return fmt.Sprint(*p)
}

func TestProcTreeIsTheLauncherDescendantsAndTheAttemptsProcesses(t *testing.T) {
	proc := fakeProc(
		fakeProcess{pid: 1, state: "S", pssKB: 10, written: 10},
		fakeProcess{pid: 100, parent: 1, state: "S", attempt: "att_1", pssKB: 1000, written: 5000}, // the launcher
		fakeProcess{pid: 101, parent: 100, state: "S", attempt: "att_1", pssKB: 2000, written: 7000, cancels: 1000},
		fakeProcess{pid: 102, parent: 101, state: "R", pssKB: 300, written: 100},                     // a grandchild with a cleaned environment
		fakeProcess{pid: 103, parent: 100, state: "Z", attempt: "att_1", pssKB: 9999, written: 9999}, // a zombie
		fakeProcess{pid: 104, parent: 100, state: "S", attempt: "att_1", pssKB: -1, written: -1},     // exited while read
		fakeProcess{pid: 200, parent: 1, state: "S", attempt: "att_1", pssKB: 40, written: 0},        // reparented, carrying the id
		fakeProcess{pid: 300, parent: 1, state: "S", attempt: "att_2", pssKB: 50000, written: 50000}, // another attempt
		fakeProcess{pid: 400, parent: 1, state: "S", pssKB: 70000, written: 70000},                   // the agent itself
	)
	targets := []usageTarget{
		{attemptID: "att_1", pid: 100, identity: "proc:100:starttime:600"},
		{attemptID: "att_2", pid: 300, identity: "proc:300:starttime:1"}, // launcher replaced: only the id counts
		{attemptID: "att_3", pid: 999, identity: "proc:999:starttime:1"}, // nothing left
	}
	got := sampleProcTrees(proc, 400, targets)
	want := map[string][2]string{
		"att_1": {fmt.Sprint((1000 + 2000 + 300 + 40) * 1024), fmt.Sprint(5000 + 6000 + 100 + 0)},
		"att_2": {fmt.Sprint(50000 * 1024), "50000"},
		"att_3": {"unknown", "unknown"},
	}
	for attempt, w := range want {
		if s := got[attempt]; value(s.ram) != w[0] || value(s.written) != w[1] {
			t.Errorf("%s: ram %s written %s, want %v", attempt, value(s.ram), value(s.written), w)
		}
	}
}

// A live process of the tree whose memory or I/O cannot be read makes that
// total unknown, not smaller.
func TestProcTreeWithAnUnreadableProcessIsUnknown(t *testing.T) {
	proc := fakeProc(
		fakeProcess{pid: 100, parent: 1, state: "S", pssKB: 1000, written: 5000},
		fakeProcess{pid: 101, parent: 100, state: "S", pssKB: 2000, written: 7000},
	)
	targets := []usageTarget{{attemptID: "att_1", pid: 100, identity: "proc:100:starttime:600"}}
	s := sampleProcTrees(deniedFS{proc, map[string]bool{"101/io": true}}, 0, targets)["att_1"]
	if value(s.ram) != fmt.Sprint(3000*1024) || s.written != nil {
		t.Fatalf("ram %s written %s", value(s.ram), value(s.written))
	}
	s = sampleProcTrees(deniedFS{proc, map[string]bool{"101/smaps_rollup": true}}, 0, targets)["att_1"]
	if s.ram != nil || value(s.written) != "12000" {
		t.Fatalf("ram %s written %s", value(s.ram), value(s.written))
	}
}

// usageRecorder records the usage reports of a fake coordinator.
type usageRecorder struct {
	Coordinator
	mu      sync.Mutex
	reports []string
}

func (u *usageRecorder) RecordAttemptUsage(_ context.Context, attemptID string, ram, written *int64) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.reports = append(u.reports, attemptID+" "+value(ram)+" "+value(written))
	return nil
}

func (u *usageRecorder) last() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.reports...)
}

func TestUsageRoundsReportAHighWaterOfBytesWrittenAndNeverBlock(t *testing.T) {
	recorder := &usageRecorder{}
	e := &Local{ID: "exec", Store: recorder, Logger: slog.New(slog.DiscardHandler)}
	e.trackUsage("att_1", 100, "proc:100:starttime:600")
	n := func(v int64) *int64 { return &v }
	rounds := []usageSample{{ram: n(10), written: n(500)}, {ram: n(8), written: n(300)}, {}}
	release := make(chan struct{})
	round := 0
	e.usageSampler = func(_ context.Context, targets []usageTarget) map[string]usageSample {
		if round == len(rounds) {
			<-release // a round that hangs
		}
		s := rounds[min(round, len(rounds)-1)]
		round++
		return map[string]usageSample{targets[0].attemptID: s}
	}
	for i := range rounds {
		e.usageSampled = time.Time{} // (due)
		e.sampleUsage(context.Background())
		waitFor(t, "the round to end", func() bool {
			e.mu.Lock()
			defer e.mu.Unlock()
			return !e.usageRunning
		})
		if got := len(recorder.last()); got != i+1 {
			t.Fatalf("round %d: %d reports", i, got)
		}
	}
	// written does not go down; an unknown sample is reported as unknown
	if got, want := strings.Join(recorder.last(), ","), "att_1 10 500,att_1 8 500,att_1 unknown unknown"; got != want {
		t.Fatalf("reports %s, want %s", got, want)
	}
	// a hung round holds no tick: further calls return at once and start nothing
	e.usageSampled = time.Time{}
	e.sampleUsage(context.Background())
	done := make(chan struct{})
	go func() {
		e.sampleUsage(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sampleUsage blocked behind a hung round")
	}
	// an attempt that exits during a round is not reported
	e.untrackUsage("att_1")
	close(release)
	waitFor(t, "the hung round to end", func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		return !e.usageRunning
	})
	if got := len(recorder.last()); got != len(rounds) {
		t.Fatalf("%d reports, want %d", got, len(rounds))
	}
}
