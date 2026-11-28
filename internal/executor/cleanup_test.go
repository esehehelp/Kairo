package executor

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"kairo/internal/store"
)

// fakeReleases answers ReleaseReservation with failures first, then success,
// and serves unlaunched leases.
type fakeReleases struct {
	Coordinator
	mu         sync.Mutex
	failures   int
	releases   []string
	unlaunched []store.UnlaunchedLease
	terminals  []string
	stale      bool // ReleaseReservation answers ErrStaleEpoch (the lease went past prepared)
}

func (f *fakeReleases) ReleaseReservation(_ context.Context, leaseID string, _ int64, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stale {
		return store.ErrStaleEpoch
	}
	if f.failures > 0 {
		f.failures--
		return errors.New("reservation cleanup lacks a fresh GPU observation allowed by its conflict policy")
	}
	f.releases = append(f.releases, leaseID)
	return nil
}

func (f *fakeReleases) RecordTerminal(_ context.Context, attemptID, _ string, _ int64, exitCode int, exitSignal string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.terminals = append(f.terminals, attemptID+" "+strconv.Itoa(exitCode)+" "+exitSignal)
	return nil
}

func (f *fakeReleases) ListUnlaunchedLeases(context.Context, string, time.Duration) ([]store.UnlaunchedLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unlaunched, nil
}

func (f *fakeReleases) released() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.releases...)
}

func quietExecutor(st Coordinator) *Local {
	return NewLocal("e", st, os.TempDir(), slog.New(slog.DiscardHandler))
}

// A reservation whose lease cannot be released yet is kept and released at a
// later tick, not forgotten with its resources held.
func TestFailedReservationCleanupIsRetriedUntilTheLeaseIsReleased(t *testing.T) {
	fake := &fakeReleases{failures: 2}
	e := quietExecutor(fake)
	lease := store.Lease{ID: "lea_1", CoordinationEpoch: 1, State: "reserved"}
	e.abandonReservation(lease, nil, "prepare failed", "", nil)
	if len(fake.released()) != 0 || len(e.pendingCleanups) != 1 {
		t.Fatalf("after the failed cleanup: released %v, pending %d", fake.released(), len(e.pendingCleanups))
	}
	for i := 0; i < 2; i++ {
		for id, c := range e.pendingCleanups {
			c.retryAt = time.Time{} // due now
			e.pendingCleanups[id] = c
		}
		e.retryCleanups(context.Background())
	}
	if got := fake.released(); len(got) != 1 || got[0] != "lea_1" || len(e.pendingCleanups) != 0 {
		t.Fatalf("released %v, pending %d", got, len(e.pendingCleanups))
	}
}

// A launch given up whose lease is past release (its activation went through
// although no answer arrived) is ended through RecordTerminal.
func TestAbandonedLaunchPastReleaseIsEndedThroughItsAttempt(t *testing.T) {
	fake := &fakeReleases{stale: true}
	e := quietExecutor(fake)
	e.abandonReservation(store.Lease{ID: "lea_2", CoordinationEpoch: 3}, nil, "launch failed", "att_2", nil)
	if len(e.pendingCleanups) != 0 || len(fake.terminals) != 1 || fake.terminals[0] != "att_2 -1 launch_failed" {
		t.Fatalf("pending %d, terminals %v", len(e.pendingCleanups), fake.terminals)
	}
}

// A lease the store holds for this executor that it does not know (a
// reservation whose answer was lost) is released; one held at the gang
// barrier is not.
func TestUnlaunchedLeasesNobodyHoldsAreReleased(t *testing.T) {
	fake := &fakeReleases{unlaunched: []store.UnlaunchedLease{
		{Lease: store.Lease{ID: "lea_orphan", CoordinationEpoch: 1, State: "reserved"}},
		{Lease: store.Lease{ID: "lea_gang", CoordinationEpoch: 1, State: "prepared"}},
	}}
	e := quietExecutor(fake)
	e.pendingGang = map[string]pendingGangRank{"lea_gang": {}}
	e.sweepUnlaunched(context.Background())
	if got := fake.released(); len(got) != 1 || got[0] != "lea_orphan" {
		t.Fatalf("released %v", got)
	}
	// not asked again before the interval
	fake.mu.Lock()
	fake.unlaunched = append(fake.unlaunched, store.UnlaunchedLease{Lease: store.Lease{ID: "lea_later"}})
	fake.mu.Unlock()
	e.sweepUnlaunched(context.Background())
	if got := fake.released(); len(got) != 1 {
		t.Fatalf("swept again within the interval: %v", got)
	}
}

type statusError int

func (s statusError) Error() string   { return "agent API " + strconv.Itoa(int(s)) }
func (s statusError) HTTPStatus() int { return int(s) }

// A call is made again only while it gets no answer from the store.
func TestRetryUnansweredAsksAgainOnlyWithoutAnAnswer(t *testing.T) {
	e := quietExecutor(nil)
	e.RetryDelay = time.Millisecond
	for _, tc := range []struct {
		err   error
		calls int
	}{
		{statusError(503), launchRetries},
		{context.DeadlineExceeded, launchRetries},
		{statusError(400), 1},
		{store.ErrStaleEpoch, 1},
		{errors.New("invalid or consumed launch authorization"), 1},
	} {
		calls := 0
		err := e.retryUnanswered(context.Background(), func() error { calls++; return tc.err })
		if !errors.Is(err, tc.err) || calls != tc.calls {
			t.Errorf("%v: %d calls, err %v; want %d", tc.err, calls, err, tc.calls)
		}
	}
	calls := 0
	if err := e.retryUnanswered(context.Background(), func() error {
		calls++
		if calls == 1 {
			return statusError(502)
		}
		return nil
	}); err != nil || calls != 2 {
		t.Fatalf("answered on the second call: %d calls, %v", calls, err)
	}
}

// refusedActivation refuses every activation (a fenced launch) after the
// launcher has had time to start what it would.
type refusedActivation struct {
	Coordinator
}

func (refusedActivation) ActivateLaunch(context.Context, string, string, int64, string, int, string) error {
	time.Sleep(time.Second)
	return store.ErrStaleEpoch
}

// A launch whose activation is refused runs nothing: its shim never gets
// the go-ahead, and is gone with no record of the attempt left.
func TestARefusedActivationRunsNothing(t *testing.T) {
	logDir := t.TempDir()
	e := NewLocal("e", refusedActivation{}, logDir, slog.New(slog.DiscardHandler))
	e.FreeSpace = func(string) (int64, error) { return 1 << 40, nil }
	launch := &store.Launch{
		Attempt: store.Attempt{ID: "att-refused"},
		Lease:   store.Lease{ID: "lea-refused", CoordinationEpoch: 1},
		Argv:    []string{os.Args[0], helperTree},
		CWD:     t.TempDir(),
	}
	if err := e.start(launch); !errors.Is(err, store.ErrStaleEpoch) {
		t.Fatalf("start: %v", err)
	}
	if out, err := os.ReadFile(filepath.Join(logDir, "att-refused.stdout.log")); err != nil || len(out) != 0 {
		t.Fatalf("the command ran: %q %v", out, err)
	}
	if _, err := os.Stat(recordPath(logDir, "att-refused")); !os.IsNotExist(err) {
		t.Fatalf("a record of the refused attempt is left: %v", err)
	}
}

// The cleanup of a launch whose activation failed releases nothing while a
// process of its tree runs: it kills the tree again and waits until it is
// seen empty.
func TestCleanupOfAnUnactivatedLaunchWaitsForItsTreeToBeEmpty(t *testing.T) {
	cmd, handle, child := startTestTree(t, helperTree)
	fake := &fakeReleases{}
	e := quietExecutor(fake)
	tree := processTree{attemptID: "att-" + helperTree, pid: cmd.Process.Pid, handle: handle}
	e.abandonReservation(store.Lease{ID: "lea_tree", CoordinationEpoch: 1}, nil, "activation refused", "att-"+helperTree, &tree)
	if got := fake.released(); len(got) != 0 || len(e.pendingCleanups) != 1 {
		t.Fatalf("released with its tree running: %v (pending %d)", got, len(e.pendingCleanups))
	}
	deadline := time.Now().Add(20 * time.Second)
	for len(fake.released()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("never released after the tree was killed")
		}
		time.Sleep(200 * time.Millisecond)
		for id, c := range e.pendingCleanups {
			c.retryAt = time.Time{}
			e.pendingCleanups[id] = c
		}
		e.retryCleanups(context.Background())
	}
	if processAlive(child) {
		t.Fatalf("child %d alive when the reservation was released", child)
	}
}
