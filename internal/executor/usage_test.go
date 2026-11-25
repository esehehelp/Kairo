package executor

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// value prints a sample's figure.
func value(p *int64) string {
	if p == nil {
		return "unknown"
	}
	return fmt.Sprint(*p)
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
