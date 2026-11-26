package loader

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"time"
)

// Status is what a loader tells of its stream (kairo loader serve --status
// writes it to a file the node's agent reads and reports to Kairo, for task
// show): where each rank stands, what is made ahead for it, how fast it is
// handed over and how long its taker asked with nothing to hand.
type Status struct {
	At time.Time `json:"at"`
	// World is the ranks the stream is for (0: no rank came yet).
	World int          `json:"world"`
	Ranks []RankStatus `json:"ranks,omitempty"`
	// Error is the stream's failure (handed to every rank).
	Error string `json:"error,omitempty"`
	// WindowSeconds is what the rates are over (0: no rates yet).
	WindowSeconds float64 `json:"window_seconds,omitempty"`
}

// RankStatus is one rank of a Status.
type RankStatus struct {
	Rank int `json:"rank"`
	// Position is its next position not handed over.
	Position Position `json:"position"`
	// Taker: a process takes it now.
	Taker bool `json:"taker"`
	// Queued and QueuedBytes are what is made for it and not handed over
	// yet, of at most QueueItems items and BudgetBytes bytes.
	Queued      int   `json:"queued"`
	QueueItems  int   `json:"queue_items"`
	QueuedBytes int64 `json:"queued_bytes"`
	BudgetBytes int64 `json:"budget_bytes"`
	// Ended: all its records were made (the stream's end).
	Ended bool `json:"ended,omitempty"`
	// Taken, TakenBytes and StarvedSeconds count over the loader's life:
	// items handed over, their bytes, and the time its taker asked for
	// items while none was made.
	Taken          int64   `json:"taken"`
	TakenBytes     int64   `json:"taken_bytes"`
	StarvedSeconds float64 `json:"starved_seconds"`
	// Reading is where its producer reads each source now (ahead of the
	// position by what is made and in the stages).
	Reading []SourceAt `json:"reading,omitempty"`
	// Over the last WindowSeconds: items and bytes handed over per second,
	// and the share of the time its taker asked with nothing to hand (the
	// loader behind: the training waits for data).
	ItemsPerSecond float64 `json:"items_per_second"`
	BytesPerSecond float64 `json:"bytes_per_second"`
	StarvedShare   float64 `json:"starved_share"`
}

// SourceAt is where a source is read: its epoch and file.
type SourceAt struct {
	Source string `json:"source"`
	Epoch  int64  `json:"epoch"`
	File   string `json:"file,omitempty"`
}

// rankCounters count what a rank was handed and how long its taker waited,
// over the loader's life.
type rankCounters struct {
	taken, takenBytes, starved atomic.Int64 // starved: nanoseconds
}

// reading is where the inputs are read now, by source: nil while it is
// where prev says (so that the order's reader allocates only on a change).
func (in *inputs) reading(prev []SourceAt) []SourceAt {
	changed := len(prev) != len(in.sources)
	for i, s := range in.sources {
		if changed || prev[i].Epoch != s.epoch || prev[i].File != s.fileName() {
			changed = true
			break
		}
	}
	if !changed {
		return nil
	}
	at := make([]SourceAt, len(in.sources))
	for i, s := range in.sources {
		at[i] = SourceAt{Source: s.spec.Name, Epoch: s.epoch, File: s.fileName()}
	}
	return at
}

// fileName is the file being read ("" between files).
func (s *sourceStream) fileName() string {
	if s.file < 0 || s.file >= len(s.order) {
		return ""
	}
	return s.spec.Files[s.order[s.file]].Name
}

// Status is the stream now, its counters over the loader's life (no rates:
// WriteStatus adds them).
func (l *Loader) Status() Status {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := Status{At: time.Now().UTC(), World: l.world}
	if l.hard != nil {
		st.Error = l.hard.Error()
	}
	gen := l.gen
	for r := 0; r < l.world; r++ {
		rs := RankStatus{Rank: r, Position: l.delivered[r], Taker: l.present[r], QueueItems: rankQueue}
		if r < len(l.counters) {
			c := &l.counters[r]
			rs.Taken, rs.TakenBytes = c.taken.Load(), c.takenBytes.Load()
			rs.StarvedSeconds = time.Duration(c.starved.Load()).Seconds()
		}
		if gen != nil {
			rs.Queued, rs.BudgetBytes = len(gen.queues[r]), gen.budget
			if gen.queuedBytes != nil {
				rs.QueuedBytes = gen.queuedBytes[r].Load()
			}
			if gen.reading != nil {
				if at := gen.reading[r].Load(); at != nil {
					rs.Reading = *at
				}
			}
			select {
			case <-gen.done[r]:
				rs.Ended = gen.last[r] != nil && gen.last[r].end
			default:
			}
		}
		st.Ranks = append(st.Ranks, rs)
	}
	return st
}

// WriteStatus writes the stream's status to path (whole, by a rename) every
// interval until ctx ends, with each rank's rates over the last window.
func (l *Loader) WriteStatus(ctx context.Context, path string, every, window time.Duration) {
	var history []Status
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		st := l.Status()
		history = append(history, st)
		for len(history) > 1 && st.At.Sub(history[0].At) > window {
			history = history[1:]
		}
		withRates(&st, history[0])
		if body, err := json.Marshal(st); err == nil {
			tmp := path + ".tmp"
			if os.WriteFile(tmp, body, 0o644) == nil {
				_ = os.Rename(tmp, path)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// withRates sets st's rates from what changed since before.
func withRates(st *Status, before Status) {
	span := st.At.Sub(before.At).Seconds()
	if span <= 0 || before.World != st.World {
		return
	}
	st.WindowSeconds = span
	for i := range st.Ranks {
		now, then := &st.Ranks[i], before.Ranks[i]
		now.ItemsPerSecond = float64(now.Taken-then.Taken) / span
		now.BytesPerSecond = float64(now.TakenBytes-then.TakenBytes) / span
		now.StarvedShare = min(1, max(0, (now.StarvedSeconds-then.StarvedSeconds)/span))
	}
}
