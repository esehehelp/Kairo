package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"kairo/internal/store"
)

// Hardware history (store/hardware_history.go):
//
//	GET /api/hardware/samples?since=&until=&step=&key=
//	GET /api/hardware/leases?since=&until=&key=
//
// since and until are RFC 3339 times (default: the last 24 hours, since at
// most store.HistoryRetention ago); step is a duration ("60s", "5m") or
// seconds (default and least: what keeps a series within maxPoints buckets,
// and never under the history's 30 s); key, repeatable, limits the answer to those series
// (a GPU's UUID, a resource id, host/<host>/cpu).

// maxPoints is the most buckets a series of /api/hardware/samples holds.
const maxPoints = 2000

// historyWindow reads since, until and key of a hardware history request.
func historyWindow(r *http.Request, now time.Time) (since, until time.Time, keys []string, err error) {
	q := r.URL.Query()
	until = now
	if v := q.Get("until"); v != "" {
		if until, err = time.Parse(time.RFC3339, v); err != nil {
			return since, until, nil, errors.New("until must be an RFC 3339 time")
		}
	}
	since = until.Add(-24 * time.Hour)
	if v := q.Get("since"); v != "" {
		if since, err = time.Parse(time.RFC3339, v); err != nil {
			return since, until, nil, errors.New("since must be an RFC 3339 time")
		}
	}
	if oldest := now.Add(-store.HistoryRetention); since.Before(oldest) {
		since = oldest
	}
	if !since.Before(until) {
		return since, until, nil, fmt.Errorf("since must be before until (and within %s)", store.HistoryRetention)
	}
	for _, key := range q["key"] {
		if key = strings.TrimSpace(key); key != "" {
			keys = append(keys, key)
		}
	}
	return since.Truncate(time.Second), until.Truncate(time.Second), keys, nil
}

// historyStep is the bucket width of a samples request over [since, until).
func historyStep(r *http.Request, since, until time.Time) (time.Duration, error) {
	least := max(store.HistoryInterval, until.Sub(since)/maxPoints)
	step := least
	if v := r.URL.Query().Get("step"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			seconds, numberErr := strconv.ParseFloat(v, 64)
			if numberErr != nil || seconds <= 0 {
				return 0, errors.New("step must be a duration such as 60s or a number of seconds")
			}
			d = time.Duration(seconds * float64(time.Second))
		}
		step = max(d, least)
	}
	// whole seconds, rounded up
	return (step + time.Second - 1).Truncate(time.Second), nil
}

func (s *Server) hardwareSamples(w http.ResponseWriter, r *http.Request) {
	since, until, keys, err := historyWindow(r, time.Now())
	if err != nil {
		writeError(w, err)
		return
	}
	step, err := historyStep(r, since, until)
	if err != nil {
		writeError(w, err)
		return
	}
	samples, err := s.Store.HardwareSamples(r.Context(), since, until, step, keys)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, samples)
}

func (s *Server) hardwareLeases(w http.ResponseWriter, r *http.Request) {
	since, until, keys, err := historyWindow(r, time.Now())
	if err != nil {
		writeError(w, err)
		return
	}
	leases, err := s.Store.LeaseIntervals(r.Context(), since, until, keys)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"since": since.UTC().Format(time.RFC3339), "until": until.UTC().Format(time.RFC3339), "leases": leases})
}
