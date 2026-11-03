package store

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"kairo/internal/machine"
)

// StoreObservation is what the executor serving a store reports of it.
// Usable is whether its root exists and is on the declared filesystem
// (Problem says why not); TotalBytes and FreeBytes are the filesystem's;
// Scratch is the size of each Kairo scratch directory under the root (one
// holding the marker that names it), by attempt id. Only what is under the
// root and Kairo made is counted.
type StoreObservation struct {
	StoreID    string           `json:"store_id"`
	ExecutorID string           `json:"executor_id"`
	Usable     bool             `json:"usable"`
	Problem    string           `json:"problem,omitempty"`
	TotalBytes *int64           `json:"total_bytes,omitempty"`
	FreeBytes  *int64           `json:"free_bytes,omitempty"`
	Scratch    map[string]int64 `json:"scratch,omitempty"`
}

// StoreStaleAfter is how old a store's latest observation may be before its
// status is stale (its executor reports every few minutes).
const StoreStaleAfter = 15 * time.Minute

// StoreNode is the node of a declared store (for checking who reports it).
func (s *Store) StoreNode(ctx context.Context, storeID string) (string, error) {
	return s.nodeOf(ctx, `SELECT node_id FROM machine_stores WHERE id=$1`, storeID)
}

// RecordStoreObservation keeps a store's latest observation. The bytes of
// each reported scratch directory are recorded on its attempt's scratch; the
// classes are summed from them: scratch_active (attempts still running),
// scratch_kept (kept attempts', and Kairo directories with no record, such as
// one whose recording failed). Pinned and cache bytes come later (P1b, P1c).
func (s *Store) RecordStoreObservation(ctx context.Context, obs StoreObservation) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var declared int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM machine_stores WHERE id=$1`, obs.StoreID).Scan(&declared); err != nil {
		return err
	}
	if declared == 0 {
		return ErrNotFound
	}
	t := now()
	var unrecorded int64
	for attemptID, bytes := range obs.Scratch {
		result, err := tx.ExecContext(ctx, `UPDATE attempt_scratch SET bytes=$1,updated_at=$2 WHERE attempt_id=$3 AND store_id=$4 AND state IN('active','kept')`, bytes, t, attemptID, obs.StoreID)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n == 0 {
			unrecorded += bytes
		}
	}
	var active, kept int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(bytes) FILTER (WHERE state='active'),0),COALESCE(SUM(bytes) FILTER (WHERE state='kept'),0) FROM attempt_scratch WHERE store_id=$1`, obs.StoreID).Scan(&active, &kept); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO store_observations(store_id,executor_id,observed_at,usable,problem,total_bytes,free_bytes,scratch_active_bytes,scratch_kept_bytes,pinned_bytes,cache_bytes)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,0,0)
		ON CONFLICT(store_id) DO UPDATE SET executor_id=excluded.executor_id,observed_at=excluded.observed_at,usable=excluded.usable,problem=excluded.problem,
		total_bytes=excluded.total_bytes,free_bytes=excluded.free_bytes,scratch_active_bytes=excluded.scratch_active_bytes,scratch_kept_bytes=excluded.scratch_kept_bytes,
		pinned_bytes=excluded.pinned_bytes,cache_bytes=excluded.cache_bytes`,
		obs.StoreID, obs.ExecutorID, t, obs.Usable, nullable(obs.Problem), obs.TotalBytes, obs.FreeBytes, active, kept+unrecorded); err != nil {
		return err
	}
	return tx.Commit()
}

// KeptScratch is an attempt's scratch kept on a store (its attempt failed or
// was lost).
type KeptScratch struct {
	AttemptID   string  `json:"attempt_id"`
	ExecutionID string  `json:"execution_id"`
	Task        string  `json:"task,omitempty"` // project/queue/task
	Path        string  `json:"path"`
	Bytes       *int64  `json:"bytes,omitempty"`
	Reason      *string `json:"reason,omitempty"`
	KeptAt      *string `json:"kept_at,omitempty"`
}

// StoreStatus is a store with what its executor last reported. State is
// "unknown" (never reported: no agent serves it yet, or one older than
// stores), "ready", "unusable" (Problem says why), "stale" (no report for
// StoreStaleAfter) or "undeclared" (no longer declared, but kept scratch is
// recorded there). ExternalBytes is what the filesystem uses that Kairo does
// not know: used minus Kairo's classes.
type StoreStatus struct {
	machine.Store
	State              string        `json:"state"`
	Problem            *string       `json:"problem,omitempty"`
	ObservedAt         *string       `json:"observed_at,omitempty"`
	ObservedBy         *string       `json:"observed_by,omitempty"`
	TotalBytes         *int64        `json:"total_bytes,omitempty"`
	FreeBytes          *int64        `json:"free_bytes,omitempty"`
	ScratchActiveBytes int64         `json:"scratch_active_bytes"`
	ScratchKeptBytes   int64         `json:"scratch_kept_bytes"`
	PinnedBytes        int64         `json:"pinned_bytes"`
	CacheBytes         int64         `json:"cache_bytes"`
	ExternalBytes      *int64        `json:"external_bytes,omitempty"`
	Kept               []KeptScratch `json:"kept"`
}

// ListStoreStatus is every declared store with its latest observation and
// kept scratch, then the stores no longer declared that still have kept
// scratch.
func (s *Store) ListStoreStatus(ctx context.Context) ([]StoreStatus, error) {
	declared, err := declaredStores(ctx, s.db)
	if err != nil {
		return nil, err
	}
	out := []StoreStatus{}
	index := map[string]int{}
	for _, d := range declared {
		index[d.ID] = len(out)
		out = append(out, StoreStatus{Store: d, State: "unknown", Kept: []KeptScratch{}})
	}
	stamp := time.Now().UTC()
	err = eachRow(ctx, s.db, `SELECT store_id,executor_id,observed_at,usable,problem,total_bytes,free_bytes,scratch_active_bytes,scratch_kept_bytes,pinned_bytes,cache_bytes FROM store_observations`, nil, func(rows *sql.Rows) error {
		var storeID, executorID, observedAt string
		var usable bool
		var problem sql.NullString
		var total, free sql.NullInt64
		var active, kept, pinned, cache int64
		if err := rows.Scan(&storeID, &executorID, &observedAt, &usable, &problem, &total, &free, &active, &kept, &pinned, &cache); err != nil {
			return err
		}
		i, ok := index[storeID]
		if !ok {
			return nil
		}
		st := &out[i]
		st.ObservedAt, st.ObservedBy = &observedAt, &executorID
		if problem.Valid {
			st.Problem = &problem.String
		}
		if total.Valid {
			st.TotalBytes = &total.Int64
		}
		if free.Valid {
			st.FreeBytes = &free.Int64
		}
		st.ScratchActiveBytes, st.ScratchKeptBytes, st.PinnedBytes, st.CacheBytes = active, kept, pinned, cache
		if total.Valid && free.Valid {
			external := max(0, total.Int64-free.Int64-active-kept-pinned-cache)
			st.ExternalBytes = &external
		}
		switch at, parseErr := time.Parse(time.RFC3339Nano, observedAt); {
		case !usable:
			st.State = "unusable"
		case parseErr != nil || stamp.Sub(at) > StoreStaleAfter:
			st.State = "stale"
		default:
			st.State = "ready"
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = eachRow(ctx, s.db, `SELECT k.store_id,k.attempt_id,a.execution_id,k.path,k.bytes,k.reason,k.settled_at,
		COALESCE(p.external_key||'/'||q.external_key||'/'||t.external_key,'')
		FROM attempt_scratch k JOIN attempts a ON a.id=k.attempt_id JOIN execution_requests e ON e.id=a.execution_id
		LEFT JOIN coordination_scopes p ON p.id=e.project_scope_id LEFT JOIN coordination_scopes q ON q.id=e.queue_scope_id LEFT JOIN coordination_scopes t ON t.id=e.task_scope_id
		WHERE k.state='kept' ORDER BY k.store_id,k.settled_at,k.attempt_id`, nil, func(rows *sql.Rows) error {
		var storeID string
		var k KeptScratch
		var bytes sql.NullInt64
		var reason, keptAt sql.NullString
		if err := rows.Scan(&storeID, &k.AttemptID, &k.ExecutionID, &k.Path, &bytes, &reason, &keptAt, &k.Task); err != nil {
			return err
		}
		if bytes.Valid {
			k.Bytes = &bytes.Int64
		}
		if reason.Valid {
			k.Reason = &reason.String
		}
		if keptAt.Valid {
			k.KeptAt = &keptAt.String
		}
		i, ok := index[storeID]
		if !ok {
			index[storeID] = len(out)
			i = len(out)
			out = append(out, StoreStatus{Store: machine.Store{ID: storeID}, State: "undeclared", Kept: []KeptScratch{}})
		}
		out[i].Kept = append(out[i].Kept, k)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool {
		if (out[i].State == "undeclared") != (out[j].State == "undeclared") {
			return out[j].State == "undeclared"
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}
