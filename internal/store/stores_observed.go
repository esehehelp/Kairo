package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"time"

	"kairo/internal/machine"
)

// StoreObservation is what the executor serving a store reports of it.
// Usable is whether its root exists and is on the declared filesystem
// (Problem says why not); TotalBytes and FreeBytes are the filesystem's.
// Scratch is each Kairo scratch directory found under the root (one holding
// the marker that names it), by attempt id, with its size, or -1 when it was
// not measured this time: a running attempt's is measured at every report, a
// kept one's once, when it is kept. ScratchScanned says the whole of
// <root>/scratch was read; only then is a record whose directory was not
// found taken as missing, and not one made during the scan (ScanSeconds
// long). Only what is under the root and Kairo made is counted.
type StoreObservation struct {
	StoreID        string           `json:"store_id"`
	ExecutorID     string           `json:"executor_id"`
	Usable         bool             `json:"usable"`
	Problem        string           `json:"problem,omitempty"`
	TotalBytes     *int64           `json:"total_bytes,omitempty"`
	FreeBytes      *int64           `json:"free_bytes,omitempty"`
	Scratch        map[string]int64 `json:"scratch,omitempty"`
	ScratchScanned bool             `json:"scratch_scanned,omitempty"`
	ScanSeconds    float64          `json:"scan_seconds,omitempty"`
}

// StoreStaleAfter is how old a store's latest observation may be before its
// status is stale (its executor reports every few minutes).
const StoreStaleAfter = 15 * time.Minute

// scratchScanMargin is added to a scan's duration before a record the scan
// did not find is taken as missing: one made while the scan ran is not.
const scratchScanMargin = time.Minute

// StoreNode is the node of a declared store (for checking who reports it).
func (s *Store) StoreNode(ctx context.Context, storeID string) (string, error) {
	return s.nodeOf(ctx, `SELECT node_id FROM machine_stores WHERE id=$1`, storeID)
}

// RecordStoreObservation keeps a store's latest observation and what it says
// of the store's scratch records, each in one statement: a found directory's
// measured bytes, a missing record found again (active again, or kept once
// settled), and, after a whole scan, a record whose directory was not found
// marked missing (bytes 0; Kairo did not delete it, so it is not deleted).
// Bytes found under the root without a record (a scratch whose recording
// failed) are kept as unrecorded. No decision reads any of this, so it is
// written without the decision lock (beginUnlockedWrite).
func (s *Store) RecordStoreObservation(ctx context.Context, obs StoreObservation) error {
	tx, err := s.beginUnlockedWrite(ctx)
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
	type found struct {
		AttemptID string `json:"attempt_id"`
		Bytes     int64  `json:"bytes"`
	}
	list := make([]found, 0, len(obs.Scratch))
	for attemptID, bytes := range obs.Scratch {
		list = append(list, found{attemptID, bytes})
	}
	body, err := json.Marshal(list)
	if err != nil {
		return err
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE attempt_scratch k SET bytes=CASE WHEN r.bytes>=0 THEN r.bytes ELSE k.bytes END,
		state=CASE WHEN k.state<>'missing' THEN k.state WHEN k.settled_at IS NULL THEN 'active' ELSE 'kept' END,
		missing_at=NULL,updated_at=$1
		FROM jsonb_to_recordset(($2)::jsonb) AS r(attempt_id text,bytes bigint)
		WHERE k.attempt_id=r.attempt_id AND k.store_id=$3 AND k.state IN('active','kept','missing')`, t, string(body), obs.StoreID); err != nil {
		return err
	}
	if obs.ScratchScanned {
		since := time.Now().UTC().Add(-time.Duration(obs.ScanSeconds*float64(time.Second)) - scratchScanMargin).Format(time.RFC3339Nano)
		if _, err = tx.ExecContext(ctx, `UPDATE attempt_scratch k SET state='missing',bytes=0,missing_at=$1,updated_at=$2
			WHERE k.store_id=$3 AND k.state IN('active','kept') AND (k.created_at)::timestamptz<($4)::timestamptz
			AND NOT EXISTS(SELECT 1 FROM jsonb_to_recordset(($5)::jsonb) AS r(attempt_id text,bytes bigint) WHERE r.attempt_id=k.attempt_id)`,
			t, t, obs.StoreID, since, string(body)); err != nil {
			return err
		}
	}
	var unrecorded, active, kept int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(r.bytes),0)::bigint FROM jsonb_to_recordset(($1)::jsonb) AS r(attempt_id text,bytes bigint)
		WHERE r.bytes>0 AND NOT EXISTS(SELECT 1 FROM attempt_scratch k WHERE k.attempt_id=r.attempt_id AND k.store_id=$2 AND k.state IN('active','kept','missing'))`,
		string(body), obs.StoreID).Scan(&unrecorded); err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(bytes) FILTER (WHERE state='active'),0)::bigint,COALESCE(SUM(bytes) FILTER (WHERE state='kept'),0)::bigint
		FROM attempt_scratch WHERE store_id=$1`, obs.StoreID).Scan(&active, &kept); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO store_observations(store_id,executor_id,observed_at,usable,problem,total_bytes,free_bytes,scratch_active_bytes,scratch_kept_bytes,scratch_unrecorded_bytes,pinned_bytes,cache_bytes)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0,0)
		ON CONFLICT(store_id) DO UPDATE SET executor_id=excluded.executor_id,observed_at=excluded.observed_at,usable=excluded.usable,problem=excluded.problem,
		total_bytes=excluded.total_bytes,free_bytes=excluded.free_bytes,scratch_active_bytes=excluded.scratch_active_bytes,scratch_kept_bytes=excluded.scratch_kept_bytes,
		scratch_unrecorded_bytes=excluded.scratch_unrecorded_bytes,pinned_bytes=excluded.pinned_bytes,cache_bytes=excluded.cache_bytes`,
		obs.StoreID, obs.ExecutorID, t, obs.Usable, nullable(obs.Problem), obs.TotalBytes, obs.FreeBytes, active, kept, unrecorded); err != nil {
		return err
	}
	return tx.Commit()
}

// KeptScratch is an attempt's scratch kept on a store (its attempt failed or
// was lost), or missing from it (MissingAt: its directory was no longer
// found, without Kairo deleting it).
type KeptScratch struct {
	AttemptID   string  `json:"attempt_id"`
	ExecutionID string  `json:"execution_id"`
	Task        string  `json:"task,omitempty"` // project/queue/task
	Path        string  `json:"path"`
	Bytes       *int64  `json:"bytes,omitempty"`
	Reason      *string `json:"reason,omitempty"`
	KeptAt      *string `json:"kept_at,omitempty"`
	MissingAt   *string `json:"missing_at,omitempty"`
}

// StoreStatus is a store with what its executor last reported. State is
// "unknown" (never reported: no agent serves it yet, or one older than
// stores), "ready", "unusable" (Problem says why), "stale" (no report for
// StoreStaleAfter) or "undeclared" (no longer declared, but kept or missing
// scratch is recorded there). The scratch classes are the records' bytes as
// they are now (the kept ones with what the last report found without a
// record); ExternalBytes is what the filesystem uses that Kairo does not
// know: used minus Kairo's classes. Missing lists the scratch whose
// directory was no longer found.
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
	Missing            []KeptScratch `json:"missing"`
}

// ListStoreStatus is every declared store with its latest observation and
// kept and missing scratch, then the stores no longer declared that still
// have some.
func (s *Store) ListStoreStatus(ctx context.Context) ([]StoreStatus, error) {
	tx, err := s.beginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return nil, err
	}
	out := []StoreStatus{}
	index := map[string]int{}
	add := func(st StoreStatus) int {
		index[st.ID] = len(out)
		out = append(out, st)
		return len(out) - 1
	}
	for _, d := range declared {
		add(StoreStatus{Store: d, State: "unknown", Kept: []KeptScratch{}, Missing: []KeptScratch{}})
	}
	err = eachRow(ctx, tx, `SELECT store_id,COALESCE(SUM(bytes) FILTER (WHERE state='active'),0)::bigint,COALESCE(SUM(bytes) FILTER (WHERE state='kept'),0)::bigint
		FROM attempt_scratch GROUP BY store_id`, nil, func(rows *sql.Rows) error {
		var storeID string
		var active, kept int64
		if err := rows.Scan(&storeID, &active, &kept); err != nil {
			return err
		}
		if i, ok := index[storeID]; ok {
			out[i].ScratchActiveBytes, out[i].ScratchKeptBytes = active, kept
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	stamp := time.Now().UTC()
	err = eachRow(ctx, tx, `SELECT store_id,executor_id,observed_at,usable,problem,total_bytes,free_bytes,scratch_unrecorded_bytes,pinned_bytes,cache_bytes FROM store_observations`, nil, func(rows *sql.Rows) error {
		var storeID, executorID, observedAt string
		var usable bool
		var problem sql.NullString
		var total, free sql.NullInt64
		var unrecorded, pinned, cache int64
		if err := rows.Scan(&storeID, &executorID, &observedAt, &usable, &problem, &total, &free, &unrecorded, &pinned, &cache); err != nil {
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
		st.ScratchKeptBytes += unrecorded
		st.PinnedBytes, st.CacheBytes = pinned, cache
		if total.Valid && free.Valid {
			external := max(0, total.Int64-free.Int64-st.ScratchActiveBytes-st.ScratchKeptBytes-pinned-cache)
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
	err = eachRow(ctx, tx, `SELECT k.store_id,k.state,k.attempt_id,a.execution_id,k.path,k.bytes,k.reason,k.settled_at,k.missing_at,
		COALESCE(p.external_key||'/'||q.external_key||'/'||t.external_key,'')
		FROM attempt_scratch k JOIN attempts a ON a.id=k.attempt_id JOIN execution_requests e ON e.id=a.execution_id
		LEFT JOIN coordination_scopes p ON p.id=e.project_scope_id LEFT JOIN coordination_scopes q ON q.id=e.queue_scope_id LEFT JOIN coordination_scopes t ON t.id=e.task_scope_id
		WHERE k.state IN('kept','missing') ORDER BY k.store_id,COALESCE(k.missing_at,k.settled_at),k.attempt_id`, nil, func(rows *sql.Rows) error {
		var storeID, state string
		var k KeptScratch
		var bytes sql.NullInt64
		var reason, keptAt, missingAt sql.NullString
		if err := rows.Scan(&storeID, &state, &k.AttemptID, &k.ExecutionID, &k.Path, &bytes, &reason, &keptAt, &missingAt, &k.Task); err != nil {
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
		if missingAt.Valid {
			k.MissingAt = &missingAt.String
		}
		i, ok := index[storeID]
		if !ok {
			i = add(StoreStatus{Store: machine.Store{ID: storeID}, State: "undeclared", Kept: []KeptScratch{}, Missing: []KeptScratch{}})
		}
		if state == "missing" {
			out[i].Missing = append(out[i].Missing, k)
		} else {
			out[i].Kept = append(out[i].Kept, k)
		}
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
