package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"kairo/internal/machine"
)

// A durable store keeps for good, and may lose bits without saying (exFAT
// has no journal): every verified copy is read back in full now and then by
// an executor serving its store, around the cache, and checked against the
// pack's size and sha256. A copy that no longer matches is marked damaged
// (check_problem, event copy_damaged) and listed by kairo store status;
// nothing is deleted or copied over it. How often each copy is read, and how
// much a store reads a day, is the daemon's data policy.

// DataPolicy is the daemon's data settings.
type DataPolicy struct {
	// ScrubInterval is how often each verified copy is read back.
	ScrubInterval time.Duration
	// ScrubBytesPerDay bounds what one durable store reads back in a day
	// (one copy bigger than it is read on its own once a day).
	ScrubBytesPerDay int64
	// ConfineFrom: every attempt launched from this time on is confined
	// (Launch.Confine), whether its execution asked or not. Zero: only those
	// that ask.
	ConfineFrom time.Time
}

// confinesAll reports whether every launch at t is confined.
func (s *Store) confinesAll(t time.Time) bool {
	from := s.data.ConfineFrom
	return !from.IsZero() && !t.Before(from)
}

// Defaults of the data policy.
const (
	DefaultScrubInterval    = 7 * 24 * time.Hour
	DefaultScrubBytesPerDay = 200 << 30
)

// SetDataPolicy replaces the data settings (a zero field keeps its default).
func (s *Store) SetDataPolicy(p DataPolicy) {
	if p.ScrubInterval <= 0 {
		p.ScrubInterval = DefaultScrubInterval
	}
	if p.ScrubBytesPerDay <= 0 {
		p.ScrubBytesPerDay = DefaultScrubBytesPerDay
	}
	s.data = p
}

// dataPolicy is the data policy, a zero field taken as its default.
func (s *Store) dataPolicy() DataPolicy {
	p := s.data
	if p.ScrubInterval <= 0 {
		p.ScrubInterval = DefaultScrubInterval
	}
	if p.ScrubBytesPerDay <= 0 {
		p.ScrubBytesPerDay = DefaultScrubBytesPerDay
	}
	return p
}

// ScrubOrder is a verified copy its executor is to read back now.
type ScrubOrder struct {
	ObjectID       string `json:"object_id"`
	StoreID        string `json:"store_id"`
	Root           string `json:"root"`
	Month          string `json:"month"`
	PackBytes      int64  `json:"pack_bytes"`
	PackSHA256     string `json:"pack_sha256"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

// ScrubResult is what reading a copy back found: its size and sha256 and its
// list's sha256 as read, and Problem when it could not be read whole.
type ScrubResult struct {
	ObjectID       string `json:"object_id"`
	StoreID        string `json:"store_id"`
	ExecutorID     string `json:"executor_id"`
	Bytes          int64  `json:"bytes"`
	SHA256         string `json:"sha256"`
	ManifestSHA256 string `json:"manifest_sha256"`
	Problem        string `json:"problem,omitempty"`
}

// ScrubsToMake is what an executor is to read back now: the verified copies
// on the durable stores it serves that were last read (or verified) before
// ScrubInterval ago, oldest first, as much as each store has left of its
// day's ScrubBytesPerDay.
func (s *Store) ScrubsToMake(ctx context.Context, executorID string) ([]ScrubOrder, error) {
	policy := s.dataPolicy()
	tx, err := s.beginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	me, err := executorInfoTx(ctx, tx, executorID)
	if err != nil {
		return nil, err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return nil, err
	}
	stamp := time.Now().UTC()
	due := stamp.Add(-policy.ScrubInterval).Format(time.RFC3339Nano)
	day := stamp.Add(-24 * time.Hour).Format(time.RFC3339Nano)
	out := []ScrubOrder{}
	for _, d := range declared {
		if d.Class != machine.StoreDurable || !me.serves(d, executorID) {
			continue
		}
		var read int64
		if err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(o.pack_bytes),0)::bigint FROM object_copies c JOIN data_objects o ON o.id=c.object_id
			WHERE c.store_id=$1 AND c.checked_at IS NOT NULL AND (c.checked_at)::timestamptz>($2)::timestamptz`, d.ID, day).Scan(&read); err != nil {
			return nil, err
		}
		left := policy.ScrubBytesPerDay - read
		err = eachRow(ctx, tx, `SELECT c.object_id,c.store_id,c.root,c.month,o.pack_bytes,o.pack_sha256,o.manifest_sha256 FROM object_copies c JOIN data_objects o ON o.id=c.object_id
			WHERE c.store_id=$1 AND c.root=$2 AND c.state='verified' AND (COALESCE(c.checked_at,c.verified_at))::timestamptz<($3)::timestamptz
			ORDER BY COALESCE(c.checked_at,c.verified_at),c.object_id`, []any{d.ID, d.Root, due}, func(rows *sql.Rows) error {
			var o ScrubOrder
			if err := rows.Scan(&o.ObjectID, &o.StoreID, &o.Root, &o.Month, &o.PackBytes, &o.PackSHA256, &o.ManifestSHA256); err != nil {
				return err
			}
			if o.PackBytes <= left || read == 0 && left == policy.ScrubBytesPerDay {
				out = append(out, o)
				left -= o.PackBytes
				read += o.PackBytes
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// RecordScrub records reading a verified copy back: checked now, damaged
// when it could not be read whole or no longer matches its pack.
func (s *Store) RecordScrub(ctx context.Context, r ScrubResult) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	me, err := executorInfoTx(ctx, tx, r.ExecutorID)
	if err != nil {
		return err
	}
	var state, path string
	var packBytes int64
	var packSHA, listSHA string
	var projectID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT c.state,COALESCE(c.path,''),o.pack_bytes,o.pack_sha256,o.manifest_sha256,o.project_scope_id FROM object_copies c JOIN data_objects o ON o.id=c.object_id
		WHERE c.object_id=$1 AND c.store_id=$2 FOR UPDATE OF c`, r.ObjectID, r.StoreID).Scan(&state, &path, &packBytes, &packSHA, &listSHA, &projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	serves := false
	for _, d := range declared {
		serves = serves || d.ID == r.StoreID && d.Class == machine.StoreDurable && me.serves(d, r.ExecutorID)
	}
	if !serves {
		return fmt.Errorf("executor %s does not serve durable store %s", r.ExecutorID, r.StoreID)
	}
	if state != "verified" {
		return fmt.Errorf("%w: the copy of %s on %s is %s, not verified", ErrStaleEpoch, r.ObjectID, r.StoreID, state)
	}
	problem := r.Problem
	if problem == "" && (r.Bytes != packBytes || r.SHA256 != packSHA || r.ManifestSHA256 != listSHA) {
		problem = fmt.Sprintf("read back as %d bytes, sha256 %s (list %s); packed as %d bytes, sha256 %s (list %s)", r.Bytes, r.SHA256, r.ManifestSHA256, packBytes, packSHA, listSHA)
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE object_copies SET checked_at=$1,check_problem=$2,updated_at=$3 WHERE object_id=$4 AND store_id=$5`, t, nullable(problem), t, r.ObjectID, r.StoreID); err != nil {
		return err
	}
	var project *string
	if projectID.Valid {
		project = &projectID.String
	}
	event, payload := "copy_checked", map[string]any{"store_id": r.StoreID, "path": path}
	if problem != "" {
		event, payload["problem"] = "copy_damaged", problem
	}
	if err = appendCoordinationEventTx(ctx, tx, event, project, "object", r.ObjectID, nil, payload); err != nil {
		return err
	}
	return tx.Commit()
}

// DamagedCopy is a verified copy that did not read back as its pack.
type DamagedCopy struct {
	ObjectID  string `json:"object_id"`
	Path      string `json:"path"`
	Problem   string `json:"problem"`
	CheckedAt string `json:"checked_at"`
}
