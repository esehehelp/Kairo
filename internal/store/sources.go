package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"kairo/internal/sourcesnap"
)

// Code snapshots (kairo source): kairo project apply packs the files of
// each source a task names ([sources.NAME] paths under its root), the daemon
// keeps the pack and its file list (api/sources.go) and records it here
// (source_snapshots; one row per distinct set of files: its id is the
// sha256 of its file list without mtimes). A task declared then is bound to
// that snapshot (orchestration_tasks.source_snapshot_id) for good: a later
// apply with other files binds only the tasks it declares, and a rerun runs
// the files it was declared with. Its executions (execution_requests
// .source_snapshot_id) run in the snapshot, unpacked on their node
// (executor/source.go).

// SourceSnapshot is a code snapshot as recorded: what it holds, and where
// and how it was made (the origin's git state is for reference only).
type SourceSnapshot struct {
	ID           string   `json:"id"`
	PackBytes    int64    `json:"pack_bytes"`
	PackSHA256   string   `json:"pack_sha256"`
	ListSHA256   string   `json:"list_sha256"`
	List         string   `json:"list,omitempty"`
	Files        int64    `json:"files"`
	ContentBytes int64    `json:"content_bytes"`
	Skipped      int64    `json:"skipped"`
	Project      string   `json:"project,omitempty"`
	SourceName   string   `json:"source_name,omitempty"`
	OriginHost   string   `json:"origin_host,omitempty"`
	OriginRoot   string   `json:"origin_root,omitempty"`
	GitHead      string   `json:"git_head,omitempty"`
	GitDirty     *bool    `json:"git_dirty,omitempty"`
	CreatedBy    string   `json:"created_by,omitempty"`
	CreatedAt    string   `json:"created_at"`
	Tasks        []string `json:"tasks,omitempty"`
}

// LaunchSource is the snapshot an attempt runs in: what its executor fetches
// and checks.
type LaunchSource struct {
	ID         string `json:"id"`
	PackBytes  int64  `json:"pack_bytes"`
	PackSHA256 string `json:"pack_sha256"`
	ListSHA256 string `json:"list_sha256"`
}

// ErrSourceInvalid is a snapshot that does not hold what it says.
var ErrSourceInvalid = errors.New("invalid code snapshot")

// CheckSourceSnapshot checks what a snapshot says of itself: its id is its
// file list's, the list has the sha256 given, and the pack is possible.
func CheckSourceSnapshot(snap SourceSnapshot) error {
	id, err := sourcesnap.ID(snap.List)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSourceInvalid, err)
	}
	sum := sha256.Sum256([]byte(snap.List))
	switch {
	case id != snap.ID:
		return fmt.Errorf("%w: its file list makes snapshot %s, not %s", ErrSourceInvalid, id, snap.ID)
	case hex.EncodeToString(sum[:]) != snap.ListSHA256:
		return fmt.Errorf("%w: the file list does not have the sha256 given", ErrSourceInvalid)
	case !sha256Hex.MatchString(snap.PackSHA256) || snap.PackBytes <= 0 || snap.Files <= 0 || snap.ContentBytes < 0 || snap.Skipped < 0:
		return fmt.Errorf("%w: impossible pack", ErrSourceInvalid)
	}
	return nil
}

// RecordSourceSnapshot records a snapshot the daemon now keeps (its pack
// checked against its file list by the caller). Its id must be its list's;
// recording one already recorded is no error (the first record stays).
func (s *Store) RecordSourceSnapshot(ctx context.Context, snap SourceSnapshot) error {
	if err := CheckSourceSnapshot(snap); err != nil {
		return err
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// the project by name: its first apply sends its snapshots before it
	// exists
	var projectID *string
	if snap.Project != "" {
		var scope string
		err := tx.QueryRowContext(ctx, `SELECT id FROM coordination_scopes WHERE kind='project' AND parent_id IS NULL AND external_key=$1`, snap.Project).Scan(&scope)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			projectID = &scope
		}
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO source_snapshots(id,pack_bytes,pack_sha256,list_sha256,list,files,content_bytes,skipped,project,source_name,origin_host,origin_root,git_head,git_dirty,created_by,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) ON CONFLICT(id) DO NOTHING`,
		snap.ID, snap.PackBytes, snap.PackSHA256, snap.ListSHA256, snap.List, snap.Files, snap.ContentBytes, snap.Skipped, nullable(snap.Project), nullable(snap.SourceName),
		nullable(snap.OriginHost), nullable(snap.OriginRoot), nullable(snap.GitHead), snap.GitDirty, nullable(snap.CreatedBy), now())
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 1 {
		if err = appendCoordinationEventTx(ctx, tx, "source_snapshot_recorded", projectID, "source", snap.ID, nil, map[string]any{"files": snap.Files, "content_bytes": snap.ContentBytes, "pack_bytes": snap.PackBytes, "source": snap.SourceName}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

const sourceColumns = `s.id,s.pack_bytes,s.pack_sha256,s.list_sha256,s.files,s.content_bytes,s.skipped,COALESCE(s.project,''),COALESCE(s.source_name,''),COALESCE(s.origin_host,''),COALESCE(s.origin_root,''),COALESCE(s.git_head,''),s.git_dirty,COALESCE(s.created_by,''),s.created_at`

func scanSource(rows interface{ Scan(...any) error }, snap *SourceSnapshot, extra ...any) error {
	var dirty sql.NullBool
	dest := append([]any{&snap.ID, &snap.PackBytes, &snap.PackSHA256, &snap.ListSHA256, &snap.Files, &snap.ContentBytes, &snap.Skipped, &snap.Project, &snap.SourceName,
		&snap.OriginHost, &snap.OriginRoot, &snap.GitHead, &dirty, &snap.CreatedBy, &snap.CreatedAt}, extra...)
	if err := rows.Scan(dest...); err != nil {
		return err
	}
	if dirty.Valid {
		snap.GitDirty = &dirty.Bool
	}
	return nil
}

// SourceSnapshotByID is a snapshot with its file list and the tasks bound to
// it; ErrNotFound for none.
func (s *Store) SourceSnapshotByID(ctx context.Context, id string) (SourceSnapshot, error) {
	var snap SourceSnapshot
	err := scanSource(s.db.QueryRowContext(ctx, `SELECT `+sourceColumns+`,s.list FROM source_snapshots s WHERE s.id=$1`, id), &snap, &snap.List)
	if errors.Is(err, sql.ErrNoRows) {
		return snap, ErrNotFound
	}
	if err != nil {
		return snap, err
	}
	err = eachRow(ctx, s.db, `SELECT p.external_key||'/'||t.name FROM orchestration_tasks t JOIN coordination_scopes p ON p.id=t.project_scope_id WHERE t.source_snapshot_id=$1 ORDER BY t.created_at,t.name`, []any{id}, func(rows *sql.Rows) error {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		snap.Tasks = append(snap.Tasks, name)
		return nil
	})
	return snap, err
}

// ListSourceSnapshots lists snapshots newest first (of one project when
// project is not ""), without their file lists.
func (s *Store) ListSourceSnapshots(ctx context.Context, project string, limit int) ([]SourceSnapshot, error) {
	query := `SELECT ` + sourceColumns + ` FROM source_snapshots s`
	args := []any{}
	if project != "" {
		query += ` WHERE s.project=` + bind(&args, project)
	}
	query += ` ORDER BY s.created_at DESC,s.id LIMIT ` + bind(&args, listLimit(limit))
	out := []SourceSnapshot{}
	err := eachRow(ctx, s.db, query, args, func(rows *sql.Rows) error {
		var snap SourceSnapshot
		if err := scanSource(rows, &snap); err != nil {
			return err
		}
		out = append(out, snap)
		return nil
	})
	return out, err
}

// launchSourceTx is the snapshot an execution runs in (nil for none).
func launchSourceTx(ctx context.Context, q queryer, executionID string) (*LaunchSource, error) {
	var src LaunchSource
	err := q.QueryRowContext(ctx, `SELECT s.id,s.pack_bytes,s.pack_sha256,s.list_sha256 FROM execution_requests e JOIN source_snapshots s ON s.id=e.source_snapshot_id WHERE e.id=$1`, executionID).
		Scan(&src.ID, &src.PackBytes, &src.PackSHA256, &src.ListSHA256)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &src, nil
}

// taskSourceTx is the snapshot a task is bound to ("" for none).
func taskSourceTx(ctx context.Context, q queryer, taskScopeID string) (string, error) {
	var id sql.NullString
	err := q.QueryRowContext(ctx, `SELECT source_snapshot_id FROM orchestration_tasks WHERE task_scope_id=$1`, taskScopeID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id.String, err
}

// sourceKnownTx reports whether a snapshot is recorded.
func sourceKnownTx(ctx context.Context, q queryer, id string) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM source_snapshots WHERE id=$1)`, id).Scan(&known)
	return known, err
}

// SourcesInUse are the snapshots of executions not yet terminal: what runs,
// or is to run, in them. An executor keeps those unpacked
// (executor/source.go).
func (s *Store) SourcesInUse(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT source_snapshot_id FROM execution_requests WHERE source_snapshot_id IS NOT NULL AND state<>'terminal' ORDER BY source_snapshot_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
