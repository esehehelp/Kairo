// Package provenance is the store's record of what a task runs: its code
// snapshot and its container image (README: Code snapshots, Containers).
//
// kairo project apply packs the files of each source a task names
// ([sources.NAME] paths under its root); the daemon keeps the pack and its
// file list (api/sources.go) and records it here (source_snapshots; one row
// per distinct set of files: its id is the sha256 of its file list without
// mtimes). A task declared then is bound to that snapshot
// (orchestration_tasks.source_snapshot_id) and to the digest its image
// named (orchestration_tasks.image_ref) for good: a later apply binds only
// the tasks it declares, and a rerun runs what it was declared with. Its
// executions (execution_requests.source_snapshot_id, image_ref) run in them.
package provenance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"

	"kairo/internal/sourcesnap"
	"kairo/internal/store/dbtx"
)

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

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

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
func RecordSourceSnapshot(ctx context.Context, db *sql.DB, snap SourceSnapshot) error {
	if err := CheckSourceSnapshot(snap); err != nil {
		return err
	}
	tx, err := dbtx.BeginWrite(ctx, db)
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
		snap.ID, snap.PackBytes, snap.PackSHA256, snap.ListSHA256, snap.List, snap.Files, snap.ContentBytes, snap.Skipped, dbtx.Nullable(snap.Project), dbtx.Nullable(snap.SourceName),
		dbtx.Nullable(snap.OriginHost), dbtx.Nullable(snap.OriginRoot), dbtx.Nullable(snap.GitHead), snap.GitDirty, dbtx.Nullable(snap.CreatedBy), dbtx.Now())
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 1 {
		if err = dbtx.AppendEvent(ctx, tx, "source_snapshot_recorded", projectID, "source", snap.ID, nil, map[string]any{"files": snap.Files, "content_bytes": snap.ContentBytes, "pack_bytes": snap.PackBytes, "source": snap.SourceName}); err != nil {
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
// it; dbtx.ErrNotFound for none.
func SourceSnapshotByID(ctx context.Context, q dbtx.Queryer, id string) (SourceSnapshot, error) {
	var snap SourceSnapshot
	err := scanSource(q.QueryRowContext(ctx, `SELECT `+sourceColumns+`,s.list FROM source_snapshots s WHERE s.id=$1`, id), &snap, &snap.List)
	if errors.Is(err, sql.ErrNoRows) {
		return snap, dbtx.ErrNotFound
	}
	if err != nil {
		return snap, err
	}
	err = dbtx.EachRow(ctx, q, `SELECT p.external_key||'/'||t.name FROM orchestration_tasks t JOIN coordination_scopes p ON p.id=t.project_scope_id WHERE t.source_snapshot_id=$1 ORDER BY t.created_at,t.name`, []any{id}, func(rows *sql.Rows) error {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		snap.Tasks = append(snap.Tasks, name)
		return nil
	})
	return snap, err
}

// ListSourceSnapshots lists at most limit snapshots newest first (of one
// project when project is not ""), without their file lists.
func ListSourceSnapshots(ctx context.Context, q dbtx.Queryer, project string, limit int) ([]SourceSnapshot, error) {
	query := `SELECT ` + sourceColumns + ` FROM source_snapshots s`
	args := []any{}
	if project != "" {
		query += ` WHERE s.project=` + dbtx.Bind(&args, project)
	}
	query += ` ORDER BY s.created_at DESC,s.id LIMIT ` + dbtx.Bind(&args, limit)
	out := []SourceSnapshot{}
	err := dbtx.EachRow(ctx, q, query, args, func(rows *sql.Rows) error {
		var snap SourceSnapshot
		if err := scanSource(rows, &snap); err != nil {
			return err
		}
		out = append(out, snap)
		return nil
	})
	return out, err
}

// LaunchSourceOf is the snapshot an execution runs in (nil for none).
func LaunchSourceOf(ctx context.Context, q dbtx.Queryer, executionID string) (*LaunchSource, error) {
	return launchSnapshot(ctx, q, executionID, "source_snapshot_id")
}

// LaunchStreamSourceOf is the snapshot an execution's streams' stages run
// in when it is not the one its program runs in (nil for that one).
func LaunchStreamSourceOf(ctx context.Context, q dbtx.Queryer, executionID string) (*LaunchSource, error) {
	return launchSnapshot(ctx, q, executionID, "stream_snapshot_id")
}

func launchSnapshot(ctx context.Context, q dbtx.Queryer, executionID, column string) (*LaunchSource, error) {
	var src LaunchSource
	err := q.QueryRowContext(ctx, `SELECT s.id,s.pack_bytes,s.pack_sha256,s.list_sha256 FROM execution_requests e JOIN source_snapshots s ON s.id=e.`+column+` WHERE e.id=$1`, executionID).
		Scan(&src.ID, &src.PackBytes, &src.PackSHA256, &src.ListSHA256)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &src, nil
}

// TaskSource is the snapshot a task is bound to ("" for none).
func TaskSource(ctx context.Context, q dbtx.Queryer, taskScopeID string) (string, error) {
	var id sql.NullString
	err := q.QueryRowContext(ctx, `SELECT source_snapshot_id FROM orchestration_tasks WHERE task_scope_id=$1`, taskScopeID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id.String, err
}

// SourceKnown reports whether a snapshot is recorded.
func SourceKnown(ctx context.Context, q dbtx.Queryer, id string) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM source_snapshots WHERE id=$1)`, id).Scan(&known)
	return known, err
}

// TaskImage is the image a task is bound to, pinned by digest ("" for
// none). Only an executor with a container runtime takes an execution in an
// image, and such an executor takes nothing else (store.selectorMatches).
func TaskImage(ctx context.Context, q dbtx.Queryer, taskScopeID string) (string, error) {
	var ref sql.NullString
	err := q.QueryRowContext(ctx, `SELECT image_ref FROM orchestration_tasks WHERE task_scope_id=$1`, taskScopeID).Scan(&ref)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return ref.String, err
}

// LaunchImage is the image an execution runs in ("" for none).
func LaunchImage(ctx context.Context, q dbtx.Queryer, executionID string) (string, error) {
	var ref sql.NullString
	err := q.QueryRowContext(ctx, `SELECT image_ref FROM execution_requests WHERE id=$1`, executionID).Scan(&ref)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return ref.String, err
}
