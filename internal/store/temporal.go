package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"kairo/internal/machine"
)

// Temporal data is data that can be made again: an orchestrated task
// declares its output regenerable ([tasks.execution] regenerable), and a
// directory can be imported as made by such a task (ImportRequest
// RegenerableBy). Its data object has lifecycle temporal: it is not packed
// and not copied to a durable store (the durable store keeps what cannot be
// made again); only its identity is made, the file list a pack would have
// (path, bytes, sha256, mtime of each file), written beside where its pack
// would be and kept here (manifest). The directory stays on its volatile
// store, read in place by every execution that names it as an input (and
// pinned while one waits to), until room runs short there and it is evicted
// (unused ones first), and it is made again by rerunning the task when an
// execution needs it, the new identity compared with the old.

// recordIdentityTx records the identity an executor made for a temporal
// object (made.Identity): the file list at <root>/packs/<object id>.tar.zst
// .files.tsv (no pack beside it) and the list itself, whose sha256 it must
// have. Recording the same identity again is no error.
func recordIdentityTx(ctx context.Context, tx *sql.Tx, made PackMade, root, state, kind, sourceID string, projectID, listSHA sql.NullString) error {
	if state == "packed" {
		if listSHA.String == made.ManifestSHA256 {
			return tx.Commit()
		}
		return fmt.Errorf("object %s already has an identity (sha256 %s)", made.ObjectID, listSHA.String)
	}
	sum := sha256.Sum256([]byte(made.Manifest))
	switch {
	case made.PackPath != "" || made.PackBytes != 0 || made.PackSHA256 != "":
		return fmt.Errorf("object %s is temporal: it has no pack", made.ObjectID)
	case !machine.IsPackPath(made.ManifestPath, root, made.ObjectID, ".files.tsv"):
		return fmt.Errorf("file list %s of object %s is not <root>/packs/<object id>.tar.zst.files.tsv under %s", made.ManifestPath, made.ObjectID, root)
	case !sha256Hex.MatchString(made.ManifestSHA256) || hex.EncodeToString(sum[:]) != made.ManifestSHA256:
		return fmt.Errorf("object %s: the file list does not have the sha256 given", made.ObjectID)
	case len(made.Manifest) > maxManifestBytes:
		return fmt.Errorf("object %s: a file list of %d bytes is more than the %d kept", made.ObjectID, len(made.Manifest), maxManifestBytes)
	case made.ContentBytes < 0 || made.Files < 0 || made.Skipped < 0:
		return fmt.Errorf("object %s: impossible sizes", made.ObjectID)
	}
	t := now()
	if _, err := tx.ExecContext(ctx, `UPDATE data_objects SET state='packed',manifest_path=$1,content_bytes=$2,files=$3,skipped=$4,pack_bytes=0,manifest_sha256=$5,manifest=$6,packed_at=$7,updated_at=$8 WHERE id=$9`,
		made.ManifestPath, made.ContentBytes, made.Files, made.Skipped, made.ManifestSHA256, made.Manifest, t, t, made.ObjectID); err != nil {
		return err
	}
	var project *string
	if projectID.Valid {
		project = &projectID.String
	}
	if err := appendCoordinationEventTx(ctx, tx, "identified", project, "object", made.ObjectID, nil, map[string]any{"kind": kind, "source_id": sourceID,
		"content_bytes": made.ContentBytes, "files": made.Files, "skipped": made.Skipped, "manifest_sha256": made.ManifestSHA256}); err != nil {
		return err
	}
	return tx.Commit()
}

// regenerableTaskTx is the task scope of a task of a project declared
// regenerable (what an import made by it names); ErrImportRefused otherwise.
func regenerableTaskTx(ctx context.Context, q queryer, projectScopeID, task string) (string, error) {
	var scope, template string
	err := q.QueryRowContext(ctx, `SELECT task_scope_id,execution_template_json FROM orchestration_tasks WHERE project_scope_id=$1 AND name=$2`, projectScopeID, task).Scan(&scope, &template)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: the project has no task %q", ErrImportRefused, task)
	}
	if err != nil {
		return "", err
	}
	var t struct {
		Regenerable bool `json:"regenerable"`
	}
	if err := json.Unmarshal([]byte(template), &t); err != nil || !t.Regenerable {
		return "", fmt.Errorf("%w: task %q is not declared regenerable ([tasks.execution] regenerable = true)", ErrImportRefused, task)
	}
	return scope, nil
}
