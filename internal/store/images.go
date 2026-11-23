package store

import (
	"context"
	"database/sql"
	"errors"
)

// Container images (README: Containers). A task declared with an image
// (orchestration.Execution.Image, naming the project's [images.NAME]) is
// bound when it is declared to the digest its reference named then
// (orchestration_tasks.image_ref, from the apply's pins), for good, as it is
// to its code snapshot; each execution of it records the image it runs in
// (execution_requests.image_ref) and is handed it at launch (Launch.Image).
// Only an executor with a container runtime takes such an execution, and
// such an executor takes nothing else (selectorMatches).

// taskImageTx is the image a task is bound to ("" for none).
func taskImageTx(ctx context.Context, q queryer, taskScopeID string) (string, error) {
	var ref sql.NullString
	err := q.QueryRowContext(ctx, `SELECT image_ref FROM orchestration_tasks WHERE task_scope_id=$1`, taskScopeID).Scan(&ref)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return ref.String, err
}

// launchImageTx is the image an execution runs in ("" for none).
func launchImageTx(ctx context.Context, q queryer, executionID string) (string, error) {
	var ref sql.NullString
	err := q.QueryRowContext(ctx, `SELECT image_ref FROM execution_requests WHERE id=$1`, executionID).Scan(&ref)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return ref.String, err
}
