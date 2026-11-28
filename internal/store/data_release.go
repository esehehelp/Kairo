package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Releasing a kept source (kairo data release): once its copy is verified, a
// run's output directory or a failed attempt's scratch whose deletion Kairo
// refused for safety (source kept: it held what its pack does not, a link or
// a socket say, or its pack's file list was gone) stays where it is for good.
// An operator who accepts losing what the pack does not hold releases it.
//
// A released source with a good copy (goodCopyTx) whose store the
// declaration still makes Kairo's goes back to be deleted (source present
// again, source_reason releasedPrefix): its deletion order says Released, so
// its executor removes it as any source (through handles, its marker checked,
// no link followed) without checking it against its pack. Work of an
// executor disabled since goes to the one serving its store now
// (handOverRetiredDataTx). One whose store the declaration no longer makes
// Kairo's, on a node that is retired (disabled), has nothing left that Kairo
// could delete: it is settled deleted at once. Anything else is refused with
// why.

// releasedPrefix begins the source_reason of a released source.
const releasedPrefix = "released by "

// ReleaseRequest selects kept sources to release: objects by id, or every
// kept source of a run output or a scratch (All). Without Confirm nothing
// changes: the answer says what would be released.
type ReleaseRequest struct {
	Objects []string `json:"objects,omitempty"`
	All     bool     `json:"all,omitempty"`
	Confirm bool     `json:"confirm,omitempty"`
}

// ReleasedSource is a kept source a release selected: released (to be
// deleted by its executor), settled deleted at once (Gone: nothing is left
// to delete), or refused with why.
type ReleasedSource struct {
	ObjectID string `json:"object_id"`
	Kind     string `json:"kind"`
	SourceID string `json:"source_id"`
	StoreID  string `json:"store_id"`
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
	// Kept is why the source was kept.
	Kept     string `json:"kept"`
	Released bool   `json:"released,omitempty"`
	Gone     bool   `json:"gone,omitempty"`
	Refused  string `json:"refused,omitempty"`
}

// ErrReleaseRefused: a release that selects nothing, or names what is not a
// kept source.
var ErrReleaseRefused = errors.New("source not released")

// ReleaseSources releases the kept sources req selects (or says what it
// would release).
func (s *Store) ReleaseSources(ctx context.Context, req ReleaseRequest, actor string) ([]ReleasedSource, error) {
	if len(req.Objects) == 0 && !req.All {
		return nil, fmt.Errorf("%w: name the objects, or all", ErrReleaseRefused)
	}
	if actor == "" {
		actor = "an operator"
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return nil, err
	}
	type row struct {
		ReleasedSource
		root, state, sourceState, nodeID string
		executorEnabled, nodeEnabled     bool
		projectID                        sql.NullString
	}
	var rows []row
	query := `SELECT d.id,d.kind,d.source_id,d.store_id,d.root,d.source_path,COALESCE(d.content_bytes,0),COALESCE(d.source_reason,''),d.state,d.source_state,
		COALESCE(x.node_id,''),COALESCE(x.enabled,false),COALESCE(n.enabled,false),d.project_scope_id
		FROM data_objects d LEFT JOIN executors x ON x.id=d.executor_id LEFT JOIN nodes n ON n.id=x.node_id`
	args := []any{}
	if req.All {
		query += ` WHERE d.source_state='kept' AND d.state='packed' AND d.kind IN('output','scratch')`
	} else {
		query += ` WHERE d.id=ANY($1::text[])`
		args = append(args, req.Objects)
	}
	err = eachRow(ctx, tx, query+` ORDER BY d.store_id,d.id FOR UPDATE OF d`, args, func(r *sql.Rows) error {
		var x row
		if err := r.Scan(&x.ObjectID, &x.Kind, &x.SourceID, &x.StoreID, &x.root, &x.Path, &x.Bytes, &x.Kept, &x.state, &x.sourceState,
			&x.nodeID, &x.executorEnabled, &x.nodeEnabled, &x.projectID); err != nil {
			return err
		}
		rows = append(rows, x)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !req.All && len(rows) != len(uniqueStrings(req.Objects)) {
		found := map[string]bool{}
		for _, x := range rows {
			found[x.ObjectID] = true
		}
		for _, id := range req.Objects {
			if !found[id] {
				return nil, fmt.Errorf("%w: no object %s", ErrReleaseRefused, id)
			}
		}
	}
	t := now()
	out := make([]ReleasedSource, 0, len(rows))
	handOver := map[string]bool{}
	for _, x := range rows {
		r := x.ReleasedSource
		good, err := goodCopyTx(ctx, tx, declared, x.ObjectID)
		if err != nil {
			return nil, err
		}
		why := sourceOwnership(declared, x.Kind, x.nodeID, x.StoreID, x.root, x.Path, x.SourceID)
		switch {
		case x.Kind != ObjectOutput && x.Kind != ObjectScratch:
			r.Refused = "only a run's output directory or a scratch is released (an import's directory is its owner's)"
		case x.state != "packed" || x.sourceState != SourceKept:
			r.Refused = fmt.Sprintf("its source is %s, not kept", x.sourceState)
		case !good:
			r.Refused = "it has no good copy on a durable store as declared"
		case why != "" && x.nodeEnabled:
			r.Refused = why + ", and its node is not retired: declare the store again, or retire the node"
		case why != "":
			r.Gone = true
		default:
			r.Released = true
		}
		out = append(out, r)
		if !req.Confirm || r.Refused != "" {
			continue
		}
		reason := releasedPrefix + actor + " (kept: " + strings.TrimPrefix(x.Kept, "not deleted: ") + ")"
		var project *string
		if x.projectID.Valid {
			project = &x.projectID.String
		}
		if r.Gone {
			// nothing Kairo could delete is left: settled as deleted
			if err := checkTransition(recSource, SourceKept, SourceDeleted); err != nil {
				return nil, err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET source_state='deleted',source_reason=$1,source_deleted_at=$2,updated_at=$2 WHERE id=$3`, reason+"; its node is retired", t, x.ObjectID); err != nil {
				return nil, err
			}
			switch x.Kind {
			case ObjectScratch:
				if err := checkRowsTransitionTx(ctx, tx, recScratch, "attempt_scratch", "attempt_id=$1", "deleted", x.SourceID); err != nil {
					return nil, err
				}
				_, err = tx.ExecContext(ctx, `UPDATE attempt_scratch SET state='deleted',bytes=0,reason=COALESCE(reason,'')||'; released: its node is retired',updated_at=$1 WHERE attempt_id=$2`, t, x.SourceID)
			case ObjectOutput:
				if err := checkRowsTransitionTx(ctx, tx, recRunOutput, "run_outputs", "run_id=$1 AND store_id=$2", "deleted", x.SourceID, x.StoreID); err != nil {
					return nil, err
				}
				_, err = tx.ExecContext(ctx, `UPDATE run_outputs SET state='deleted',bytes=0,updated_at=$1 WHERE run_id=$2 AND store_id=$3`, t, x.SourceID, x.StoreID)
			}
			if err != nil {
				return nil, err
			}
			if err = appendCoordinationEventTx(ctx, tx, "source_released", project, "object", x.ObjectID, nil, map[string]any{"kind": x.Kind, "source_id": x.SourceID, "store_id": x.StoreID, "path": x.Path, "actor": actor, "gone": true}); err != nil {
				return nil, err
			}
			continue
		}
		if err := checkTransition(recSource, SourceKept, SourcePresent); err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE data_objects SET source_state='present',source_reason=$1,updated_at=$2 WHERE id=$3`, reason, t, x.ObjectID); err != nil {
			return nil, err
		}
		if err = appendCoordinationEventTx(ctx, tx, "source_released", project, "object", x.ObjectID, nil, map[string]any{"kind": x.Kind, "source_id": x.SourceID, "store_id": x.StoreID, "path": x.Path, "actor": actor}); err != nil {
			return nil, err
		}
		if !x.executorEnabled && x.nodeID != "" {
			handOver[x.nodeID] = true
		}
	}
	for nodeID := range handOver {
		if _, err := handOverRetiredDataTx(ctx, tx, nodeID); err != nil {
			return nil, err
		}
	}
	if !req.Confirm {
		return out, nil
	}
	return out, tx.Commit()
}

// uniqueStrings is ids without repeats.
func uniqueStrings(ids []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}
