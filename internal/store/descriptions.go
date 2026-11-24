package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// A durable copy describes itself: beside <object id>.tar.zst and its file
// list, <object id>.tar.zst.object.json (ObjectDescription) says whose it is
// and what it holds, so what a durable store keeps can be known, and the
// objects found again, from the store alone (kairo store catalogue; README:
// Rebuilding an installation from its durable store). A copy made now is
// described as it is written (CopyOrder.Description, CopyMade.Described); one
// made before is described afterwards (describe-only orders,
// RecordDescribed), a few at a time.

// DescriptionFormat is the first field of every description.
const DescriptionFormat = "kairo object 1"

// describeBatch bounds the describe-only orders one CopiesToMake hands out.
const describeBatch = 200

// ObjectDescription is what a durable copy says of itself.
type ObjectDescription struct {
	Format    string `json:"format"`
	ObjectID  string `json:"object_id"`
	Kind      string `json:"kind"`
	Lifecycle string `json:"lifecycle"`
	// Project and Name: the project it belongs to and the name it was given
	// (an import's), when it has them.
	Project *string `json:"project,omitempty"`
	Name    *string `json:"name,omitempty"`
	// Task: the task whose run made it (an output or a scratch's), as
	// project/queue/task.
	Task *string `json:"task,omitempty"`
	// SourceID and SourcePath: what was packed (a run, an attempt, an import)
	// and where it was, on Node, packed on store PackedOn.
	SourceID       string  `json:"source_id"`
	SourcePath     string  `json:"source_path"`
	Node           string  `json:"node"`
	PackedOn       string  `json:"packed_on"`
	Files          *int64  `json:"files,omitempty"`
	ContentBytes   *int64  `json:"content_bytes,omitempty"`
	PackBytes      int64   `json:"pack_bytes"`
	PackSHA256     string  `json:"pack_sha256"`
	ManifestSHA256 string  `json:"manifest_sha256"`
	CreatedAt      string  `json:"created_at"`
	PackedAt       *string `json:"packed_at,omitempty"`
}

// describeObjectsTx is the description of each of ids (those packed).
func describeObjectsTx(ctx context.Context, q queryer, ids []string) (map[string]ObjectDescription, error) {
	out := map[string]ObjectDescription{}
	if len(ids) == 0 {
		return out, nil
	}
	err := eachRow(ctx, q, `SELECT d.id,d.kind,d.lifecycle,p.external_key,d.name,
		(SELECT tp.external_key||'/'||tq.external_key||'/'||tt.external_key FROM execution_requests e
			JOIN coordination_scopes tp ON tp.id=e.project_scope_id JOIN coordination_scopes tq ON tq.id=e.queue_scope_id JOIN coordination_scopes tt ON tt.id=e.task_scope_id
			WHERE e.id=d.source_id OR e.id=(SELECT a.execution_id FROM attempts a WHERE a.id=d.source_id) LIMIT 1),
		d.source_id,d.source_path,x.node_id,d.store_id,d.files,d.content_bytes,COALESCE(d.pack_bytes,0),COALESCE(d.pack_sha256,''),COALESCE(d.manifest_sha256,''),d.created_at,d.packed_at
		FROM data_objects d JOIN executors x ON x.id=d.executor_id LEFT JOIN coordination_scopes p ON p.id=d.project_scope_id
		WHERE d.id=ANY($1::text[])`, []any{ids}, func(rows *sql.Rows) error {
		var o ObjectDescription
		var project, name, task, packedAt sql.NullString
		var files, content sql.NullInt64
		if err := rows.Scan(&o.ObjectID, &o.Kind, &o.Lifecycle, &project, &name, &task, &o.SourceID, &o.SourcePath, &o.Node, &o.PackedOn,
			&files, &content, &o.PackBytes, &o.PackSHA256, &o.ManifestSHA256, &o.CreatedAt, &packedAt); err != nil {
			return err
		}
		o.Format = DescriptionFormat
		o.Project, o.Name, o.Task, o.PackedAt = nullableString(project), nullableString(name), nullableString(task), nullableString(packedAt)
		if files.Valid {
			o.Files = &files.Int64
		}
		if content.Valid {
			o.ContentBytes = &content.Int64
		}
		out[o.ObjectID] = o
		return nil
	})
	return out, err
}

func nullableString(s sql.NullString) *string {
	if !s.Valid {
		return nil
	}
	return &s.String
}

// describeOrdersTx are describe-only orders (at most describeBatch) for the
// verified copies on durable stores executor me serves that are not described
// yet: the copy is there, its description is to be written beside it.
func describeOrdersTx(ctx context.Context, tx *sql.Tx, me executorInfo, executorID string) ([]CopyOrder, error) {
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return nil, err
	}
	var served []string
	roots := map[string]string{}
	for _, d := range declared {
		if d.Class == "durable" && d.MovedFrom == "" && me.serves(d, executorID) {
			served = append(served, d.ID)
			roots[d.ID] = d.Root
		}
	}
	if len(served) == 0 {
		return nil, nil
	}
	var orders []CopyOrder
	err = eachRow(ctx, tx, `SELECT c.object_id,c.store_id,c.root,c.month FROM object_copies c
		WHERE c.store_id=ANY($1::text[]) AND c.state='verified' AND c.described_at IS NULL
		AND NOT EXISTS(SELECT 1 FROM data_objects d WHERE d.id=c.object_id AND d.dropped_at IS NOT NULL) ORDER BY c.verified_at,c.object_id LIMIT $2`,
		[]any{served, describeBatch}, func(rows *sql.Rows) error {
			var o CopyOrder
			if err := rows.Scan(&o.ObjectID, &o.StoreID, &o.Root, &o.Month); err != nil {
				return err
			}
			if o.Root == roots[o.StoreID] {
				o.DescribeOnly = true
				orders = append(orders, o)
			}
			return nil
		})
	if err != nil || len(orders) == 0 {
		return nil, err
	}
	ids := make([]string, len(orders))
	for i, o := range orders {
		ids[i] = o.ObjectID
	}
	descriptions, err := describeObjectsTx(ctx, tx, ids)
	if err != nil {
		return nil, err
	}
	for i := range orders {
		if d, ok := descriptions[orders[i].ObjectID]; ok {
			orders[i].Description = &d
		}
	}
	return orders, nil
}

// RecordDescribed records that the verified copy of objectID on storeID,
// which executorID serves, is described beside itself now.
func (s *Store) RecordDescribed(ctx context.Context, executorID, objectID, storeID string) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	me, err := executorInfoTx(ctx, tx, executorID)
	if err != nil {
		return err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	ok := false
	for _, d := range declared {
		ok = ok || d.ID == storeID && d.Class == "durable" && me.serves(d, executorID)
	}
	if !ok {
		return fmt.Errorf("executor %s does not serve durable store %s", executorID, storeID)
	}
	result, err := tx.ExecContext(ctx, `UPDATE object_copies SET described_at=$1 WHERE object_id=$2 AND store_id=$3 AND state='verified'`, now(), objectID, storeID)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return errors.Join(ErrNotFound, fmt.Errorf("no verified copy of object %s on %s", objectID, storeID))
	}
	return tx.Commit()
}
