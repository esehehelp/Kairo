package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"kairo/internal/machine"
)

// A packed data object is copied to a durable store, the first declared by
// id (one copy for now), by an executor serving that store: copied locally
// when the pack is on a store of the same node in its namespace, else
// fetched from the agent of the pack's node (the address its executor
// advertises as pack_addr) with a token the daemon issued for that copy.
// The copy goes to <root>/objects/<YYYYMM>/<object id>.tar.zst with its file
// list beside it, and is verified only once read back from disk (around the
// cache) and found to match the pack's size and sha256 and the list's
// sha256. Kairo never deletes from a durable store what it keeps there.

// copyTokenLife is how long a copy's token can be used: as long as a copying
// round may last.
const copyTokenLife = 12 * time.Hour

// CopySource is where a copy's pack is read: on a store of this node in
// this executor's namespace (Local), or from Addr (the pack's agent) with
// Token.
type CopySource struct {
	Local   bool   `json:"local,omitempty"`
	StoreID string `json:"store_id"`
	Root    string `json:"root"`
	Addr    string `json:"addr,omitempty"`
	Token   string `json:"token,omitempty"`
}

// CopyOrder is a pack its executor is to copy to the durable store StoreID
// at Root, under Month, with what the copy must match.
type CopyOrder struct {
	ObjectID       string     `json:"object_id"`
	StoreID        string     `json:"store_id"`
	Root           string     `json:"root"`
	Month          string     `json:"month"`
	PackBytes      int64      `json:"pack_bytes"`
	PackSHA256     string     `json:"pack_sha256"`
	ManifestSHA256 string     `json:"manifest_sha256"`
	Source         CopySource `json:"source"`
}

// CopyMade is a copy an executor wrote and read back: where, its size and
// the sha256 of its pack and file list as read from disk.
type CopyMade struct {
	ObjectID       string `json:"object_id"`
	StoreID        string `json:"store_id"`
	ExecutorID     string `json:"executor_id"`
	Path           string `json:"path"`
	ManifestPath   string `json:"manifest_path"`
	Bytes          int64  `json:"bytes"`
	SHA256         string `json:"sha256"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

// PackRead is the pack a token lets an agent hand over: on StoreID at Root.
type PackRead struct {
	StoreID string `json:"store_id"`
	Root    string `json:"root"`
}

// ErrCopyMismatch: a copy read back does not match its pack.
var ErrCopyMismatch = errors.New("the copy does not match the pack")

type executorInfo struct {
	nodeID string
	wsl    bool
	attrs  map[string]string
}

func executorInfoTx(ctx context.Context, q queryer, executorID string) (executorInfo, error) {
	var info executorInfo
	var kind, attrs string
	if err := q.QueryRowContext(ctx, `SELECT node_id,kind,attributes_json FROM executors WHERE id=$1`, executorID).Scan(&info.nodeID, &kind, &attrs); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return info, ErrNotFound
		}
		return info, err
	}
	_ = json.Unmarshal([]byte(attrs), &info.attrs)
	info.wsl = kind == "wsl2" || info.attrs["environment"] == "wsl2"
	return info, nil
}

// serves reports whether an executor serves a store: one of its node, naming
// it, or naming no executor while the executor is not a WSL one.
func (info executorInfo) serves(s machine.Store, executorID string) bool {
	return s.Node == info.nodeID && (s.Executor == executorID || s.Executor == "" && !info.wsl)
}

// copyTarget is the durable store objects are copied to: the first declared
// durable store by id.
func copyTarget(declared []machine.Store) (machine.Store, bool) {
	var durable []machine.Store
	for _, s := range declared {
		if s.Class == machine.StoreDurable {
			durable = append(durable, s)
		}
	}
	if len(durable) == 0 {
		return machine.Store{}, false
	}
	sort.Slice(durable, func(i, j int) bool { return durable[i].ID < durable[j].ID })
	return durable[0], true
}

// CopiesToMake is what an executor is to copy now: the copies it has taken
// and not reported verified, after taking, when it serves the target durable
// store, every packed object without a copy yet. Each order carries where to
// read the pack: locally, or from its agent with a token issued now (the
// order's earlier token no longer works).
func (s *Store) CopiesToMake(ctx context.Context, executorID string) ([]CopyOrder, error) {
	tx, err := s.beginUnlockedWrite(ctx)
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
	byID := map[string]machine.Store{}
	for _, d := range declared {
		byID[d.ID] = d
	}
	t := now()
	if target, ok := copyTarget(declared); ok && me.serves(target, executorID) {
		var due []struct{ id, created, projectID string }
		err = eachRow(ctx, tx, `SELECT d.id,d.created_at,COALESCE(d.project_scope_id,'') FROM data_objects d
			WHERE d.state='packed' AND NOT EXISTS(SELECT 1 FROM object_copies c WHERE c.object_id=d.id) ORDER BY d.packed_at,d.id`, nil, func(rows *sql.Rows) error {
			var d struct{ id, created, projectID string }
			if err := rows.Scan(&d.id, &d.created, &d.projectID); err != nil {
				return err
			}
			due = append(due, d)
			return nil
		})
		if err != nil {
			return nil, err
		}
		for _, d := range due {
			month := d.created[0:4] + d.created[5:7]
			if _, err = tx.ExecContext(ctx, `INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'copying',$6,$7)`,
				d.id, target.ID, executorID, target.Root, month, t, t); err != nil {
				return nil, err
			}
			var project *string
			if d.projectID != "" {
				project = &d.projectID
			}
			if err = appendCoordinationEventTx(ctx, tx, "copy_ordered", project, "object", d.id, nil, map[string]any{"store_id": target.ID, "executor_id": executorID}); err != nil {
				return nil, err
			}
		}
	}
	type row struct {
		order                         CopyOrder
		sourceExecutor, sourceStoreID string
		sourceRoot                    string
	}
	var rows []row
	err = eachRow(ctx, tx, `SELECT c.object_id,c.store_id,c.root,c.month,d.pack_bytes,d.pack_sha256,d.manifest_sha256,d.executor_id,d.store_id,d.root
		FROM object_copies c JOIN data_objects d ON d.id=c.object_id WHERE c.executor_id=$1 AND c.state='copying' ORDER BY c.created_at,c.object_id`, []any{executorID}, func(r *sql.Rows) error {
		var x row
		if err := r.Scan(&x.order.ObjectID, &x.order.StoreID, &x.order.Root, &x.order.Month, &x.order.PackBytes, &x.order.PackSHA256, &x.order.ManifestSHA256,
			&x.sourceExecutor, &x.sourceStoreID, &x.sourceRoot); err != nil {
			return err
		}
		rows = append(rows, x)
		return nil
	})
	if err != nil {
		return nil, err
	}
	expires := time.Now().UTC().Add(copyTokenLife).Format(time.RFC3339Nano)
	out := []CopyOrder{}
	for _, x := range rows {
		// Copied only to the store as it is still declared, and served here.
		if d, ok := byID[x.order.StoreID]; !ok || d.Class != machine.StoreDurable || d.Root != x.order.Root || !me.serves(d, executorID) {
			continue
		}
		o := x.order
		o.Source = CopySource{StoreID: x.sourceStoreID, Root: x.sourceRoot}
		source, err := executorInfoTx(ctx, tx, x.sourceExecutor)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if src, declared := byID[x.sourceStoreID]; err == nil && declared && src.Root == x.sourceRoot && me.serves(src, executorID) {
			o.Source.Local = true
		} else {
			o.Source.Addr = source.attrs["pack_addr"]
			token, hash, err := newSecret("kairo_pack_")
			if err != nil {
				return nil, err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE object_copies SET token_hash=$1,token_expires_at=$2,updated_at=$3 WHERE object_id=$4 AND store_id=$5`,
				hash, expires, t, o.ObjectID, o.StoreID); err != nil {
				return nil, err
			}
			o.Source.Token = token
		}
		out = append(out, o)
	}
	return out, tx.Commit()
}

// AuthorizePackRead is the pack a copy's token lets the agent of node hand
// over: the object's, packed by an executor of that node, for a copy being
// made whose token it is and has not expired. ErrNotFound otherwise, saying
// nothing of which.
func (s *Store) AuthorizePackRead(ctx context.Context, nodeID, objectID, token string) (PackRead, error) {
	var read PackRead
	var expires string
	err := s.db.QueryRowContext(ctx, `SELECT d.store_id,d.root,c.token_expires_at FROM data_objects d JOIN executors x ON x.id=d.executor_id
		JOIN object_copies c ON c.object_id=d.id AND c.state='copying' AND c.token_hash=$1
		WHERE d.id=$2 AND d.state='packed' AND x.node_id=$3`, hashToken(token), objectID, nodeID).Scan(&read.StoreID, &read.Root, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return read, ErrNotFound
	}
	if err != nil {
		return read, err
	}
	if at, parseErr := time.Parse(time.RFC3339Nano, expires); parseErr != nil || !time.Now().Before(at) {
		return read, ErrNotFound
	}
	return read, nil
}

// RecordCopy records a copy its executor wrote and read back: at
// <root>/objects/<month>/<object id>.tar.zst with its list beside it, of the
// pack's size and sha256 and the list's sha256 (ErrCopyMismatch otherwise).
// Recording it again is no error.
func (s *Store) RecordCopy(ctx context.Context, made CopyMade) error {
	tx, err := s.beginUnlockedWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var executorID, root, month, state, packSHA, listSHA string
	var packBytes int64
	var projectID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT c.executor_id,c.root,c.month,c.state,d.pack_bytes,d.pack_sha256,d.manifest_sha256,d.project_scope_id
		FROM object_copies c JOIN data_objects d ON d.id=c.object_id WHERE c.object_id=$1 AND c.store_id=$2 FOR UPDATE OF c`, made.ObjectID, made.StoreID).
		Scan(&executorID, &root, &month, &state, &packBytes, &packSHA, &listSHA, &projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	switch {
	case executorID != made.ExecutorID:
		return fmt.Errorf("the copy of object %s to %s is executor %s's, not %s's", made.ObjectID, made.StoreID, executorID, made.ExecutorID)
	case !machine.IsObjectPath(made.Path, root, month, made.ObjectID, ""):
		return fmt.Errorf("copy %s of object %s is not <root>/objects/%s/<object id>.tar.zst under %s", made.Path, made.ObjectID, month, root)
	case !machine.IsObjectPath(made.ManifestPath, root, month, made.ObjectID, ".files.tsv"):
		return fmt.Errorf("file list %s of object %s is not beside its copy", made.ManifestPath, made.ObjectID)
	case made.Bytes != packBytes || made.SHA256 != packSHA || made.ManifestSHA256 != listSHA:
		return fmt.Errorf("%w: object %s copied as %d bytes, sha256 %s (list %s); packed as %d bytes, sha256 %s (list %s)",
			ErrCopyMismatch, made.ObjectID, made.Bytes, made.SHA256, made.ManifestSHA256, packBytes, packSHA, listSHA)
	}
	if state == "verified" {
		return tx.Commit()
	}
	t := now()
	if _, err = tx.ExecContext(ctx, `UPDATE object_copies SET state='verified',path=$1,manifest_path=$2,token_hash=NULL,token_expires_at=NULL,verified_at=$3,updated_at=$4 WHERE object_id=$5 AND store_id=$6`,
		made.Path, made.ManifestPath, t, t, made.ObjectID, made.StoreID); err != nil {
		return err
	}
	var project *string
	if projectID.Valid {
		project = &projectID.String
	}
	if err = appendCoordinationEventTx(ctx, tx, "copy_verified", project, "object", made.ObjectID, nil, map[string]any{"store_id": made.StoreID, "path": made.Path, "bytes": made.Bytes, "sha256": made.SHA256}); err != nil {
		return err
	}
	return tx.Commit()
}
