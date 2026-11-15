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
// id with room for it (one copy for now: copyTargetsTx, copyRoomTx), by an
// executor serving that store: copied locally
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
	// Month: the pack is a durable store's copy, under
	// <root>/objects/<month>; empty: <root>/packs (placements.go).
	Month string `json:"month,omitempty"`
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
	// Description: what the copy is to say of itself, written beside it
	// (descriptions.go); DescribeOnly: the copy is there and verified, only
	// its description is to be written (RecordDescribed).
	Description  *ObjectDescription `json:"description,omitempty"`
	DescribeOnly bool               `json:"describe_only,omitempty"`
	// Shed: the copy is to be removed, its store being below its reserve
	// (shed.go): claimed first (ClaimShed), then its files removed.
	Shed bool `json:"shed,omitempty"`
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
	// Described: its description was written beside it.
	Described bool `json:"described,omitempty"`
}

// PackRead is the pack a token lets an agent hand over: on StoreID at Root.
type PackRead struct {
	StoreID string `json:"store_id"`
	Root    string `json:"root"`
	// Month: a durable copy, under <root>/objects/<month> (a fetch's).
	Month string `json:"month,omitempty"`
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

// copyTargetsTx are the durable stores objects are copied to, in the order
// they are taken: the declared durable stores by id but those inside a WSL
// distro (copies are not made there yet) and those being moved.
func copyTargetsTx(ctx context.Context, q queryer, declared []machine.Store) ([]machine.Store, error) {
	var durable []machine.Store
	for _, s := range declared {
		if s.Class == machine.StoreDurable && s.MovedFrom == "" {
			durable = append(durable, s)
		}
	}
	sort.Slice(durable, func(i, j int) bool { return durable[i].ID < durable[j].ID })
	var out []machine.Store
	for _, s := range durable {
		if s.Executor != "" {
			info, err := executorInfoTx(ctx, q, s.Executor)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return nil, err
			}
			if err != nil || info.wsl {
				continue
			}
		}
		out = append(out, s)
	}
	return out, nil
}

// unknownRoom: the room of a durable store never reported usable (it is
// taken as having room: a copy there fails if it has none).
const unknownRoom = int64(1) << 62

// copyRoomTx is what each of stores can still take: its free bytes as last
// reported (usable), less its reserve_free_bytes and the packs of the copies
// being made there or verified there since that report (a report is minutes
// old; copies made meanwhile are not in it); unknownRoom for one never
// reported usable.
func copyRoomTx(ctx context.Context, q queryer, stores []machine.Store) (map[string]int64, error) {
	room := map[string]int64{}
	for _, s := range stores {
		var free sql.NullInt64
		var observed string
		err := q.QueryRowContext(ctx, `SELECT free_bytes,observed_at FROM store_observations WHERE store_id=$1 AND usable`, s.ID).Scan(&free, &observed)
		if errors.Is(err, sql.ErrNoRows) || err == nil && !free.Valid {
			room[s.ID] = unknownRoom
			continue
		}
		if err != nil {
			return nil, err
		}
		var copying int64
		if err = q.QueryRowContext(ctx, `SELECT COALESCE(SUM(d.pack_bytes),0)::bigint FROM object_copies c JOIN data_objects d ON d.id=c.object_id
			WHERE c.store_id=$1 AND (c.state='copying' OR c.state='verified' AND c.verified_at>$2)`, s.ID, observed).Scan(&copying); err != nil {
			return nil, err
		}
		room[s.ID] = free.Int64 - s.ReserveFreeBytes - copying
	}
	return room, nil
}

// CopiesToMake is what an executor is to copy now: the copies it has taken
// and not reported verified, after taking, when it serves the target durable
// store, every packed object without a copy yet. Each order carries where to
// read the pack: locally, or from its agent with a token issued now (the
// order's earlier token no longer works).
func (s *Store) CopiesToMake(ctx context.Context, executorID string) ([]CopyOrder, error) {
	tx, err := s.beginWrite(ctx)
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
	targets, err := copyTargetsTx(ctx, tx, declared)
	if err != nil {
		return nil, err
	}
	served := false
	for _, target := range targets {
		served = served || me.serves(target, executorID)
	}
	if served {
		room, err := copyRoomTx(ctx, tx, targets)
		if err != nil {
			return nil, err
		}
		var due []struct {
			id, created, projectID, node string
			bytes                        int64
		}
		err = eachRow(ctx, tx, `SELECT d.id,d.created_at,COALESCE(d.project_scope_id,''),COALESCE(d.pack_bytes,0),COALESCE(x.node_id,'') FROM data_objects d LEFT JOIN executors x ON x.id=d.executor_id
			WHERE d.state='packed' AND d.lifecycle='durable' AND NOT EXISTS(SELECT 1 FROM object_copies c WHERE c.object_id=d.id) ORDER BY d.packed_at,d.id`, nil, func(rows *sql.Rows) error {
			var d struct {
				id, created, projectID, node string
				bytes                        int64
			}
			if err := rows.Scan(&d.id, &d.created, &d.projectID, &d.bytes, &d.node); err != nil {
				return err
			}
			due = append(due, d)
			return nil
		})
		if err != nil {
			return nil, err
		}
		for _, d := range due {
			// the first durable store with room for it on the pack's node,
			// else the first elsewhere (read through the pack's agent);
			// another executor's to take when that one is not served here
			var target machine.Store
			found := false
			for _, local := range []bool{true, false} {
				for _, t := range targets {
					if (t.Node == d.node) == local && room[t.ID] >= d.bytes {
						target, found = t, true
						break
					}
				}
				if found {
					break
				}
			}
			if !found || !me.serves(target, executorID) {
				continue
			}
			month := d.created[0:4] + d.created[5:7]
			// Another executor serving the store may take it at the same
			// time: it is then that one's.
			result, err := tx.ExecContext(ctx, `INSERT INTO object_copies(object_id,store_id,executor_id,root,month,state,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'copying',$6,$7)
				ON CONFLICT(object_id,store_id) DO NOTHING`, d.id, target.ID, executorID, target.Root, month, t, t)
			if err != nil {
				return nil, err
			}
			if n, _ := result.RowsAffected(); n == 0 {
				continue
			}
			if room[target.ID] != unknownRoom {
				room[target.ID] -= d.bytes
			}
			var project *string
			if d.projectID != "" {
				project = &d.projectID
			}
			if err = appendCoordinationEventTx(ctx, tx, "copy_ordered", project, "object", d.id, nil, map[string]any{"store_id": target.ID, "executor_id": executorID}); err != nil {
				return nil, err
			}
		}
		if err = orderReplicasTx(ctx, tx, me, executorID, targets, room, t); err != nil {
			return nil, err
		}
	}
	// a durable copy this executor can read where it is, for a replica
	localCopies, err := localCopiesTx(ctx, tx, me, executorID, byID)
	if err != nil {
		return nil, err
	}
	type row struct {
		order                         CopyOrder
		sourceExecutor, sourceStoreID string
		sourceRoot                    string
		replica                       bool
	}
	var rows []row
	// an object nowhere durable yet first, then the replicas
	err = eachRow(ctx, tx, `SELECT c.object_id,c.store_id,c.root,c.month,d.pack_bytes,d.pack_sha256,d.manifest_sha256,d.executor_id,d.store_id,d.root,
		EXISTS(SELECT 1 FROM object_copies v WHERE v.object_id=c.object_id AND v.state='verified') AS replica
		FROM object_copies c JOIN data_objects d ON d.id=c.object_id WHERE c.executor_id=$1 AND c.state='copying' ORDER BY replica,c.created_at,c.object_id`, []any{executorID}, func(r *sql.Rows) error {
		var x row
		if err := r.Scan(&x.order.ObjectID, &x.order.StoreID, &x.order.Root, &x.order.Month, &x.order.PackBytes, &x.order.PackSHA256, &x.order.ManifestSHA256,
			&x.sourceExecutor, &x.sourceStoreID, &x.sourceRoot, &x.replica); err != nil {
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
		// Copied only to the store as it is still declared, and served here;
		// a copy taken for a store no longer so is given up (nothing of it was
		// verified), so the object is copied where the declaration says now.
		if d, ok := byID[x.order.StoreID]; !ok || d.Class != machine.StoreDurable || d.Root != x.order.Root || !me.serves(d, executorID) {
			if _, err = tx.ExecContext(ctx, `DELETE FROM object_copies WHERE object_id=$1 AND store_id=$2 AND executor_id=$3 AND state='copying'`, x.order.ObjectID, x.order.StoreID, executorID); err != nil {
				return nil, err
			}
			continue
		}
		o := x.order
		if src, ok := localCopies[o.ObjectID]; ok && x.replica && src.StoreID != o.StoreID {
			// read from a durable copy on this node: the pack may be gone
			o.Source = src
			out = append(out, o)
			continue
		}
		if x.replica {
			// else from one on another node, through its pack server
			src, node, found, err := remoteCopyTx(ctx, tx, me, executorID, o.ObjectID, byID)
			if err != nil {
				return nil, err
			}
			if found {
				token, hash, err := newSecret("kairo_pack_")
				if err != nil {
					return nil, err
				}
				if _, err = tx.ExecContext(ctx, `UPDATE object_copies SET token_hash=$1,token_expires_at=$2,source_store_id=$3,source_root=$4,source_month=$5,source_node_id=$6,updated_at=$7 WHERE object_id=$8 AND store_id=$9`,
					hash, expires, src.StoreID, src.Root, src.Month, node, t, o.ObjectID, o.StoreID); err != nil {
					return nil, err
				}
				src.Token = token
				o.Source = src
				out = append(out, o)
				continue
			}
		}
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
	ids := make([]string, len(out))
	for i, o := range out {
		ids[i] = o.ObjectID
	}
	descriptions, err := describeObjectsTx(ctx, tx, ids)
	if err != nil {
		return nil, err
	}
	for i := range out {
		if d, ok := descriptions[out[i].ObjectID]; ok {
			out[i].Description = &d
		}
	}
	describe, err := describeOrdersTx(ctx, tx, me, executorID)
	if err != nil {
		return nil, err
	}
	out = append(out, describe...)
	if served {
		room, err := copyRoomTx(ctx, tx, targets)
		if err != nil {
			return nil, err
		}
		shed, err := shedOrdersTx(ctx, tx, me, executorID, targets, room, byID)
		if err != nil {
			return nil, err
		}
		out = append(out, shed...)
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
		JOIN object_copies c ON c.object_id=d.id AND c.state='copying' AND c.token_hash=$1 AND c.source_store_id IS NULL
		WHERE d.id=$2 AND d.state='packed' AND x.node_id=$3`, hashToken(token), objectID, nodeID).Scan(&read.StoreID, &read.Root, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		// a replica's, read from a durable copy on this node (replicas.go)
		var month sql.NullString
		err = s.db.QueryRowContext(ctx, `SELECT c.source_store_id,c.source_root,c.source_month,c.token_expires_at FROM object_copies c
			WHERE c.object_id=$1 AND c.token_hash=$2 AND c.state='copying' AND c.source_node_id=$3`, objectID, hashToken(token), nodeID).Scan(&read.StoreID, &read.Root, &month, &expires)
		read.Month = month.String
	}
	if errors.Is(err, sql.ErrNoRows) {
		// a fetch's (placements.go): read from the store it was issued for,
		// on that node
		var month sql.NullString
		err = s.db.QueryRowContext(ctx, `SELECT p.source_store_id,p.source_root,p.source_month,p.token_expires_at FROM object_placements p
			WHERE p.object_id=$1 AND p.token_hash=$2 AND p.state='fetching' AND p.source_node_id=$3`, objectID, hashToken(token), nodeID).Scan(&read.StoreID, &read.Root, &month, &expires)
		read.Month = month.String
	}
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
	tx, err := s.beginWrite(ctx)
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
	t := now()
	var described *string
	if made.Described {
		described = &t
	}
	if state == "verified" {
		if described != nil {
			if _, err = tx.ExecContext(ctx, `UPDATE object_copies SET described_at=COALESCE(described_at,$1) WHERE object_id=$2 AND store_id=$3`, t, made.ObjectID, made.StoreID); err != nil {
				return err
			}
		}
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `UPDATE object_copies SET state='verified',path=$1,manifest_path=$2,token_hash=NULL,token_expires_at=NULL,verified_at=$3,updated_at=$4,described_at=$5 WHERE object_id=$6 AND store_id=$7`,
		made.Path, made.ManifestPath, t, t, described, made.ObjectID, made.StoreID); err != nil {
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
