package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"unicode"

	"kairo/internal/id"
	"kairo/internal/machine"
)

// Existing data comes under Kairo by being imported (kairo data import): a
// directory on a node, outside every store of the node, becomes a data
// object of kind import (source_id the object itself, source_path the
// directory). The executor of the node that serves the node's first volatile
// store (by id; never a WSL executor) packs it there, into
// <root>/packs/<object id>.tar.zst as an output directory is packed, the
// directory reached through handles; it is then copied to a durable store
// and read back like every object. The directory is not Kairo's: it is only
// read, and deleted only when the import asked for it (delete_source), once
// a good copy exists, under the rules of every deletion (deletions.go): each
// regular file there must be in the pack with the size and mtime it was
// packed with and nothing else may be there but directories, else it is
// kept, and the executor deletes it only on the filesystem of the store the
// pack was made on (never a durable store's). An import without
// delete_source keeps the directory: once copied its pack goes and the
// source is settled kept.

// ObjectImport is the kind of an imported directory's object.
const ObjectImport = "import"

// ImportRequest asks for directories of a node to be imported into a
// project: each path becomes one object, named Name (with several paths,
// Name/<the directory's last component>; without Name, that component).
type ImportRequest struct {
	Project      string   `json:"project"`
	Node         string   `json:"node"`
	Paths        []string `json:"paths"`
	Name         string   `json:"name,omitempty"`
	DeleteSource bool     `json:"delete_source,omitempty"`
}

// ImportedObject is an object an import made: the directory, its name, the
// store it is packed on and the executor that packs it.
type ImportedObject struct {
	ObjectID   string `json:"object_id"`
	Name       string `json:"name"`
	Path       string `json:"path"`
	StoreID    string `json:"store_id"`
	ExecutorID string `json:"executor_id"`
}

// ErrImportRefused: an import that cannot be made as asked.
var ErrImportRefused = errors.New("import refused")

// maxImportPaths bounds the directories of one import.
const maxImportPaths = 1000

// checkDataName checks an object name: 1 to 200 characters, no control
// character, no space at either end.
func checkDataName(name string) error {
	switch {
	case name == "" || len(name) > 200:
		return fmt.Errorf("%w: a name is 1 to 200 characters, not %q", ErrImportRefused, name)
	case strings.ContainsFunc(name, unicode.IsControl):
		return fmt.Errorf("%w: name %q contains a control character", ErrImportRefused, name)
	case strings.TrimSpace(name) != name:
		return fmt.Errorf("%w: name %q starts or ends with a space", ErrImportRefused, name)
	}
	return nil
}

// importOwnership says why an imported directory is no longer to be packed
// or deleted as ordered ("" while it is): the store its pack is made on is
// still declared volatile on the node at the root, and the directory is
// inside no store of the node and holds none.
func importOwnership(declared []machine.Store, nodeID, storeID, root, dir string) string {
	var store *machine.Store
	for i := range declared {
		if declared[i].ID == storeID {
			store = &declared[i]
		}
	}
	switch {
	case store == nil:
		return "store " + storeID + " is no longer declared"
	case store.Class != machine.StoreVolatile:
		return "store " + storeID + " is no longer volatile"
	case store.Node != nodeID:
		return "store " + storeID + " is no longer on node " + nodeID
	case store.Root != root:
		return "store " + storeID + " has moved from " + root + " to " + store.Root
	case machine.CheckDataPath(dir) != nil:
		return dir + " is not a directory an import can name"
	}
	for _, other := range declared {
		if other.Node == nodeID && machine.RootsOverlap(dir, other.Root) {
			return dir + " overlaps store " + other.ID + " (" + other.Class + ", " + other.Root + ")"
		}
	}
	return ""
}

// driveOf is the drive letter of a drive path ("" for a POSIX one), folded.
func driveOf(p string) string {
	if len(p) >= 2 && p[1] == ':' {
		return strings.ToLower(p[:1])
	}
	return ""
}

// ImportData orders the directories of an import packed: each must be
// absolute in the namespace of the node's packing store (a drive path on a
// Windows store, a POSIX path on a Linux one), outside every store of the
// node and holding none, and not inside or around a directory being
// imported now (packed, or copied and its directory not yet deleted or
// kept). With DeleteSource a drive path must be on the packing store's
// drive (on Linux the executor checks the filesystem).
func (s *Store) ImportData(ctx context.Context, req ImportRequest) ([]ImportedObject, error) {
	if len(req.Paths) == 0 || len(req.Paths) > maxImportPaths {
		return nil, fmt.Errorf("%w: an import names 1 to %d directories, not %d", ErrImportRefused, maxImportPaths, len(req.Paths))
	}
	if req.Name != "" {
		if err := checkDataName(req.Name); err != nil {
			return nil, err
		}
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var projectID string
	err = tx.QueryRowContext(ctx, `SELECT id FROM coordination_scopes WHERE kind='project' AND external_key=$1`, req.Project).Scan(&projectID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: unknown project %q", ErrImportRefused, req.Project)
	}
	if err != nil {
		return nil, err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return nil, err
	}
	// the packing store and its executor
	var executorIDs []string
	err = eachRow(ctx, tx, `SELECT id FROM executors WHERE node_id=$1 ORDER BY id`, []any{req.Node}, func(rows *sql.Rows) error {
		var executorID string
		if err := rows.Scan(&executorID); err != nil {
			return err
		}
		executorIDs = append(executorIDs, executorID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	var volatile []machine.Store
	for _, d := range declared {
		if d.Node == req.Node && d.Class == machine.StoreVolatile {
			volatile = append(volatile, d)
		}
	}
	sort.Slice(volatile, func(i, j int) bool { return volatile[i].ID < volatile[j].ID })
	var packStore machine.Store
	packExecutor := ""
	for _, d := range volatile {
		for _, executorID := range executorIDs {
			info, err := executorInfoTx(ctx, tx, executorID)
			if err != nil {
				return nil, err
			}
			if !info.wsl && info.serves(d, executorID) {
				packStore, packExecutor = d, executorID
				break
			}
		}
		if packExecutor != "" {
			break
		}
	}
	if packExecutor == "" {
		return nil, fmt.Errorf("%w: node %q has no volatile store an executor packs on (none declared, or only WSL ones)", ErrImportRefused, req.Node)
	}
	// directories being imported on the node now
	var busy []struct{ objectID, dir string }
	err = eachRow(ctx, tx, `SELECT d.id,d.source_path FROM data_objects d JOIN executors x ON x.id=d.executor_id
		WHERE d.kind='import' AND x.node_id=$1 AND (d.state='packing' OR d.source_state IN('present','deleting'))`, []any{req.Node}, func(rows *sql.Rows) error {
		var b struct{ objectID, dir string }
		if err := rows.Scan(&b.objectID, &b.dir); err != nil {
			return err
		}
		busy = append(busy, b)
		return nil
	})
	if err != nil {
		return nil, err
	}
	posix := strings.HasPrefix(packStore.Root, "/")
	out := make([]ImportedObject, 0, len(req.Paths))
	t := now()
	for _, dir := range req.Paths {
		if err := machine.CheckDataPath(dir); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrImportRefused, err)
		}
		if strings.HasPrefix(dir, "/") != posix {
			return nil, fmt.Errorf("%w: %s is not a path of node %s's store %s (%s)", ErrImportRefused, dir, req.Node, packStore.ID, packStore.Root)
		}
		if req.DeleteSource && !posix && driveOf(dir) != driveOf(packStore.Root) {
			return nil, fmt.Errorf("%w: %s is deleted once copied only on the drive of store %s (%s)", ErrImportRefused, dir, packStore.ID, packStore.Root)
		}
		if why := importOwnership(declared, req.Node, packStore.ID, packStore.Root, dir); why != "" {
			return nil, fmt.Errorf("%w: %s", ErrImportRefused, why)
		}
		for _, b := range busy {
			if machine.RootsOverlap(dir, b.dir) {
				return nil, fmt.Errorf("%w: %s overlaps %s, being imported as %s", ErrImportRefused, dir, b.dir, b.objectID)
			}
		}
		base := path.Base(machine.StoreRootKey(dir))
		name := base
		switch {
		case req.Name != "" && len(req.Paths) == 1:
			name = req.Name
		case req.Name != "":
			name = req.Name + "/" + base
		}
		if err := checkDataName(name); err != nil {
			return nil, err
		}
		objectID := id.New("obj")
		if _, err = tx.ExecContext(ctx, `INSERT INTO data_objects(id,kind,source_id,store_id,executor_id,root,source_path,project_scope_id,state,name,delete_source,created_at,updated_at)
			VALUES($1,'import',$1,$2,$3,$4,$5,$6,'packing',$7,$8,$9,$10)`, objectID, packStore.ID, packExecutor, packStore.Root, dir, projectID, name, req.DeleteSource, t, t); err != nil {
			return nil, err
		}
		if err = appendCoordinationEventTx(ctx, tx, "import_ordered", &projectID, "object", objectID, nil, map[string]any{"node": req.Node, "path": dir, "name": name,
			"store_id": packStore.ID, "executor_id": packExecutor, "delete_source": req.DeleteSource}); err != nil {
			return nil, err
		}
		busy = append(busy, struct{ objectID, dir string }{objectID, dir})
		out = append(out, ImportedObject{ObjectID: objectID, Name: name, Path: dir, StoreID: packStore.ID, ExecutorID: packExecutor})
	}
	return out, tx.Commit()
}

// DataObject is a data object as kairo data list shows it: what it was made
// from (a run's output directory, a kept scratch, an imported directory),
// its pack, its verified copies and what became of its source.
type DataObject struct {
	ID           string  `json:"id"`
	Kind         string  `json:"kind"`
	Name         *string `json:"name,omitempty"`
	Project      *string `json:"project,omitempty"`
	Node         string  `json:"node"`
	StoreID      string  `json:"store_id"`
	SourceID     string  `json:"source_id"`
	SourcePath   string  `json:"source_path"`
	State        string  `json:"state"`
	SourceState  string  `json:"source_state"`
	SourceReason *string `json:"source_reason,omitempty"`
	DeleteSource bool    `json:"delete_source"`
	Files        *int64  `json:"files,omitempty"`
	ContentBytes *int64  `json:"content_bytes,omitempty"`
	PackBytes    *int64  `json:"pack_bytes,omitempty"`
	Copies       int     `json:"copies"`
	CopyPath     *string `json:"copy_path,omitempty"`
	CreatedAt    string  `json:"created_at"`
	PackedAt     *string `json:"packed_at,omitempty"`
}

// DataObjectFilter narrows kairo data list: a project, a kind, and at most
// Limit objects (newest first; 0: 200).
type DataObjectFilter struct {
	Project string
	Kind    string
	Limit   int
}

// DataObjects lists data objects, newest first.
func (s *Store) DataObjects(ctx context.Context, filter DataObjectFilter) ([]DataObject, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 200
	}
	out := []DataObject{}
	err := eachRow(ctx, s.db, `SELECT d.id,d.kind,d.name,p.external_key,x.node_id,d.store_id,d.source_id,d.source_path,d.state,d.source_state,d.source_reason,d.delete_source,
			d.files,d.content_bytes,d.pack_bytes,d.created_at,d.packed_at,
			(SELECT COUNT(*) FROM object_copies c WHERE c.object_id=d.id AND c.state='verified' AND c.check_problem IS NULL),
			(SELECT c.path FROM object_copies c WHERE c.object_id=d.id AND c.state='verified' AND c.check_problem IS NULL ORDER BY c.store_id LIMIT 1)
		FROM data_objects d JOIN executors x ON x.id=d.executor_id LEFT JOIN coordination_scopes p ON p.id=d.project_scope_id
		WHERE ($1='' OR p.external_key=$1) AND ($2='' OR d.kind=$2) ORDER BY d.created_at DESC,d.id DESC LIMIT $3`, []any{filter.Project, filter.Kind, limit}, func(rows *sql.Rows) error {
		var o DataObject
		var name, project, reason, packedAt, copyPath sql.NullString
		var files, content, pack sql.NullInt64
		if err := rows.Scan(&o.ID, &o.Kind, &name, &project, &o.Node, &o.StoreID, &o.SourceID, &o.SourcePath, &o.State, &o.SourceState, &reason, &o.DeleteSource,
			&files, &content, &pack, &o.CreatedAt, &packedAt, &o.Copies, &copyPath); err != nil {
			return err
		}
		o.Name, o.Project, o.SourceReason, o.PackedAt, o.CopyPath = stringPtr(name), stringPtr(project), stringPtr(reason), stringPtr(packedAt), stringPtr(copyPath)
		o.Files, o.ContentBytes, o.PackBytes = int64Ptr(files), int64Ptr(content), int64Ptr(pack)
		out = append(out, o)
		return nil
	})
	return out, err
}

func stringPtr(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	return &v.String
}

func int64Ptr(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	return &v.Int64
}
