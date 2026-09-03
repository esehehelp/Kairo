package store

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func TestOpenMigratesV1WithoutDigestUniquenessAndBackfillsCapacityResources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
INSERT INTO schema_migrations VALUES(1,'now');
CREATE TABLE queues(id TEXT PRIMARY KEY, project TEXT NOT NULL, name TEXT NOT NULL, current_revision INTEGER NOT NULL DEFAULT 0, desired_state TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE queue_revisions(queue_id TEXT NOT NULL REFERENCES queues(id), revision INTEGER NOT NULL, digest TEXT NOT NULL, normalized_json TEXT NOT NULL, source_text TEXT NOT NULL, actor TEXT NOT NULL, request_id TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(queue_id,revision), UNIQUE(queue_id,digest), UNIQUE(queue_id,request_id));
CREATE TABLE executors(id TEXT PRIMARY KEY, node_id TEXT NOT NULL);
CREATE TABLE resource_instances(id TEXT PRIMARY KEY, node_id TEXT NOT NULL, kind TEXT NOT NULL, stable_identity TEXT NOT NULL);
CREATE TABLE leases(id TEXT PRIMARY KEY, executor_id TEXT NOT NULL, state TEXT NOT NULL);
CREATE TABLE lease_items(lease_id TEXT NOT NULL, resource_id TEXT, kind TEXT NOT NULL, quantity INTEGER NOT NULL, filesystem TEXT NOT NULL DEFAULT '', PRIMARY KEY(lease_id,kind,resource_id,filesystem));
CREATE TABLE attempts(id TEXT PRIMARY KEY, task_id TEXT, workload_id TEXT, ordinal INTEGER NOT NULL);
CREATE TABLE commands(id TEXT PRIMARY KEY, task_id TEXT, workload_id TEXT, attempt_id TEXT NOT NULL);
INSERT INTO attempts VALUES('attempt-v1','task-v1','workload-v1',1);
INSERT INTO commands VALUES('command-v1','task-v1','workload-v1','attempt-v1');
CREATE TRIGGER exclusive_resource_lease_guard BEFORE INSERT ON lease_items WHEN NEW.resource_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'old trigger'); END;
INSERT INTO executors VALUES('e','n');
INSERT INTO resource_instances VALUES('n-cpu','n','cpu','logical-cpu');
INSERT INTO leases VALUES('lease','e','stale');
INSERT INTO lease_items VALUES('lease',NULL,'cpu',1000,'');
`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var version int
	if err = st.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != 2 {
		t.Fatalf("migration version: %d %v", version, err)
	}
	var resourceID string
	if err = st.db.QueryRow(`SELECT resource_id FROM lease_items WHERE lease_id='lease'`).Scan(&resourceID); err != nil || resourceID != "n-cpu" {
		t.Fatalf("capacity resource backfill: %q %v", resourceID, err)
	}
	var workloadID string
	if err = st.db.QueryRow(`SELECT workload_id FROM attempts WHERE id='attempt-v1'`).Scan(&workloadID); err != nil || workloadID != "workload-v1" {
		t.Fatalf("existing attempts data changed: %q %v", workloadID, err)
	}
	if err = st.db.QueryRow(`SELECT workload_id FROM commands WHERE id='command-v1'`).Scan(&workloadID); err != nil || workloadID != "workload-v1" {
		t.Fatalf("existing commands data changed: %q %v", workloadID, err)
	}
	if _, err = st.db.Exec(`INSERT INTO queues(id,project,name,current_revision,desired_state,created_at,updated_at) VALUES('q','p','q',2,'active','now','now')`); err != nil {
		t.Fatal(err)
	}
	for revision, request := range []string{"one", "two"} {
		if _, err = st.db.Exec(`INSERT INTO queue_revisions(queue_id,revision,digest,normalized_json,source_text,actor,request_id,created_at) VALUES('q',?,'same','{}','source','test',?,'now')`, revision+1, request); err != nil {
			t.Fatalf("duplicate digest revision %d: %v", revision+1, err)
		}
	}
}

func TestFreshSchemaExcludesLegacyLifecycleTablesAndIndex(t *testing.T) {
	st := openTestStore(t)
	for _, name := range []string{"resources", "workloads"} {
		var count int
		if err := st.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("fresh schema contains legacy table %s", name)
		}
	}
	var count int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='attempts_workload_idx'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("fresh schema contains attempts_workload_idx")
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "kairo.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}
