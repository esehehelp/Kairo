package sqliteimport

import (
	"context"
	"database/sql"
	_ "embed"
	"path/filepath"
	"testing"

	"kairo/internal/pgtest"
	"kairo/internal/store"
)

//go:embed testdata/sqlite_v3_schema.sql
var sqliteSchema string

// sqliteFixture creates a SQLite Kairo database with a few rows of the kinds
// the copy must get right: flags, NULLs, reals, a scope inserted before its
// parent, a GPU lease item next to a live lease of the same GPU, a capacity
// item without a resource, and ids deleted from the top of a table.
func sqliteFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kairo-v3.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(sqliteSchema); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`INSERT INTO schema_migrations VALUES(3,'2026-01-01T00:00:00Z')`,
		`INSERT INTO orchestration_schema_migrations VALUES(1,'2026-01-01T00:00:00Z')`,
		`INSERT INTO nodes VALUES('n1','n1','linux','amd64','{}',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		`INSERT INTO executors VALUES('x1','n1','linux','{"pool":"all"}',0,NULL,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		`INSERT INTO resource_providers VALUES('p1','n1','nvidia','{}',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		`INSERT INTO resource_instances VALUES('gpu-1','n1','p1','gpu','GPU-1','{}','{}','enabled',NULL,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		// a task scope before its queue and project
		`INSERT INTO coordination_scopes VALUES('s-task','task','s-queue','train','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		`INSERT INTO coordination_scopes VALUES('s-queue','queue','s-project','q','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		`INSERT INTO coordination_scopes VALUES('s-project','project',NULL,'proj','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		`INSERT INTO execution_requests(id,project_scope_id,queue_scope_id,task_scope_id,client_request_id,spec_digest,state,argv_json,cwd,priority,checkpointable,preemptible,submitted_at)
		 VALUES('e1','s-project','s-queue','s-task','r1','d1','started','["run"]','/w',5,1,1,'2026-01-01T00:00:00Z'),
		 ('e2','s-project','s-queue','s-task','r2','d2','terminal','["run"]','/w',0,0,0,'2026-01-01T00:00:00Z')`,
		`INSERT INTO leases(id,execution_id,executor_id,state,created_at) VALUES('lease-b','e2','x1','released','2026-01-01T00:00:00Z')`,
		`INSERT INTO lease_items VALUES('lease-b','gpu-1','gpu',1,'',1)`,
		`INSERT INTO lease_items VALUES('lease-b',NULL,'cpu',1000,'',1)`,
		`INSERT INTO leases(id,execution_id,executor_id,state,created_at) VALUES('lease-a','e1','x1','active','2026-01-02T00:00:00Z')`,
		`INSERT INTO lease_items VALUES('lease-a','gpu-1','gpu',1,'',0)`,
		`INSERT INTO lease_items VALUES('lease-a',NULL,'cpu',1000,'',0)`,
		`INSERT INTO resource_observations(resource_id,observed_at,valid_until,total_bytes,free_bytes,utilization,temperature_c) VALUES('gpu-1','a','b',24,23,0.1,NULL),('gpu-1','c','d',24,22,1e-300,65.5)`,
		`INSERT INTO resource_observations(id,resource_id,observed_at,valid_until) VALUES(10,'gpu-1','e','f')`,
		`DELETE FROM resource_observations WHERE id=10`,
		`INSERT INTO attempts(id,execution_id,state,executor_id) VALUES('a1','e1','running','x1')`,
		`INSERT INTO attempt_energy(attempt_id,gpu_joules,partial,updated_at) VALUES('a1',12.25,1,'2026-01-01T00:00:00Z')`,
		`INSERT INTO bench_runs(id,host_id,device_id,class,kinds_json,node_id,execution_id,state,requested_by,created_at) VALUES('b1','h','GPU-1','gpu','[]','n1','e1','running','t','now')`,
	} {
		if _, err = db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	return path
}

func TestImportCopiesAndVerifiesEveryTable(t *testing.T) {
	dsn := pgtest.DSN(t)
	results, err := Import(context.Background(), sqliteFixture(t), dsn)
	if err != nil {
		t.Fatalf("import: %v (%+v)", err, results)
	}
	rows := map[string]int64{}
	for _, r := range results {
		if !r.OK() {
			t.Errorf("%s: %+v", r.Table, r)
		}
		rows[r.Table] = r.TargetRows
	}
	if rows["coordination_scopes"] != 3 || rows["lease_items"] != 4 || rows["resource_observations"] != 2 || len(results) < 50 {
		t.Fatalf("rows: %v", rows)
	}
	st, err := store.Open(dsn)
	if err != nil {
		t.Fatalf("open the imported database: %v", err)
	}
	st.Close()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var enabled, partial bool
	var valid sql.NullBool
	var utilization float64
	if err = db.QueryRow(`SELECT (SELECT enabled FROM executors WHERE id='x1'),(SELECT partial FROM attempt_energy),(SELECT valid FROM bench_runs),(SELECT utilization FROM resource_observations WHERE id=1)`).Scan(&enabled, &partial, &valid, &utilization); err != nil {
		t.Fatal(err)
	}
	if enabled || !partial || valid.Valid || utilization != 0.1 {
		t.Fatalf("values: enabled=%v partial=%v valid=%v utilization=%v", enabled, partial, valid, utilization)
	}
	// SQLite gave out id 10 before deleting it: ids go on after it.
	var next int64
	if err = db.QueryRow(`SELECT nextval(pg_get_serial_sequence('resource_observations','id'))`).Scan(&next); err != nil || next != 11 {
		t.Fatalf("next observation id %d: %v", next, err)
	}
	// The lease guard is back on.
	if _, err = db.Exec(`SET default_transaction_read_only=off; INSERT INTO lease_items VALUES('lease-b','gpu-1','gpu',1,'x',false)`); err == nil {
		t.Fatal("a second live item of a leased GPU was accepted")
	}
}

func TestImportRefusesATargetThatIsNotEmpty(t *testing.T) {
	dsn := pgtest.DSN(t)
	st, err := store.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	if _, err = Import(context.Background(), sqliteFixture(t), dsn); err == nil {
		t.Fatal("imported into a database that has a schema")
	}
	if _, err = Import(context.Background(), filepath.Join(t.TempDir(), "missing.db"), pgtest.DSN(t)); err == nil {
		t.Fatal("imported a missing file")
	}
}
