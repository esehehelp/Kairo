package store

import (
	"context"
	"database/sql"
	"testing"

	"kairo/internal/pgtest"
)

// The stores migrations (0007-0009) take a database of before them as it
// is: its executions keep their state and get scratch_tmpdir off, and it
// has no stores, observations or scratch until they are declared.
func TestStoresMigrationsKeepAPopulatedDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = CreateSchemaAt(ctx, tx, 6); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`INSERT INTO coordination_scopes VALUES('s-project','project',NULL,'kairo','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		`INSERT INTO execution_requests(id,project_scope_id,client_request_id,spec_digest,state,argv_json,cwd,submitted_at) VALUES('e1','s-project','r1','d1','waiting','["run"]','/w','2026-01-01T00:00:00Z')`,
	} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var state string
	var tmpdir bool
	if err = store.db.QueryRow(`SELECT state,scratch_tmpdir FROM execution_requests WHERE id='e1'`).Scan(&state, &tmpdir); err != nil || state != "waiting" || tmpdir {
		t.Fatalf("execution after the migration: %s %v %v", state, tmpdir, err)
	}
	for _, table := range []string{"machine_stores", "store_observations", "attempt_scratch"} {
		var rows int
		if err = store.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&rows); err != nil || rows != 0 {
			t.Fatalf("%s: %d %v", table, rows, err)
		}
	}
	if status, err := store.ListStoreStatus(ctx); err != nil || len(status) != 0 {
		t.Fatalf("status: %+v %v", status, err)
	}
	if machines, err := store.ListMachines(ctx); err != nil || len(machines.Stores) != 0 {
		t.Fatalf("machines: %+v %v", machines, err)
	}
}
