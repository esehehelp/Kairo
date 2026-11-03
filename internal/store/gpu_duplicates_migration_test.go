package store

import (
	"context"
	"strings"
	"testing"
)

// Migrating to the device-wide lease guard stops, naming the device, when one
// physical GPU is already held by two live leases through two nodes' rows.
func TestMigrationRefusesAGPUAlreadyHeldByTwoLiveLeases(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	ctx := context.Background()
	launch := startShared(t, store, "holder-on-a", "a", 0)
	var other string
	if err := store.db.QueryRow(`SELECT id FROM resource_instances WHERE kind='gpu' AND stable_identity='GPU-S' AND node_id='b'`).Scan(&other); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`ALTER TABLE lease_items DISABLE TRIGGER exclusive_resource_lease_guard`,
		`INSERT INTO leases(id,execution_id,executor_id,state,created_at) VALUES('lease-dup-b','` + launch.Execution.ID + `','exec-b','active','now')`,
		`INSERT INTO lease_items(lease_id,resource_id,kind,quantity,filesystem) VALUES('lease-dup-b','` + other + `','gpu',1,'')`,
		`ALTER TABLE lease_items ENABLE TRIGGER exclusive_resource_lease_guard`,
		`DELETE FROM schema_version WHERE version>=6`,
	} {
		if _, err := store.testExec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	tx, err := store.beginWrite(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	err = MigrateTx(ctx, tx)
	if err == nil || !strings.Contains(err.Error(), "GPU-S") || !strings.Contains(err.Error(), "lease-dup-b") {
		t.Fatalf("migration over a doubly held GPU: %v", err)
	}
}
