package store

import (
	"database/sql"
	"fmt"
)

// migrateExecutionEnv adds execution_requests.env_json (an execution's
// declared environment, NULL when it has none) to databases created before
// it existed. A database that already has the column is left alone.
func migrateExecutionEnv(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(execution_requests)`)
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notNull, pk int
		var name, columnType string
		var defaultValue sql.NullString
		if err = rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			return err
		}
		if name == "env_json" {
			found = true
		}
	}
	if err = rows.Close(); err != nil {
		return err
	}
	if found {
		return nil
	}
	if _, err = db.Exec(`ALTER TABLE execution_requests ADD COLUMN env_json TEXT`); err != nil {
		return fmt.Errorf("add execution_requests.env_json: %w", err)
	}
	return nil
}
