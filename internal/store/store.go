package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

//go:embed orchestration_schema.sql
var orchestrationSchema string

//go:embed quarantine_schema.sql
var quarantineSchema string

var (
	ErrNotFound              = errors.New("not found")
	ErrIdempotencyConflict   = errors.New("idempotency key was already used for different content")
	ErrGateClosed            = errors.New("coordination admission gate is closed")
	ErrExecutionStarted      = errors.New("execution has already started")
	ErrStaleEpoch            = errors.New("stale coordination epoch")
	ErrOrchestrationConflict = errors.New("logical task was already declared with different immutable content")
	ErrLegacySchema          = errors.New("legacy Kairo database detected; archive it and create a fresh V3 coordination database")
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	var migrated int
	err = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'`).Scan(&migrated)
	if err != nil {
		db.Close()
		return nil, err
	}
	if migrated == 0 {
		var userTables int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&userTables); err != nil {
			db.Close()
			return nil, err
		}
		if userTables != 0 {
			db.Close()
			return nil, ErrLegacySchema
		}
		if _, err := db.Exec(schema); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply schema: %w", err)
		}
	} else {
		var version int
		if err := db.QueryRow(`SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&version); err != nil {
			db.Close()
			return nil, err
		}
		if version == 1 || version == 2 {
			db.Close()
			return nil, ErrLegacySchema
		}
		if version != 3 {
			db.Close()
			return nil, fmt.Errorf("unsupported schema version %d", version)
		}
	}
	var orchestrationMigrated int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='orchestration_schema_migrations'`).Scan(&orchestrationMigrated); err != nil {
		db.Close()
		return nil, err
	}
	if orchestrationMigrated != 0 {
		var version int
		if err := db.QueryRow(`SELECT COALESCE(MAX(version),0) FROM orchestration_schema_migrations`).Scan(&version); err != nil {
			db.Close()
			return nil, err
		}
		if version != 1 {
			db.Close()
			return nil, fmt.Errorf("unsupported orchestration schema version %d", version)
		}
	}
	if _, err := db.Exec(orchestrationSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply orchestration schema: %w", err)
	}
	// Node quarantine adds a suspend origin and an orchestration reason.
	for _, w := range []struct{ table, old, new string }{
		{"commands", "origin IN('scope_pause','priority_preemption')", "origin IN('scope_pause','priority_preemption','node_quarantine')"},
		{"orchestration_decisions", "reason IN('initial','continuation')", "reason IN('initial','continuation','quarantine_restart')"},
	} {
		if err := widenCheck(db, w.table, w.old, w.new); err != nil {
			db.Close()
			return nil, err
		}
	}
	if _, err := db.Exec(quarantineSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply quarantine schema: %w", err)
	}
	return &Store{db: db}, nil
}

// widenCheck adds a value to a CHECK(... IN(...)) list of an existing table,
// for databases created before the value existed. SQLite cannot alter a
// CHECK, so the table is rebuilt in one transaction with foreign keys off
// (the documented procedure); its indexes are recreated and foreign keys are
// verified before committing. A table that already allows the value is left
// alone.
func widenCheck(db *sql.DB, table, oldList, newList string) error {
	var ddl string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&ddl); err != nil {
		return err
	}
	if strings.Contains(ddl, newList) {
		return nil
	}
	if !strings.Contains(ddl, oldList) {
		return fmt.Errorf("unexpected %s definition: %s", table, ddl)
	}
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, `SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL`, table)
	if err != nil {
		return err
	}
	var indexes []string
	for rows.Next() {
		var ix string
		if err = rows.Scan(&ix); err != nil {
			rows.Close()
			return err
		}
		indexes = append(indexes, ix)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		return err
	}
	defer conn.ExecContext(ctx, `PRAGMA foreign_keys = ON`)
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	widened := table + "_widened"
	// The stored DDL names the table plainly, or quoted after a rename.
	head := regexp.MustCompile(`(?i)^\s*CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?("` + regexp.QuoteMeta(table) + `"|` + regexp.QuoteMeta(table) + `)\s*\(`)
	loc := head.FindStringIndex(ddl)
	if loc == nil {
		return fmt.Errorf("unexpected %s definition: %s", table, ddl)
	}
	newDDL := "CREATE TABLE " + widened + "(" + ddl[loc[1]:]
	newDDL = strings.Replace(newDDL, oldList, newList, 1)
	stmts := []string{newDDL, `INSERT INTO ` + widened + ` SELECT * FROM ` + table, `DROP TABLE ` + table, `ALTER TABLE ` + widened + ` RENAME TO ` + table}
	stmts = append(stmts, indexes...)
	for _, stmt := range stmts {
		if _, err = tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("widen %s: %s: %w", table, strings.SplitN(stmt, "(", 2)[0], err)
		}
	}
	check, err := tx.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return err
	}
	violation := check.Next()
	if err = check.Close(); err != nil {
		return err
	}
	if violation {
		return fmt.Errorf("foreign key check failed after rebuilding %s", table)
	}
	return tx.Commit()
}

func (s *Store) Close() error { return s.db.Close() }

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }
