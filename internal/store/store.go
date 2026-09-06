package store

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

var (
	ErrNotFound            = errors.New("not found")
	ErrIdempotencyConflict = errors.New("idempotency key was already used for different content")
	ErrGateClosed          = errors.New("coordination admission gate is closed")
	ErrExecutionStarted    = errors.New("execution has already started")
	ErrStaleEpoch          = errors.New("stale coordination epoch")
	ErrLegacySchema        = errors.New("legacy Kairo database detected; archive it and create a fresh V3 coordination database")
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
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }
