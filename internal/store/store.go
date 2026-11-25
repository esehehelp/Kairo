package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"kairo/internal/store/dbtx"
)

// migrations are the schema's numbered steps (NNNN_name.sql), applied in
// order; schema_version records the ones a database has.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

var (
	ErrNotFound              = dbtx.ErrNotFound
	ErrIdempotencyConflict   = errors.New("idempotency key was already used for different content")
	ErrGateClosed            = errors.New("coordination admission gate is closed")
	ErrExecutionStarted      = errors.New("execution has already started")
	ErrStaleEpoch            = errors.New("stale coordination epoch")
	ErrOrchestrationConflict = errors.New("logical task was already declared with different immutable content")
	// ErrNoSchema: OpenExisting found no Kairo schema in the database.
	ErrNoSchema = errors.New("the database holds no Kairo schema")
	// ErrForeignDatabase: the database has tables but no Kairo schema.
	ErrForeignDatabase = errors.New("the database is not empty and holds no Kairo schema; use an empty database")
)

// LockStatement makes a transaction writable and takes the decision lock
// (dbtx.LockStatement), for tools that write with a connection of their own.
var LockStatement = dbtx.LockStatement

// maxConns bounds the pool. Writers wait for each other on the decision
// lock, so more connections only serve more concurrent reads.
const maxConns = 16

type Store struct {
	// db is the pool. Its connections are read-only by default
	// (default_transaction_read_only): read through it directly or with
	// beginRead; write through beginWrite or exec, except as documented
	// by beginUnlockedWrite.
	db     *sql.DB
	policy SchedulingPolicy
	// lastPass is when ReserveNext last completed a whole planning pass
	// (UnixNano; 0: never), which lets a poll with nothing to plan skip one.
	lastPass atomic.Int64
	// runs is the run history the planner estimates run times from
	// (estimate.go).
	runs runHistoryCache
	// data is the data policy (scrub.go).
	data DataPolicy
}

// Open connects to the PostgreSQL database dsn (a postgres:// URL or a
// key=value connection string). An empty database gets the schema; a Kairo
// database gets the migrations it lacks.
func Open(dsn string) (*Store, error) { return open(dsn, true, nil) }

// OpenExisting is Open for tools that must not create a database of their
// own (a token in a database the daemon does not use would never
// authenticate): it fails with ErrNoSchema when the database holds no Kairo
// schema.
func OpenExisting(dsn string) (*Store, error) { return open(dsn, false, nil) }

func open(dsn string, create bool, tracer pgx.QueryTracer) (*Store, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("database url: %w", err)
	}
	config.RuntimeParams["default_transaction_read_only"] = "on"
	if config.RuntimeParams["application_name"] == "" {
		config.RuntimeParams["application_name"] = "kairo"
	}
	config.Tracer = tracer
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxIdleTime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := migrate(ctx, db, create); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, policy: SchedulingPolicy{DiskMinFreeBytes: DefaultDiskMinFreeBytes},
		data: DataPolicy{ScrubInterval: DefaultScrubInterval, ScrubBytesPerDay: DefaultScrubBytesPerDay}}, nil
}

// Migrations returns the schema's numbered steps in order (the import tool
// creates the schema with them).
func Migrations() ([]string, error) {
	names, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	var files []string
	for _, entry := range names {
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	out := make([]string, 0, len(files))
	for i, name := range files {
		number, _, ok := strings.Cut(name, "_")
		if n, err := strconv.Atoi(number); !ok || err != nil || n != i+1 {
			return nil, fmt.Errorf("migration %s is out of sequence", name)
		}
		body, err := migrationFiles.ReadFile(path.Join("migrations", name))
		if err != nil {
			return nil, err
		}
		out = append(out, string(body))
	}
	return out, nil
}

// migrate checks the database and brings its schema up to date, in one
// transaction under the decision lock (two daemons starting at once migrate
// one after the other).
func migrate(ctx context.Context, db *sql.DB, create bool) error {
	tx, err := beginWriteDB(ctx, db)
	if err != nil {
		return fmt.Errorf("connect to the database: %w", err)
	}
	defer tx.Rollback()
	if err := migrateTx(ctx, tx, create); err != nil {
		return err
	}
	return tx.Commit()
}

// CreateSchema creates the schema in an empty database within tx, which
// must be writable (the import tool's transaction).
func CreateSchema(ctx context.Context, tx *sql.Tx) error {
	return CreateSchemaAt(ctx, tx, 0)
}

// CreateSchemaAt is CreateSchema up to migration version (0: every one):
// the import tool copies into the schema its source had, then migrates it
// (MigrateTx) as a daemon would.
func CreateSchemaAt(ctx context.Context, tx *sql.Tx, version int) error {
	var versioned bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('schema_version') IS NOT NULL`).Scan(&versioned); err != nil {
		return err
	}
	if versioned {
		return errors.New("the database already holds a Kairo schema; use an empty database")
	}
	return migrateTo(ctx, tx, true, version)
}

// MigrateTx brings the schema of tx's database up to date.
func MigrateTx(ctx context.Context, tx *sql.Tx) error {
	return migrateTx(ctx, tx, false)
}

func migrateTx(ctx context.Context, tx *sql.Tx, create bool) error {
	return migrateTo(ctx, tx, create, 0)
}

// migrateTo applies the migrations the database lacks up to target (0:
// every one).
func migrateTo(ctx context.Context, tx *sql.Tx, create bool, target int) error {
	steps, err := Migrations()
	if err != nil {
		return err
	}
	if target > 0 && target < len(steps) {
		steps = steps[:target]
	}
	if err := checkDatabase(ctx, tx); err != nil {
		return err
	}
	var versioned bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('schema_version') IS NOT NULL`).Scan(&versioned); err != nil {
		return err
	}
	version := 0
	if versioned {
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM schema_version`).Scan(&version); err != nil {
			return err
		}
	} else {
		if !create {
			return ErrNoSchema
		}
		var tables int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_tables WHERE schemaname=current_schema()`).Scan(&tables); err != nil {
			return err
		}
		if tables != 0 {
			return ErrForeignDatabase
		}
	}
	if version > len(steps) {
		return fmt.Errorf("the database schema is version %d, newer than this Kairo knows (%d)", version, len(steps))
	}
	for v := version + 1; v <= len(steps); v++ {
		if _, err := tx.ExecContext(ctx, steps[v-1]); err != nil {
			return fmt.Errorf("apply schema migration %d: %w", v, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_version(version,applied_at) VALUES($1,$2)`, v, now()); err != nil {
			return err
		}
	}
	return nil
}

// checkDatabase refuses a database whose text does not compare byte by byte
// (identifiers and some timestamps are compared as text) or is not UTF-8.
func checkDatabase(ctx context.Context, tx *sql.Tx) error {
	var encoding string
	var bytewise bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_encoding_to_char(encoding),'B'<'a' AND 'a-z'<'a.b' AND 'Z'<'_' AND '9Z'<'9_' FROM pg_database WHERE datname=current_database()`).Scan(&encoding, &bytewise); err != nil {
		return err
	}
	if encoding != "UTF8" || !bytewise {
		return fmt.Errorf("the database must use encoding UTF8 and collation C (it has %s, and its collation does not compare text byte by byte); create it with CREATE DATABASE ... TEMPLATE template0 ENCODING 'UTF8' LOCALE 'C'", encoding)
	}
	return nil
}

// beginWrite begins a writing transaction. It takes the decision lock first,
// so decisions never overlap.
func (s *Store) beginWrite(ctx context.Context) (*sql.Tx, error) { return beginWriteDB(ctx, s.db) }

// beginUnlockedWrite is only for RecordStoreObservation and the single
// statement that refreshes api_tokens.last_used_at, which no decision uses.
// Copy room, shedding, fetch room and evictions read committed store
// observations without locking them. Writers that lock a machine_stores row
// must do so before touching its attempt_scratch, run_outputs or
// store_observations rows. Reports change shared rows with single statements
// whose conditions PostgreSQL rechecks under the row lock.
func (s *Store) beginUnlockedWrite(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "SET TRANSACTION READ WRITE"); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

// beginWriteDB is beginWrite on a pool (Open migrates through it).
func beginWriteDB(ctx context.Context, db *sql.DB) (*sql.Tx, error) { return dbtx.BeginWrite(ctx, db) }

// beginRead begins a read-only transaction whose statements all see one
// snapshot; it takes no lock.
func (s *Store) beginRead(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
}

// exec runs one writing statement in its own transaction (beginWrite).
func (s *Store) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return result, tx.Commit()
}

func (s *Store) Close() error { return s.db.Close() }

// The shared SQL helpers (dbtx), by the names the store uses.
func now() string                    { return dbtx.Now() }
func bind(args *[]any, v any) string { return dbtx.Bind(args, v) }
func uniqueViolation(err error) bool { return dbtx.UniqueViolation(err) }
