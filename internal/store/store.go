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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// migrations are the schema's numbered steps (NNNN_name.sql), applied in
// order; schema_version records the ones a database has.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

var (
	ErrNotFound              = errors.New("not found")
	ErrIdempotencyConflict   = errors.New("idempotency key was already used for different content")
	ErrGateClosed            = errors.New("coordination admission gate is closed")
	ErrExecutionStarted      = errors.New("execution has already started")
	ErrStaleEpoch            = errors.New("stale coordination epoch")
	ErrOrchestrationConflict = errors.New("logical task was already declared with different immutable content")
	// ErrNoSchema: OpenExisting found no Kairo schema in the database.
	ErrNoSchema = errors.New("the database holds no Kairo schema")
	// ErrForeignDatabase: the database has tables but no Kairo schema.
	ErrForeignDatabase = errors.New("the database is not empty and holds no Kairo schema; use an empty database")
	// ErrGangNotReady: a gang rank is prepared but not every rank is yet; the
	// executor keeps its prepared reservation and asks again.
	ErrGangNotReady = errors.New("gang placement is not prepared on every rank yet")
	// ErrGangAborted: the gang placement was given up; the executor must clean
	// up and release its reservation.
	ErrGangAborted = errors.New("gang placement was aborted")
)

// decisionLock is the key of the transaction-scoped advisory lock that every
// writing transaction takes before anything else (beginWrite). Kairo's
// decisions (admission, leases, launch, gates, preemption, quarantine,
// commands, device settings, machine apply, orchestration) read state and
// write what follows from it; holding one lock for the whole transaction runs
// them one at a time, as if there were a single connection, whatever the
// pool size and however many processes share the database. Reads take no
// lock and run concurrently.
const decisionLock int64 = 0x4b4149524f // "KAIRO"

// LockStatement makes a transaction writable (connections default to
// read-only, so a write outside beginWrite fails) and takes the decision
// lock, in one round trip. Tools that write with a connection of their own
// (the SQLite import) begin with it too.
var LockStatement = "SET TRANSACTION READ WRITE; SELECT pg_advisory_xact_lock(" + strconv.FormatInt(decisionLock, 10) + ")"

// maxConns bounds the pool. Writers wait for each other on the decision
// lock, so more connections only serve more concurrent reads.
const maxConns = 16

type Store struct {
	// db is the pool. Its connections are read-only by default
	// (default_transaction_read_only): read through it directly or with
	// beginRead; write only through beginWrite or exec.
	db     *sql.DB
	policy SchedulingPolicy
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
	return &Store{db: db, policy: SchedulingPolicy{DiskMinFreeBytes: DefaultDiskMinFreeBytes}}, nil
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
	var versioned bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('schema_version') IS NOT NULL`).Scan(&versioned); err != nil {
		return err
	}
	if versioned {
		return errors.New("the database already holds a Kairo schema; use an empty database")
	}
	return migrateTx(ctx, tx, true)
}

func migrateTx(ctx context.Context, tx *sql.Tx, create bool) error {
	steps, err := Migrations()
	if err != nil {
		return err
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
// (times and identifiers are compared as text) or is not UTF-8.
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

// beginWrite begins a transaction that may write: the only way to write.
// It takes the decision lock first, so writing transactions never overlap.
func (s *Store) beginWrite(ctx context.Context) (*sql.Tx, error) { return beginWriteDB(ctx, s.db) }

// beginWriteDB is beginWrite on a pool (Open migrates through it).
func beginWriteDB(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, LockStatement); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

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

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// bind appends v to args and returns its placeholder ($n), for a query built
// piece by piece.
func bind(args *[]any, v any) string {
	*args = append(*args, v)
	return "$" + strconv.Itoa(len(*args))
}

// uniqueViolation reports whether err is a unique constraint's refusal.
func uniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
