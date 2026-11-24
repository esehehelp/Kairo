// Package dbtx holds what every part of the store shares to read and write
// the database (README: Architecture): beginning a transaction under the
// decision lock, walking rows, the event log, and the small SQL helpers. The
// store and each domain package split from it (store/provenance, ...)
// import it, so the domains need not import the store.
package dbtx

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrNotFound: what a call names is not recorded.
var ErrNotFound = errors.New("not found")

// decisionLock is the key of the transaction-scoped advisory lock that
// BeginWrite takes before anything else. Kairo's decisions (admission,
// leases, launch, gates, preemption, quarantine, commands, device settings,
// machine apply, orchestration) read state and write what follows from it;
// holding one lock for the whole transaction runs them one at a time, as if
// there were a single connection, whatever the pool size and however many
// processes share the database. Reads take no lock and run concurrently.
const decisionLock int64 = 0x4b4149524f // "KAIRO"

// LockStatement makes a transaction writable (connections default to
// read-only, so a write outside BeginWrite fails) and takes the decision
// lock, in one round trip. Tools that write with a connection of their own
// (the SQLite import) begin with it too.
var LockStatement = "SET TRANSACTION READ WRITE; SELECT pg_advisory_xact_lock(" + strconv.FormatInt(decisionLock, 10) + ")"

// BeginWrite begins a writing transaction. It takes the decision lock first,
// so decisions never overlap.
func BeginWrite(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
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

// Queryer reads: a pool or a transaction.
type Queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// EachRow runs a query and calls scan on each of its rows.
func EachRow(ctx context.Context, q Queryer, query string, args []any, scan func(*sql.Rows) error) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Now is the time a record is stamped with.
func Now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// Nullable is value, or NULL when it is empty.
func Nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// Bind appends v to args and returns its placeholder ($n), for a query built
// piece by piece.
func Bind(args *[]any, v any) string {
	*args = append(*args, v)
	return "$" + strconv.Itoa(len(*args))
}

// UniqueViolation reports whether err is a unique constraint's refusal.
func UniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// AppendEvent appends an event to the coordination event log (GET
// /api/events), in tx.
func AppendEvent(ctx context.Context, tx *sql.Tx, eventType string, projectScopeID *string, aggregateType, aggregateID string, epoch *int64, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO coordination_events(event_type,project_scope_id,aggregate_type,aggregate_id,coordination_epoch,payload_json,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, eventType, projectScopeID, aggregateType, aggregateID, epoch, string(body), Now())
	return err
}
