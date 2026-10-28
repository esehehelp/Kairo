// Package pgtest gives each test a database of its own on a PostgreSQL test
// server, so tests run in parallel and leave nothing behind.
//
// A run that cannot clean up (a panic, a killed test binary) leaves its
// databases; the next run drops those older than six hours.
//
// The server is KAIRO_TEST_PG (a postgres:// URL or key=value connection
// string of a role that may create databases), by default the local Docker
// server of the README. A test fails, it is not skipped, when the server
// cannot be reached.
package pgtest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// DefaultDSN is the test server when KAIRO_TEST_PG is not set.
const DefaultDSN = "postgres://kairo:kairo-test@127.0.0.1:55432/kairo?sslmode=disable"

var (
	adminOnce sync.Once
	admin     *sql.DB
	adminErr  error
)

// Server returns the test server's connection string.
func Server() string {
	if dsn := os.Getenv("KAIRO_TEST_PG"); dsn != "" {
		return dsn
	}
	return DefaultDSN
}

func adminDB() (*sql.DB, error) {
	adminOnce.Do(func() {
		admin, adminErr = sql.Open("pgx", Server())
		if adminErr == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if adminErr = admin.PingContext(ctx); adminErr == nil {
				sweep(ctx, admin)
			}
		}
	})
	return admin, adminErr
}

// staleAfter: a test database this old was left by a run that never
// cleaned up (a panic, a killed test binary) and is dropped.
const staleAfter = 6 * time.Hour

// sweep drops the stale test databases (their names carry their creation
// time); a failure only leaves them for the next run.
func sweep(ctx context.Context, db *sql.DB) {
	rows, err := db.QueryContext(ctx, `SELECT datname FROM pg_database WHERE datname LIKE 'kairo\_test\_%'`)
	if err != nil {
		return
	}
	var stale []string
	for rows.Next() {
		var name string
		if rows.Scan(&name) != nil {
			break
		}
		parts := strings.Split(name, "_")
		if len(parts) != 4 {
			continue
		}
		created, err := strconv.ParseInt(parts[2], 10, 64)
		if err == nil && time.Since(time.Unix(created, 0)) > staleAfter {
			stale = append(stale, name)
		}
	}
	rows.Close()
	for _, name := range stale {
		_, _ = db.ExecContext(ctx, `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`)
	}
}

// DSN creates an empty database for t (UTF-8, collation C, as Kairo needs)
// and returns its connection string. The database is dropped when t ends.
func DSN(t testing.TB) string {
	t.Helper()
	db, err := adminDB()
	if err != nil {
		t.Fatalf("PostgreSQL test server %s is unreachable (start it, or point KAIRO_TEST_PG at one): %v", redact(Server()), err)
	}
	name := NewName()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, `CREATE DATABASE `+name+` TEMPLATE template0 ENCODING 'UTF8' LOCALE 'C'`); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() { Drop(t, name) })
	return WithDatabase(Server(), name)
}

// NewName returns a random database name for a test, with its creation
// time (kairo_test_<unix seconds>_<random>).
func NewName() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return "kairo_test_" + strconv.FormatInt(time.Now().Unix(), 10) + "_" + hex.EncodeToString(b[:])
}

// Drop drops a test database, ending its connections.
func Drop(t testing.TB, name string) {
	db, err := adminDB()
	if err != nil {
		t.Errorf("drop test database %s: %v", name, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`); err != nil {
		t.Errorf("drop test database %s: %v", name, err)
	}
}

// WithDatabase returns dsn naming database name instead of its own.
func WithDatabase(dsn, name string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			panic(fmt.Sprintf("KAIRO_TEST_PG: %v", err))
		}
		u.Path = "/" + name
		return u.String()
	}
	return dsn + " dbname=" + name
}

// redact hides a URL's password.
func redact(dsn string) string {
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		return u.Redacted()
	}
	return dsn
}
