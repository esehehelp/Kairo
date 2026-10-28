// Package pgtest gives each test a database of its own on a PostgreSQL test
// server, so tests run in parallel and leave nothing behind.
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
			adminErr = admin.PingContext(ctx)
		}
	})
	return admin, adminErr
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

// NewName returns a random database name for a test.
func NewName() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return "kairo_test_" + hex.EncodeToString(b[:])
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
