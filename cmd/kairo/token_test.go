package main

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"kairo/internal/api"
	"kairo/internal/store"
)

// tokenDB creates an empty Kairo database and returns its path.
func tokenDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kairo.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func authenticate(t *testing.T, db, token string) error {
	t.Helper()
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_, err = st.AuthenticateAPIToken(context.Background(), token)
	return err
}

func TestTokenCreateListRevoke(t *testing.T) {
	isolateClientEnv(t)
	db := tokenDB(t)
	out, err := captureStdout(t, func() error {
		return run([]string{"token", "create", "--db", db, "--name", "operator", "--role", "admin"})
	})
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(out)
	if !strings.HasPrefix(token, "kairo_admin_") || strings.Contains(token, "\n") {
		t.Fatalf("create printed %q", out)
	}
	if err := authenticate(t, db, token); err != nil {
		t.Fatalf("created token does not authenticate: %v", err)
	}

	out, err = captureStdout(t, func() error { return run([]string{"token", "list", "--db", db}) })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "LAST USED") || !strings.Contains(out, "operator") || !strings.Contains(out, "admin") {
		t.Fatalf("list:\n%s", out)
	}

	// flags after the positional argument are parsed too
	if _, err := captureStdout(t, func() error { return run([]string{"token", "revoke", "operator", "--db", db}) }); err != nil {
		t.Fatal(err)
	}
	if err := authenticate(t, db, token); err == nil {
		t.Fatal("revoked token still authenticates")
	}
	if _, err := captureStdout(t, func() error { return run([]string{"token", "revoke", "--db", db, "operator"}) }); err == nil {
		t.Fatal("revoking an already revoked token succeeded")
	}
}

func TestTokenCreateOutWritesAPrivateFileOnce(t *testing.T) {
	isolateClientEnv(t)
	db := tokenDB(t)
	file := filepath.Join(t.TempDir(), "secrets", "token")
	out, err := captureStdout(t, func() error {
		return run([]string{"token", "create", "--db", db, "--name", "reader", "--role", "read", "--out", file})
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != file {
		t.Fatalf("create --out printed %q, want the path", out)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(string(data))
	if !strings.HasPrefix(token, "kairo_read_") {
		t.Fatalf("file holds %q", data)
	}
	if err := authenticate(t, db, token); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("mode %v", info.Mode().Perm())
		}
	}
	_, err = captureStdout(t, func() error {
		return run([]string{"token", "create", "--db", db, "--name", "second", "--role", "read", "--out", file})
	})
	if err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("overwrite without --force: %v", err)
	}
	listed, _ := captureStdout(t, func() error { return run([]string{"token", "list", "--db", db}) })
	if strings.Contains(listed, "second") {
		t.Fatalf("a refused create left a token behind:\n%s", listed)
	}
	if _, err := captureStdout(t, func() error {
		return run([]string{"token", "create", "--db", db, "--name", "second", "--role", "read", "--out", file, "--force"})
	}); err != nil {
		t.Fatal(err)
	}
	if replaced, _ := os.ReadFile(file); string(replaced) == string(data) {
		t.Fatal("--force did not replace the file")
	}
}

func TestTokenCreateSaveFeedsTheClient(t *testing.T) {
	dir := isolateClientEnv(t)
	db := tokenDB(t)
	out, err := captureStdout(t, func() error {
		return run([]string{"token", "create", "--db", db, "--name", "operator", "--role", "admin", "--save"})
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != filepath.Join(dir, "token") {
		t.Fatalf("create --save printed %q", out)
	}
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	server := httptest.NewServer((&api.Server{Store: st}).Handler())
	defer server.Close()
	out, err = captureStdout(t, func() error { return run([]string{"doctor", "--api", server.URL}) })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out, "authenticated as operator (admin)\n") {
		t.Fatalf("doctor: %q", out)
	}
	path := filepath.Join(t.TempDir(), "execution.toml")
	writeFile(t, path, "schema_version = 2\nclient_request_id = \"req-1\"\nproject = \"p\"\nargv = [\"echo\"]\ncwd = \".\"\n")
	if out, err = captureStdout(t, func() error { return run([]string{"execution", "submit", "--api", server.URL, path}) }); err != nil {
		t.Fatalf("submit against the real API: %v %s", err, out)
	}
}

func TestTokenCreateValidation(t *testing.T) {
	isolateClientEnv(t)
	db := tokenDB(t)
	for _, args := range [][]string{
		{"create", "--db", db, "--name", "agent", "--role", "node"},
		{"create", "--db", db, "--name", "x", "--role", "admin", "--node", "pve0"},
		{"create", "--db", db, "--name", "x", "--role", "root"},
		{"create", "--db", db, "--role", "admin"},
		{"create", "--name", "x", "--role", "admin"},
		{"create", "--db", db, "--config", "kairo.toml", "--name", "x", "--role", "admin"},
		{"create", "--db", filepath.Join(t.TempDir(), "missing.db"), "--name", "x", "--role", "admin"},
		{"create", "--db", db, "--name", "x", "--role", "admin", "--save", "--out", "f"},
		{"create", "--db", db, "--name", "x", "--role", "admin", "--force"},
		{"list", "--db", db, "extra"},
		{"revoke", "--db", db},
		{"rotate", "--db", db},
	} {
		if _, err := captureStdout(t, func() error { return tokenCommand(args) }); err == nil {
			t.Fatalf("token %v was accepted", args)
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(db), "missing.db")); err == nil {
		t.Fatal("token create created a database")
	}
	out, err := captureStdout(t, func() error {
		return tokenCommand([]string{"create", "--db", db, "--name", "agent", "--role", "node", "--node", "pve0"})
	})
	if err != nil || !strings.HasPrefix(strings.TrimSpace(out), "kairo_node_") {
		t.Fatalf("node token: %v %q", err, out)
	}
}
