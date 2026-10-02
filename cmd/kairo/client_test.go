package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"kairo/internal/tlsutil"
)

const testToken = "kairo_admin_test"

// isolateClientEnv clears the client's environment and points the user config
// directory at a temporary one; it returns <user config dir>/kairo.
func isolateClientEnv(t *testing.T) string {
	t.Helper()
	for _, key := range []string{"KAIRO_API", "KAIRO_TOKEN", "KAIRO_TOKEN_FILE", "KAIRO_CA_FILE"} {
		t.Setenv(key, "")
	}
	home := t.TempDir()
	if runtime.GOOS == "windows" {
		t.Setenv("APPDATA", home)
	} else {
		t.Setenv("XDG_CONFIG_HOME", home)
		t.Setenv("HOME", home)
	}
	dir, err := kairoConfigDir()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(dir, home) {
		t.Fatalf("config dir %s is not under %s", dir, home)
	}
	return dir
}

// fakeAPI serves handler with KAIRO_TOKEN set, failing any request that does
// not carry it as a bearer token.
func fakeAPI(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	isolateClientEnv(t)
	t.Setenv("KAIRO_TOKEN", testToken)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("%s %s: Authorization = %q", r.Method, r.URL.Path, got)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"valid token required","code":"unauthorized"}`))
			return
		}
		handler.ServeHTTP(w, r)
	}))
}

// captureStdout returns what fn writes to os.Stdout.
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		data, _ := io.ReadAll(r)
		done <- string(data)
	}()
	runErr := fn()
	os.Stdout = saved
	w.Close()
	out := <-done
	r.Close()
	return out, runErr
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestClientResolvesURLAndToken(t *testing.T) {
	dir := isolateClientEnv(t)
	c, err := newAPIClient("")
	if err != nil {
		t.Fatal(err)
	}
	if c.baseURL != defaultAPIURL || c.token != "" {
		t.Fatalf("defaults: %q %q", c.baseURL, c.token)
	}
	if _, err := c.call(http.MethodGet, "/api/whoami", nil); !errors.Is(err, errNoToken) {
		t.Fatalf("request without a token: %v", err)
	}

	writeFile(t, filepath.Join(dir, "token"), "  from-default-file\n")
	t.Setenv("KAIRO_API", "https://kairo.example:7474/")
	if c, err = newAPIClient(""); err != nil || c.baseURL != "https://kairo.example:7474" || c.token != "from-default-file" {
		t.Fatalf("env URL, default token file: %+v %v", c, err)
	}
	if c, err = newAPIClient("https://flag.example:1"); err != nil || c.baseURL != "https://flag.example:1" {
		t.Fatalf("--api wins over KAIRO_API: %+v %v", c, err)
	}

	tokenFile := filepath.Join(t.TempDir(), "tok")
	writeFile(t, tokenFile, "from-token-file\r\n")
	t.Setenv("KAIRO_TOKEN_FILE", tokenFile)
	if c, err = newAPIClient(""); err != nil || c.token != "from-token-file" {
		t.Fatalf("KAIRO_TOKEN_FILE: %+v %v", c, err)
	}
	t.Setenv("KAIRO_TOKEN", "from-env")
	if c, err = newAPIClient(""); err != nil || c.token != "from-env" {
		t.Fatalf("KAIRO_TOKEN wins: %+v %v", c, err)
	}

	t.Setenv("KAIRO_TOKEN", "")
	t.Setenv("KAIRO_TOKEN_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err = newAPIClient(""); err == nil {
		t.Fatal("a missing KAIRO_TOKEN_FILE was ignored")
	}
	t.Setenv("KAIRO_TOKEN_FILE", "")
	t.Setenv("KAIRO_CA_FILE", filepath.Join(t.TempDir(), "missing.pem"))
	if _, err = newAPIClient(""); err == nil {
		t.Fatal("a missing KAIRO_CA_FILE was ignored")
	}
	t.Setenv("KAIRO_CA_FILE", "")

	for _, bad := range []string{"127.0.0.1:7474", "ftp://host", "http://kairo.example:7474"} {
		if _, err = newAPIClient(bad); err == nil {
			t.Fatalf("%s was accepted (with a token)", bad)
		}
	}
	if _, err = newAPIClient("http://127.0.0.1:7474"); err != nil {
		t.Fatalf("plain http to loopback: %v", err)
	}
}

func TestClientHealthNeedsNoToken(t *testing.T) {
	isolateClientEnv(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Errorf("sent Authorization without a token")
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	c, err := newAPIClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.call(http.MethodGet, "/health", nil); err != nil {
		t.Fatal(err)
	}
}

func TestClientErrorNamesStatusAndServerMessage(t *testing.T) {
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"token is not allowed to do this","code":"forbidden"}`))
	}))
	defer server.Close()
	c, err := newAPIClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.call(http.MethodGet, "/api/resources", nil)
	if err == nil || err.Error() != "HTTP 403 forbidden: token is not allowed to do this" {
		t.Fatalf("error = %v", err)
	}
}

// tlsAPI serves whoami over TLS with a certificate from a fresh Kairo CA and
// returns the server and the CA PEM path.
func tlsAPI(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()
	if err := tlsutil.InitCA(dir); err != nil {
		t.Fatal(err)
	}
	if err := tlsutil.IssueServer(dir, []string{"127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, tlsutil.ServerCertFile), filepath.Join(dir, tlsutil.ServerKeyFile))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"name":"operator","role":"admin"}`))
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, filepath.Join(dir, tlsutil.CACertFile)
}

func TestClientTrustsTheKairoCA(t *testing.T) {
	dir := isolateClientEnv(t)
	t.Setenv("KAIRO_TOKEN", testToken)
	server, caFile := tlsAPI(t)
	whoami := func() error {
		c, err := newAPIClient(server.URL)
		if err != nil {
			return err
		}
		_, err = c.call(http.MethodGet, "/api/whoami", nil)
		return err
	}
	if err := whoami(); err == nil {
		t.Fatal("a server signed by an unknown CA was trusted with the system roots")
	}
	t.Setenv("KAIRO_CA_FILE", caFile)
	if err := whoami(); err != nil {
		t.Fatalf("with KAIRO_CA_FILE: %v", err)
	}
	t.Setenv("KAIRO_CA_FILE", "")
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, tlsutil.CACertFile), string(caPEM))
	if err := whoami(); err != nil {
		t.Fatalf("with the user config ca.pem: %v", err)
	}
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	var mu sync.Mutex
	followed := 0
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/elsewhere" {
			mu.Lock()
			followed++
			mu.Unlock()
			return
		}
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer server.Close()
	c, err := newAPIClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.call(http.MethodGet, "/api/whoami", nil)
	if err == nil || !strings.Contains(err.Error(), "HTTP 302") {
		t.Fatalf("error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if followed != 0 {
		t.Fatal("the redirect was followed")
	}
}

func TestClientIgnoresProxyEnvironment(t *testing.T) {
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	for _, key := range []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"} {
		t.Setenv(key, "http://127.0.0.1:1")
	}
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	c, err := newAPIClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if transport := c.http.Transport.(*http.Transport); transport.Proxy != nil {
		t.Fatal("the transport uses a proxy")
	}
	if _, err := c.call(http.MethodGet, "/api/whoami", nil); err != nil {
		t.Fatal(err)
	}
}

// recordingAPI records "METHOD PATH BODY" of every request and answers {}.
func recordingAPI(t *testing.T, requests *[]string) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	return fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		*requests = append(*requests, strings.TrimSpace(r.Method+" "+r.URL.RequestURI()+" "+string(body)))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/whoami" {
			_, _ = w.Write([]byte(`{"name":"operator","role":"admin"}`))
			return
		}
		_, _ = w.Write([]byte(`{"resources":[]}`))
	}))
}

func TestResourceAndNodeCommandsUseTheAPIRoutes(t *testing.T) {
	var requests []string
	server := recordingAPI(t, &requests)
	defer server.Close()
	api := "--api=" + server.URL
	for _, args := range [][]string{
		{"resource", "status", api},
		{"resource", "quarantine", api, "--reason", "fan noise", "gpu-0"},
		{"resource", "enable", api, "gpu-0"},
		{"resource", "reconcile", api, "--confirm-process-absent", "gpu-0"},
		{"node", "quarantine", api, "--actor", "me", "--reason", "maintenance", "pve0"},
		{"node", "release", api, "--actor", "me", "--all"},
		{"node", "quarantine-status", api},
	} {
		if _, err := captureStdout(t, func() error { return run(args) }); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	want := []string{
		"GET /api/resources",
		`POST /api/resources/gpu-0/quarantine {"reason":"fan noise"}`,
		"POST /api/resources/gpu-0/enable",
		`POST /api/resources/gpu-0/reconcile {"confirm_process_absent":true}`,
		`POST /api/nodes/pve0/quarantine {"actor":"me","reason":"maintenance"}`,
		`DELETE /api/nodes/all/quarantine {"actor":"me"}`,
		"GET /api/nodes/quarantines",
	}
	if strings.Join(requests, "\n") != strings.Join(want, "\n") {
		t.Fatalf("requests:\n%s\nwant:\n%s", strings.Join(requests, "\n"), strings.Join(want, "\n"))
	}
}

func TestExecutionCommandsUseTheAPIRoutesWithoutSchemaVersion(t *testing.T) {
	var requests []string
	server := recordingAPI(t, &requests)
	defer server.Close()
	path := filepath.Join(t.TempDir(), "execution.toml")
	writeFile(t, path, `schema_version = 2
client_request_id = "req-1"
project = "p"
argv = ["python", "train.py"]
cwd = "/work"
`)
	api := "--api=" + server.URL
	for _, args := range [][]string{
		{"execution", "submit", api, path},
		{"execution", "list", api, "--project", "p", "--limit", "5"},
		{"execution", "show", api, "exe_1"},
		{"execution", "withdraw", api, "exe_1"},
	} {
		if _, err := captureStdout(t, func() error { return run(args) }); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	if len(requests) != 4 {
		t.Fatalf("requests: %v", requests)
	}
	submit, ok := strings.CutPrefix(requests[0], "POST /api/executions ")
	if !ok {
		t.Fatalf("submit: %s", requests[0])
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(submit), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["schema_version"]; ok || body["client_request_id"] != "req-1" {
		t.Fatalf("submit body: %v", body)
	}
	want := []string{"GET /api/executions?limit=5&project=p", "GET /api/executions/exe_1", "POST /api/executions/exe_1/withdraw"}
	if strings.Join(requests[1:], "\n") != strings.Join(want, "\n") {
		t.Fatalf("requests: %v", requests[1:])
	}
}

func TestDoctorChecksTheTokenThenShowsResources(t *testing.T) {
	var requests []string
	server := recordingAPI(t, &requests)
	defer server.Close()
	out, err := captureStdout(t, func() error { return run([]string{"doctor", "--api", server.URL}) })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out, "authenticated as operator (admin)\n") || !strings.Contains(out, `"resources"`) {
		t.Fatalf("output: %q", out)
	}
	if strings.Join(requests, ",") != "GET /api/whoami,GET /api/resources" {
		t.Fatalf("requests: %v", requests)
	}
}

func TestProjectStatusAndPruneUseTheAPIRoutes(t *testing.T) {
	status := http.StatusNotFound
	var paths []string
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"project not found","code":"not_found"}`))
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "project.toml")
	writeFile(t, path, pruneSpecSource)
	out, err := captureStdout(t, func() error { return run([]string{"project", "prune", "--api", server.URL, path}) })
	if err != nil || !strings.Contains(out, "undeclared") {
		t.Fatalf("prune before apply: %v %q", err, out)
	}
	status = http.StatusForbidden
	if _, err := captureStdout(t, func() error { return run([]string{"project", "prune", "--api", server.URL, path}) }); err == nil || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("prune on 403: %v", err)
	}
	if _, err := captureStdout(t, func() error { return run([]string{"project", "status", "--api", server.URL, "p"}) }); err == nil || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("status on 403: %v", err)
	}
	for _, p := range paths {
		if p != "/api/projects/p" {
			t.Fatalf("paths: %v", paths)
		}
	}
}
