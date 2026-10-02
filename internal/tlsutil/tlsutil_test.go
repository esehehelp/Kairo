package tlsutil

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func newMaterial(t *testing.T, hosts ...string) string {
	t.Helper()
	dir := t.TempDir()
	if err := InitCA(dir); err != nil {
		t.Fatalf("InitCA: %v", err)
	}
	if err := IssueServer(dir, hosts); err != nil {
		t.Fatalf("IssueServer: %v", err)
	}
	return dir
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func parseCert(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(readFile(t, path))
	if block == nil {
		t.Fatalf("%s: no PEM block", path)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// startServer serves 200 OK over TLS with the server certificate in dir and
// returns the server URL rewritten to 127.0.0.1.
func startServer(t *testing.T, dir string) string {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, ServerCertFile), filepath.Join(dir, ServerKeyFile))
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	server.TLS = ServerConfig()
	server.TLS.Certificates = []tls.Certificate{cert}
	server.StartTLS()
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return "https://127.0.0.1:" + u.Port()
}

func get(t *testing.T, caPEM []byte, target string) (*http.Response, error) {
	t.Helper()
	cfg, err := ClientConfig(caPEM)
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
	t.Cleanup(client.CloseIdleConnections)
	resp, err := client.Get(target)
	if err == nil {
		resp.Body.Close()
	}
	return resp, err
}

func TestClientTrustsIssuedServer(t *testing.T) {
	dir := newMaterial(t, "127.0.0.1", "localhost")
	target := startServer(t, dir)
	resp, err := get(t, readFile(t, filepath.Join(dir, CACertFile)), target)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if resp.TLS == nil || resp.TLS.Version != tls.VersionTLS13 {
		t.Fatalf("connection is not TLS 1.3: %+v", resp.TLS)
	}
}

func TestOtherCAIsRejected(t *testing.T) {
	dir := newMaterial(t, "127.0.0.1")
	other := newMaterial(t, "127.0.0.1")
	target := startServer(t, dir)
	if _, err := get(t, readFile(t, filepath.Join(other, CACertFile)), target); err == nil {
		t.Fatal("client accepted a server signed by a different CA")
	}
}

func TestHostMismatchIsRejected(t *testing.T) {
	dir := newMaterial(t, "localhost")
	target := startServer(t, dir)
	_, err := get(t, readFile(t, filepath.Join(dir, CACertFile)), target)
	if err == nil {
		t.Fatal("client accepted a certificate without a 127.0.0.1 SAN")
	}
	if !strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitCARefusesOverwriteAndProtectsKeys(t *testing.T) {
	dir := newMaterial(t, "127.0.0.1")
	before := readFile(t, filepath.Join(dir, CACertFile))
	if err := InitCA(dir); err == nil {
		t.Fatal("InitCA overwrote an existing CA")
	}
	if !bytes.Equal(before, readFile(t, filepath.Join(dir, CACertFile))) {
		t.Fatal("ca.pem changed after refused InitCA")
	}
	keyOnly := t.TempDir()
	if err := os.WriteFile(filepath.Join(keyOnly, CAKeyFile), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := InitCA(keyOnly); err == nil {
		t.Fatal("InitCA ran with an existing ca-key.pem")
	}
	if err := IssueServer(dir, nil); err == nil {
		t.Fatal("IssueServer accepted no hosts")
	}
	if runtime.GOOS == "windows" {
		t.Skip("file modes are not meaningful on Windows")
	}
	for _, name := range []string{CAKeyFile, ServerKeyFile} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if mode := info.Mode().Perm(); mode != 0o600 {
			t.Fatalf("%s mode %o, want 600", name, mode)
		}
	}
}

func TestCAEnvRoundTrip(t *testing.T) {
	dir := newMaterial(t, "127.0.0.1")
	caPEM := readFile(t, filepath.Join(dir, CACertFile))
	value, err := CAEnv(caPEM)
	if err != nil {
		t.Fatalf("CAEnv: %v", err)
	}
	if strings.ContainsAny(value, "\r\n") {
		t.Fatal("CAEnv value contains a newline")
	}
	back, err := CAFromEnv(value)
	if err != nil {
		t.Fatalf("CAFromEnv: %v", err)
	}
	if !bytes.Equal(back, caPEM) {
		t.Fatal("CAFromEnv did not reproduce ca.pem")
	}
	if _, err := LoadCertPool(back); err != nil {
		t.Fatalf("LoadCertPool: %v", err)
	}
	if _, err := LoadCertPool([]byte("not pem")); err == nil {
		t.Fatal("LoadCertPool accepted data without certificates")
	}
	if _, err := CAFromEnv("!!!"); err == nil {
		t.Fatal("CAFromEnv accepted invalid base64")
	}
}

func TestClientConfigDefaults(t *testing.T) {
	cfg, err := ClientConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion != tls.VersionTLS13 || cfg.RootCAs != nil {
		t.Fatalf("empty CA config: %+v", cfg)
	}
	if ServerConfig().MinVersion != tls.VersionTLS13 {
		t.Fatal("ServerConfig is not TLS 1.3")
	}
}

func TestCertificateExtensions(t *testing.T) {
	dir := newMaterial(t, "127.0.0.1", "kairo.example")
	ca := parseCert(t, filepath.Join(dir, CACertFile))
	leaf := parseCert(t, filepath.Join(dir, ServerCertFile))

	if !ca.IsCA || !ca.BasicConstraintsValid || !ca.MaxPathLenZero || ca.MaxPathLen != 0 {
		t.Fatalf("CA constraints: IsCA=%v valid=%v maxPathLen=%d zero=%v", ca.IsCA, ca.BasicConstraintsValid, ca.MaxPathLen, ca.MaxPathLenZero)
	}
	if len(ca.SubjectKeyId) == 0 {
		t.Fatal("CA has no SubjectKeyId")
	}
	if ca.KeyUsage&x509.KeyUsageCertSign == 0 || ca.KeyUsage&x509.KeyUsageCRLSign == 0 {
		t.Fatalf("CA key usage %v", ca.KeyUsage)
	}
	if !bytes.Equal(leaf.AuthorityKeyId, ca.SubjectKeyId) {
		t.Fatal("leaf AuthorityKeyId does not match CA SubjectKeyId")
	}
	if len(leaf.SubjectKeyId) == 0 {
		t.Fatal("leaf has no SubjectKeyId")
	}
	if leaf.IsCA || !leaf.BasicConstraintsValid {
		t.Fatal("leaf basic constraints are wrong")
	}
	if len(leaf.ExtKeyUsage) != 1 || leaf.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
		t.Fatalf("leaf ExtKeyUsage %v", leaf.ExtKeyUsage)
	}
	if leaf.KeyUsage != x509.KeyUsageDigitalSignature {
		t.Fatalf("leaf key usage %v", leaf.KeyUsage)
	}
	if leaf.Subject.CommonName != "127.0.0.1" {
		t.Fatalf("leaf CN %q", leaf.Subject.CommonName)
	}
	if len(leaf.IPAddresses) != 1 || leaf.IPAddresses[0].String() != "127.0.0.1" {
		t.Fatalf("leaf IPs %v", leaf.IPAddresses)
	}
	if len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != "kairo.example" {
		t.Fatalf("leaf DNS names %v", leaf.DNSNames)
	}
	if leaf.NotAfter.After(ca.NotAfter) {
		t.Fatal("leaf outlives the CA")
	}
	if err := leaf.CheckSignatureFrom(ca); err != nil {
		t.Fatalf("leaf not signed by CA: %v", err)
	}
}
