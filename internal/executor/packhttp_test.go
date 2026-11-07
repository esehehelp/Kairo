package executor

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"kairo/internal/tlsutil"
)

// A pack server answers in HTTP/1.1 a client whose TLS settings offer
// HTTP/2 (copied from the daemon client's, whose HTTP/2 support filled them
// in) but whose transport does not speak it: the agents built before
// PackHTTPClient fetch so. PackHTTPClient itself offers HTTP/1.1 alone.
func TestPackServerAndClientSpeakHTTP1(t *testing.T) {
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
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := PackHTTPServer(listener.Addr().String(), cert, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	go func() {
		if err := server.ServeTLS(listener, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	}()
	defer server.Close()
	url := "https://" + listener.Addr().String() + "/packs/obj_1.tar.zst"

	daemonTLS, err := tlsutil.ClientConfig(caPEM)
	if err != nil {
		t.Fatal(err)
	}
	daemonTLS.NextProtos = []string{"h2", "http/1.1"} // as the daemon client's HTTP/2 support leaves it
	old := &http.Client{Transport: &http.Transport{TLSClientConfig: daemonTLS.Clone()}}
	for name, client := range map[string]*http.Client{"an agent built before": old, "PackHTTPClient": PackHTTPClient(daemonTLS)} {
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden || resp.ProtoMajor != 1 {
			t.Fatalf("%s: %s %s", name, resp.Proto, resp.Status)
		}
	}
}
