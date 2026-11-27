package executor

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A sample is served to a peer on the fabric only.
func TestAFabricSampleIsServedToAPeerOnTheFabricOnly(t *testing.T) {
	p := &PackServer{Logger: slog.Default(), FabricPeer: func(host string) bool { return host == "10.66.0.2" }}
	for _, x := range []struct {
		remote string
		status int
	}{{"10.66.0.2:40000", http.StatusOK}, {"192.0.2.9:40000", http.StatusNotFound}} {
		r := httptest.NewRequest(http.MethodGet, FabricSamplePath, nil)
		r.RemoteAddr = x.remote
		w := httptest.NewRecorder()
		p.ServeHTTP(w, r)
		body, _ := io.ReadAll(w.Result().Body)
		if w.Code != x.status || (x.status == http.StatusOK && len(body) != FabricSampleBytes) {
			t.Fatalf("%s: %d, %d bytes", x.remote, w.Code, len(body))
		}
	}
	// a peer whose endpoint is off the fabric sends one
	r := httptest.NewRequest(http.MethodPost, FabricSamplePath, strings.NewReader(strings.Repeat("x", 5<<20)))
	r.RemoteAddr = "10.66.0.2:40000"
	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("a sample sent: %d", w.Code)
	}
}
