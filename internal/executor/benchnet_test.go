package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kairo/internal/bench"
	"kairo/internal/store"
)

// benchNetCoordinator authorizes one network bench's token.
type benchNetCoordinator struct {
	*fakeCopies
	run, token string
}

func (f *benchNetCoordinator) AuthorizeBenchNet(_ context.Context, nodeID, runID, token string) error {
	if nodeID != "node" || runID != f.run || token != f.token {
		return store.ErrNotFound
	}
	return nil
}

// A network bench reaches the transfer endpoint as transfers do (TLS, the
// run's token) and the endpoint hands its connection to the bench's server:
// the workload measures through it. A wrong token is refused.
func TestANetworkBenchMeasuresThroughTheTransferEndpoint(t *testing.T) {
	fake := &benchNetCoordinator{fakeCopies: newFakeCopies(), run: "bnc_1", token: "kairo_bench_secret"}
	server := httptest.NewUnstartedServer(&PackServer{Store: fake, NodeID: "node", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	server.EnableHTTP2 = false
	server.StartTLS()
	defer server.Close()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	measure := func(token string) (map[string]any, error) {
		t.Setenv(bench.TokenEnv, token)
		cfg, err := bench.Defaults("net-latency")
		if err != nil {
			t.Fatal(err)
		}
		cfg.Seconds, cfg.Interval = 0.2, 0.05
		cfg.PackPeer, cfg.PackRun, cfg.PackCA = strings.TrimPrefix(server.URL, "https://"), "bnc_1", caFile
		var out bytes.Buffer
		if err := bench.Run(context.Background(), &out, cfg); err != nil {
			return nil, err
		}
		var end map[string]any
		for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
			var rec map[string]any
			if json.Unmarshal([]byte(line), &rec) == nil && rec["type"] == "end" {
				end = rec
			}
		}
		return end, nil
	}
	end, err := measure("kairo_bench_secret")
	if err != nil {
		t.Fatal(err)
	}
	if end == nil || end["work"].(map[string]any)["request"].(float64) == 0 {
		t.Fatalf("end %v", end)
	}
	if _, err := measure("kairo_bench_wrong"); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("a wrong token: %v", err)
	}
	// the endpoint's packs are still its packs
	resp, err := server.Client().Get(server.URL + "/packs/obj_1.tar.zst")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("a pack without a token: %s", resp.Status)
	}
}
