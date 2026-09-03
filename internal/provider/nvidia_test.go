package provider

import (
	"context"
	"strings"
	"testing"
)

func TestNVIDIAOnlyTreatsComputeProcessesAsClaims(t *testing.T) {
	p := &NVIDIA{ProviderID: "p", NodeID: "n", ProcessIdentity: func(pid int) (string, error) { return "pid:202:start:test", nil }, Command: func(_ context.Context, args ...string) ([]byte, error) {
		if args[0] == "pmon" {
			return []byte("# gpu pid type fb command\n0 101 C+G 0 explorer.exe\n1 202 C+G 0 python.exe\n"), nil
		}
		return []byte("GPU-a, 0, 8192, 7000, 2, 40\nGPU-b, 1, 24576, 12000, 90, 70\n"), nil
	}}
	snap, err := p.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Resources) != 2 || len(snap.Claims) != 1 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	if snap.Claims[0].ProcessIdentity == nil || *snap.Claims[0].ProcessIdentity != "pid:202:start:test" || !strings.Contains(string(snap.Claims[0].Evidence), `"process_type":"C+G"`) {
		t.Fatalf("wrong claim: %+v", snap.Claims[0])
	}
}
