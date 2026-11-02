package provider

import (
	"context"
	"errors"
	"testing"
)

// Graphics processes explain a desktop's share of GPU memory only: beyond the
// allowance (3 GiB by default) the memory is unattributed activity, as for
// a game or a D3D/Vulkan job that pmon shows only as graphics.
func TestNVIDIAGraphicsExplainOnlyTheDesktopsShareOfVRAM(t *testing.T) {
	explain := true
	for _, tc := range []struct {
		name      string
		freeMiB   string
		allowance int64
		wantClaim bool
	}{
		{"desktop", "8900", 0, false},           // 1.3 GiB used
		{"large graphics use", "4200", 0, true}, // 5.9 GiB used
		{"raised allowance", "4200", 8 << 30, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &NVIDIA{ProviderID: "p", NodeID: "n", GraphicsExplainVRAM: &explain, GraphicsVRAMAllowance: tc.allowance,
				ProcessIdentity: func(int) (string, error) { return "", errors.New("unused") },
				Command: func(_ context.Context, args ...string) ([]byte, error) {
					if args[0] == "pmon" {
						return []byte("# gpu pid type fb ccpm command\n# Idx # C/G MB MB name\n    0  14628  C+G  0  0  explorer.exe\n    0  21000  G  0  0  strata.exe\n"), nil
					}
					return queryAnswer(args, inventory("GPU-a, 0, 10240, "+tc.freeMiB+", 68, 70")), nil
				}}
			snap, err := p.Observe(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got := false
			for _, c := range snap.Claims {
				if c.ClaimKind == "unattributed_activity" {
					got = true
				}
			}
			if got != tc.wantClaim {
				t.Fatalf("unattributed claim = %v, want %v: %+v", got, tc.wantClaim, snap.Claims)
			}
		})
	}
}
