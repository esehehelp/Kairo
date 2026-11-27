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

// What Kairo's work holds in the wslc VM (KairoVRAM, by PCI address) is
// Kairo's: on Windows nvidia-smi shows none of the VM's processes, only the
// memory they hold. The rest is weighed as before; memory Kairo's work
// cannot be told to hold (no reading, another GPU) stays unattributed.
func TestNVIDIAMemoryKairosWSLCVMHoldsIsNotUnattributed(t *testing.T) {
	explain := true
	for _, tc := range []struct {
		name      string
		held      map[string]int64
		err       error
		wantClaim bool
	}{
		{"the VM holds it", map[string]int64{"2b:00.0": 5 << 30}, nil, false},
		{"another GPU's", map[string]int64{"01:00.0": 5 << 30}, nil, true},
		{"not read", nil, errors.New("no counter"), true},
		{"the VM holds part", map[string]int64{"2b:00.0": 1 << 30}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &NVIDIA{ProviderID: "p", NodeID: "n", GraphicsExplainVRAM: &explain,
				KairoVRAM:       func() (map[string]int64, error) { return tc.held, tc.err },
				ProcessIdentity: func(int) (string, error) { return "", errors.New("unused") },
				Command: func(_ context.Context, args ...string) ([]byte, error) {
					if args[0] == "pmon" {
						return []byte("# gpu pid type fb ccpm command\n# Idx # C/G MB MB name\n    0  14628  C+G  0  0  explorer.exe\n"), nil
					}
					// 6.3 GiB used
					return queryAnswer(args, inventory("GPU-a, 0, 10240, 3800, 83, 70", "pci.bus_id=00000000:2B:00.0")), nil
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
			if tc.name == "the VM holds it" && string(snap.Observations[0].Evidence) != `{"kairo_vm_bytes":5368709120,"source":"nvidia-smi"}` {
				t.Fatalf("evidence %s", snap.Observations[0].Evidence)
			}
		})
	}
}

func TestNVIDIAPCIAddressIsTheBusDeviceAndFunction(t *testing.T) {
	for in, want := range map[string]string{"00000000:2B:00.0": "2b:00.0", "0000:01:00.0": "01:00.0", "[N/A]": "", "01:00": ""} {
		got, _ := nvidiaPCIAddress(map[string]string{"pci.bus_id": in})
		if got != want {
			t.Fatalf("%s: %q, want %q", in, got, want)
		}
	}
}
