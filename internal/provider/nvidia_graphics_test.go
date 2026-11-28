package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"kairo/internal/store"
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
// Kairo's only while an attempt of Kairo's runs on the GPU, which the
// daemon knows: on Windows nvidia-smi shows none of the VM's processes, only
// the memory they hold. The memory is reported unattributed, and the claim
// says whether the VM's memory explains it (kairo_vm_explains): the daemon
// takes it off while the GPU's lease is live. Memory Kairo's work cannot
// be told to hold (no reading, another GPU) is not explained by it.
func TestNVIDIAMemoryKairosWSLCVMHoldsIsNotUnattributed(t *testing.T) {
	explain := true
	for _, tc := range []struct {
		name     string
		held     map[string]int64
		err      error
		explains string // the claim's kairo_vm_explains ("" for none)
	}{
		{"the VM holds it", map[string]int64{"2b:00.0": 5 << 30}, nil, "true"},
		{"another GPU's", map[string]int64{"01:00.0": 5 << 30}, nil, ""},
		{"not read", nil, errors.New("no counter"), ""},
		{"the VM holds part", map[string]int64{"2b:00.0": 1 << 30}, nil, "false"},
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
			var claims []store.ExternalClaim
			for _, c := range snap.Claims {
				if c.ClaimKind == "unattributed_activity" {
					claims = append(claims, c)
				}
			}
			if len(claims) != 1 {
				t.Fatalf("unattributed claims %+v", snap.Claims)
			}
			var evidence map[string]any
			if err := json.Unmarshal(claims[0].Evidence, &evidence); err != nil {
				t.Fatal(err)
			}
			got := ""
			if v, ok := evidence["kairo_vm_explains"]; ok {
				got = fmt.Sprint(v)
			}
			if got != tc.explains || evidence["used_memory_bytes"] == nil {
				t.Fatalf("evidence %s, want kairo_vm_explains %q", claims[0].Evidence, tc.explains)
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

// Without the process list a GPU's claims are unknown: pmon failing, in
// time or not, fails the observation rather than record the GPU unclaimed.
func TestNVIDIAWithoutTheProcessListObservesNothing(t *testing.T) {
	p := &NVIDIA{ProviderID: "p", NodeID: "n",
		KairoVRAM: func() (map[string]int64, error) { return nil, nil },
		Command: func(_ context.Context, args ...string) ([]byte, error) {
			if args[0] == "pmon" {
				return nil, errors.New("exit status 15: driver not loaded")
			}
			return queryAnswer(args, inventory("GPU-a, 0, 10240, 9000, 83, 70")), nil
		}}
	if _, err := p.Observe(context.Background()); err == nil {
		t.Fatal("observed without the process list")
	}
}
