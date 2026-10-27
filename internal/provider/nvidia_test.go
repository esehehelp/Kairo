package provider

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"kairo/internal/processidentity"
	"kairo/internal/store"
)

func TestNVIDIAOnlyTreatsComputeProcessesAsClaims(t *testing.T) {
	p := &NVIDIA{ProviderID: "p", NodeID: "n", ProcessIdentity: func(pid int) (string, error) { return "pid:202:start:test", nil }, Command: func(_ context.Context, args ...string) ([]byte, error) {
		if args[0] == "pmon" {
			return []byte("# gpu pid type fb command\n0 101 C+G 0 explorer.exe\n1 202 C+G 0 python.exe\n"), nil
		}
		return queryAnswer(args, inventory("GPU-a, 0, 8192, 7600, 2, 40"), inventory("GPU-b, 1, 24576, 12000, 90, 70")), nil
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

// A run that never returns must fail the observation, naming the command that
// hung, after its timeout; no GPU is involved (the command is a fake).
func TestNVIDIAHungCommandFailsTheObservationNamingIt(t *testing.T) {
	for _, tc := range []struct{ hungArg, wantName string }{{"--query-gpu", "nvidia-smi inventory"}, {"pmon", "nvidia-smi pmon"}} {
		t.Run(tc.hungArg, func(t *testing.T) {
			p := &NVIDIA{ProviderID: "p", NodeID: "n", Timeout: 20 * time.Millisecond, Command: func(ctx context.Context, args ...string) ([]byte, error) {
				if strings.HasPrefix(args[0], tc.hungArg) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				if args[0] == "pmon" {
					return []byte("# gpu pid type fb command\n"), nil
				}
				return queryAnswer(args, inventory("GPU-a, 0, 8192, 7600, 2, 40")), nil
			}}
			_, err := p.Observe(context.Background())
			if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), tc.wantName) {
				t.Fatalf("hung %s must fail the observation naming %q: %v", tc.hungArg, tc.wantName, err)
			}
		})
	}
}

func TestNVIDIAPrepareMatchesPhysicalIdentityNotStoreResourceID(t *testing.T) {
	p := &NVIDIA{ProviderID: "p", NodeID: "n", Command: func(_ context.Context, args ...string) ([]byte, error) {
		if args[0] == "pmon" {
			return []byte("# gpu pid type fb command\n"), nil
		}
		return queryAnswer(args, inventory("GPU-a, 0, 12288, 12287, 0, 40")), nil
	}}
	for _, tc := range []struct {
		name, id, uuid, node, provider, kind string
		wantOK                               bool
	}{
		{"original", "gpu-a", "GPU-a", "n", "p", "gpu", true},
		{"shared", "gpu-a@n", "GPU-a", "n", "p", "gpu", true},
		{"missing", "gpu-a@n", "GPU-missing", "n", "p", "gpu", false},
		{"wrong-node", "gpu-a", "GPU-a", "other", "p", "gpu", false},
		{"wrong-provider", "gpu-a", "GPU-a", "n", "other", "gpu", false},
		{"wrong-kind", "gpu-a", "GPU-a", "n", "p", "cpu", false},
		{"empty-identity", "gpu-a", "", "n", "p", "gpu", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := p.Prepare(context.Background(), store.ResourceInstance{
				ID: tc.id, StableIdentity: tc.uuid, NodeID: tc.node, ProviderID: tc.provider, Kind: tc.kind,
			})
			if (err == nil) != tc.wantOK {
				t.Fatalf("Prepare() = %v, wantOK %v", err, tc.wantOK)
			}
		})
	}
}

func TestNVIDIAIdleVRAMFailsClosedAsUnattributedActivity(t *testing.T) {
	p := &NVIDIA{ProviderID: "p", NodeID: "n", Command: func(_ context.Context, args ...string) ([]byte, error) {
		if args[0] == "pmon" {
			return []byte("# gpu pid type fb command\n"), nil
		}
		return queryAnswer(args, inventory("GPU-a, 0, 24576, 20000, 0, 42")), nil
	}}
	snap, err := p.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Claims) != 1 || snap.Claims[0].ClaimKind != "unattributed_activity" {
		t.Fatalf("idle but occupied GPU must fail closed: %+v", snap.Claims)
	}
}

// Each GPU process claim carries the lineage the provider found for it, which
// lets the daemon recognise the work of the attempt holding the GPU; a process
// whose identity is unknown gets none.
func TestNVIDIAClaimsCarryTheProcessLineage(t *testing.T) {
	var asked []string
	p := &NVIDIA{ProviderID: "p", NodeID: "n",
		ProcessIdentity: func(pid int) (string, error) {
			if pid == 303 {
				return "", errors.New("gone")
			}
			return "proc:" + strconv.Itoa(pid) + ":starttime:1", nil
		},
		ProcessLineage: func(pid int, identity string) processidentity.Lineage {
			asked = append(asked, identity)
			if pid == 19774 {
				return processidentity.Lineage{AttemptID: "att_1", Ancestors: []string{"proc:19770:starttime:13733837", "proc:1:starttime:1"}}
			}
			return processidentity.Lineage{}
		},
		Command: func(_ context.Context, args ...string) ([]byte, error) {
			if args[0] == "pmon" {
				return []byte("# gpu pid type fb command\n0 19774 C 9000 maya\n0 202 C 100 python\n0 303 C 100 python\n"), nil
			}
			return queryAnswer(args, inventory("GPU-a, 0, 24576, 12000, 90, 70")), nil
		}}
	snap, err := p.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Claims) != 3 || !reflect.DeepEqual(asked, []string{"proc:19774:starttime:1", "proc:202:starttime:1"}) {
		t.Fatalf("claims %+v, lineage asked for %v", snap.Claims, asked)
	}
	descendant, foreign, unknown := snap.Claims[0], snap.Claims[1], snap.Claims[2]
	if descendant.AttemptID != "att_1" || !reflect.DeepEqual(descendant.Ancestors, []string{"proc:19770:starttime:13733837", "proc:1:starttime:1"}) || !strings.Contains(string(descendant.Evidence), `"attempt_id":"att_1"`) {
		t.Fatalf("descendant claim: %+v %s", descendant, descendant.Evidence)
	}
	if foreign.AttemptID != "" || foreign.Ancestors != nil || strings.Contains(string(foreign.Evidence), "attempt_id") {
		t.Fatalf("foreign claim: %+v %s", foreign, foreign.Evidence)
	}
	if *unknown.ProcessIdentity != "pid:303:start:unknown" || unknown.AttemptID != "" || unknown.Ancestors != nil {
		t.Fatalf("claim of an unidentified process: %+v", unknown)
	}
}
