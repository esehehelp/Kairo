//go:build linux

package executor

import (
	"bytes"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"net/netip"
)

// gangMembers are n members on loopback addresses (127.0.0.11, .12, ...),
// as on a segment.
func gangMembers(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "127.0.0." + strconv.Itoa(11+i) + "/8"
	}
	return out
}

// Every member returns from the barrier once each has reached every other
// and been reached by it, whatever order they come in.
func TestGangBarrierPassesOnceEveryMemberReachedEveryOther(t *testing.T) {
	members := gangMembers(3)
	errs := make([]error, len(members))
	var wg sync.WaitGroup
	for r := range members {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// the last one comes a second late
			if r == len(members)-1 {
				time.Sleep(time.Second)
			}
			g := &containerGang{Members: members, Rank: r, Placement: "gpl_1", Address: members[r]}
			errs[r] = gangBarrier(g, netip.MustParsePrefix(members[r]).Addr(), io.Discard, nil, 20*time.Second)
		}()
	}
	wg.Wait()
	for r, err := range errs {
		if err != nil {
			t.Fatalf("member %d: %v", r, err)
		}
	}
}

// A member that never comes holds the others at the barrier until it gives
// up, saying who was not reached; a hello of another placement is not one
// of this gang's.
func TestGangBarrierGivesUpOnAMissingMember(t *testing.T) {
	members := gangMembers(2)
	// member 1's address answers with another placement's hello only
	stranger, err := net.Listen("tcp", net.JoinHostPort("127.0.0.12", strconv.Itoa(gangBarrierPort)))
	if err != nil {
		t.Fatal(err)
	}
	defer stranger.Close()
	go func() {
		for {
			c, err := stranger.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(100 * time.Millisecond):
			}
			c, err := net.Dial("tcp", net.JoinHostPort("127.0.0.11", strconv.Itoa(gangBarrierPort)))
			if err != nil {
				continue
			}
			_, _ = io.WriteString(c, gangHello("gpl_other", 1))
			c.Close()
		}
	}()
	var logs bytes.Buffer
	g := &containerGang{Members: members, Rank: 0, Placement: "gpl_1", Address: members[0]}
	err = gangBarrier(g, netip.MustParsePrefix(members[0]).Addr(), &logs, nil, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "members none not reached, members 1 not heard from") {
		t.Fatalf("barrier with a stranger: %v", err)
	}
}

// A stop at the barrier ends it.
func TestGangBarrierEndsOnAStop(t *testing.T) {
	members := gangMembers(2)
	stop := make(chan os.Signal, 1)
	stop <- os.Interrupt
	g := &containerGang{Members: members, Rank: 0, Placement: "gpl_1", Address: members[0]}
	if err := gangBarrier(g, netip.MustParsePrefix(members[0]).Addr(), io.Discard, stop, time.Minute); err != errStoppedAtBarrier {
		t.Fatalf("stopped barrier: %v", err)
	}
}

// The init gives an interface an address and brings it up over rtnetlink
// (no ip(8) in the image): here loopback, in the test's own namespace.
func TestAnInterfaceIsGivenItsAddressOverNetlink(t *testing.T) {
	lo, err := net.InterfaceByName("lo")
	if err != nil {
		t.Skip("no loopback interface:", err)
	}
	want := netip.MustParsePrefix("127.0.0.99/8")
	if err := addAddress(lo.Index, want); err != nil {
		if os.Geteuid() != 0 || strings.Contains(err.Error(), "not permitted") {
			t.Skip("cannot change interfaces here:", err)
		}
		t.Fatal(err)
	}
	if name, ok := interfaceWith(want.Addr()); !ok || name != "lo" {
		t.Fatalf("address on %q (%v)", name, ok)
	}
	if err := addAddress(lo.Index, want); err == nil {
		t.Fatal("the same address given twice")
	}
	if err := setLinkUp(lo.Index); err != nil {
		t.Fatal(err)
	}
}
