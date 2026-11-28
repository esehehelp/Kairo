//go:build linux

package executor

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// errStoppedAtBarrier: the container was stopped before its argv started.
var errStoppedAtBarrier = errors.New("stopped before the argv started")

// gangJoin is a member's init on its segment: it finds its NIC there (one
// with its address: wslc's runtime gave it; else gangNIC, which it gives the
// address and brings up), then waits at the barrier. It returns the NIC's
// name.
func gangJoin(g *containerGang, logs io.Writer, stop <-chan os.Signal) (string, error) {
	want, err := netip.ParsePrefix(g.Address)
	if err != nil || !want.Addr().Is4() {
		return "", fmt.Errorf("member address %q: %v", g.Address, err)
	}
	if g.Rank < 0 || g.Rank >= len(g.Members) || g.Members[g.Rank] != g.Address {
		return "", fmt.Errorf("member %d is not at %s among %v", g.Rank, g.Address, g.Members)
	}
	deadline := time.Now().Add(gangAddressWait)
	iface := ""
	for iface == "" {
		if name, ok := interfaceWith(want.Addr()); ok {
			iface = name
			break
		}
		if ifi, err := net.InterfaceByName(gangNIC); err == nil {
			if err := addAddress(ifi.Index, want); err != nil && !errors.Is(err, unix.EEXIST) {
				return "", fmt.Errorf("give %s the address %s: %w", gangNIC, want, err)
			}
			if err := setLinkUp(ifi.Index); err != nil {
				return "", fmt.Errorf("bring %s up: %w", gangNIC, err)
			}
			continue
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("no NIC with %s on the segment after %s", want, gangAddressWait)
		}
		select {
		case <-stop:
			return "", errStoppedAtBarrier
		case <-time.After(200 * time.Millisecond):
		}
	}
	fmt.Fprintf(logs, "kairo container: gang member %d of %d at %s (%s): waiting for the others\n", g.Rank, len(g.Members), want.Addr(), iface)
	if err := gangBarrier(g, want.Addr(), logs, stop, gangBarrierWait); err != nil {
		return "", err
	}
	fmt.Fprintf(logs, "kairo container: gang member %d: every member reached, both ways\n", g.Rank)
	return iface, nil
}

// interfaceWith names the interface that has addr.
func interfaceWith(addr netip.Addr) (string, bool) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", false
	}
	for _, ifi := range ifaces {
		addrs, _ := ifi.Addrs()
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok {
				if ip, ok := netip.AddrFromSlice(n.IP); ok && ip.Unmap() == addr {
					return ifi.Name, true
				}
			}
		}
	}
	return "", false
}

// gangHello is what a member says to another at the barrier.
func gangHello(placement string, rank int) string {
	return "kairo-gang " + placement + " " + strconv.Itoa(rank) + "\n"
}

// gangBarrier returns once this member (at self) has said hello to every
// other member on the segment and every other member has said hello to
// it; an error when limit passes first or it is stopped. A member that
// returned has been reached by every other member, which therefore reached
// it in turn: the others need nothing more from it, and its listener goes.
func gangBarrier(g *containerGang, self netip.Addr, logs io.Writer, stop <-chan os.Signal, limit time.Duration) error {
	ln, err := net.Listen("tcp", net.JoinHostPort(self.String(), strconv.Itoa(gangBarrierPort)))
	if err != nil {
		return fmt.Errorf("listen on the segment: %w", err)
	}
	defer ln.Close()
	// each member's address on the segment: a hello is a member's only
	// from it
	addrs := make([]netip.Addr, len(g.Members))
	for r, member := range g.Members {
		p, err := netip.ParsePrefix(member)
		if err != nil {
			return fmt.Errorf("member %d address %q: %v", r, member, err)
		}
		addrs[r] = p.Addr()
	}
	var mu sync.Mutex
	heard := map[int]bool{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				line, err := bufio.NewReader(io.LimitReader(conn, 256)).ReadString('\n')
				if err != nil {
					return
				}
				f := strings.Fields(line)
				if len(f) != 3 || f[0] != "kairo-gang" || f[1] != g.Placement {
					return
				}
				from, _ := netip.ParseAddrPort(conn.RemoteAddr().String())
				if r, err := strconv.Atoi(f[2]); err == nil && r >= 0 && r < len(g.Members) && r != g.Rank && from.Addr().Unmap() == addrs[r] {
					mu.Lock()
					heard[r] = true
					mu.Unlock()
					// the sender counts this member reached only on this
					_, _ = io.WriteString(conn, "ok\n")
				}
			}()
		}
	}()
	reached := map[int]bool{}
	deadline := time.Now().Add(limit)
	nextNote := time.Now().Add(time.Minute)
	for {
		// every member not reached at once, a few seconds at most: a round
		// takes no longer with many members, and a stop is seen between
		// rounds
		var targets []int
		for r := range g.Members {
			if r != g.Rank && !reached[r] {
				targets = append(targets, r)
			}
		}
		// each dial says its own result; reached is this goroutine's
		ok := make([]bool, len(targets))
		var round sync.WaitGroup
		for i, r := range targets {
			round.Add(1)
			go func() {
				defer round.Done()
				// from this member's address: its hello is taken from it only
				dialer := net.Dialer{Timeout: 2 * time.Second, LocalAddr: &net.TCPAddr{IP: self.AsSlice()}}
				conn, err := dialer.Dial("tcp", net.JoinHostPort(addrs[r].String(), strconv.Itoa(gangBarrierPort)))
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				// reached once it says it heard (a hello written may still be
				// lost with the connection)
				if _, err := io.WriteString(conn, gangHello(g.Placement, g.Rank)); err == nil {
					if ack, err := bufio.NewReader(io.LimitReader(conn, 8)).ReadString('\n'); err == nil && ack == "ok\n" {
						ok[i] = true
					}
				}
			}()
		}
		round.Wait()
		for i, r := range targets {
			if ok[i] {
				reached[r] = true
			}
		}
		mu.Lock()
		var notReached, notHeard []int
		for r := range g.Members {
			if r == g.Rank {
				continue
			}
			if !reached[r] {
				notReached = append(notReached, r)
			}
			if !heard[r] {
				notHeard = append(notHeard, r)
			}
		}
		mu.Unlock()
		if len(notReached) == 0 && len(notHeard) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("after %s: members %s not reached, members %s not heard from", limit, ranks(notReached), ranks(notHeard))
		}
		if time.Now().After(nextNote) {
			fmt.Fprintf(logs, "kairo container: gang member %d: still waiting: members %s not reached, members %s not heard from\n", g.Rank, ranks(notReached), ranks(notHeard))
			nextNote = time.Now().Add(time.Minute)
		}
		select {
		case <-stop:
			return errStoppedAtBarrier
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func ranks(rs []int) string {
	if len(rs) == 0 {
		return "none"
	}
	slices.Sort(rs)
	s := make([]string, len(rs))
	for i, r := range rs {
		s[i] = strconv.Itoa(r)
	}
	return strings.Join(s, ",")
}

// addAddress gives interface index the IPv4 address p (rtnetlink
// RTM_NEWADDR); setLinkUp brings it up (RTM_NEWLINK). The init does this as
// root in the container's own network namespace: the attempt is given no
// privilege, and the image needs no ip(8).
func addAddress(index int, p netip.Prefix) error {
	a := p.Addr().As4()
	msg := make([]byte, unix.SizeofIfAddrmsg)
	msg[0] = unix.AF_INET
	msg[1] = byte(p.Bits())
	binary.NativeEndian.PutUint32(msg[4:], uint32(index))
	msg = append(msg, rtattr(unix.IFA_LOCAL, a[:])...)
	msg = append(msg, rtattr(unix.IFA_ADDRESS, a[:])...)
	return netlinkRequest(unix.RTM_NEWADDR, unix.NLM_F_CREATE|unix.NLM_F_EXCL, msg)
}

func setLinkUp(index int) error {
	msg := make([]byte, unix.SizeofIfInfomsg)
	msg[0] = unix.AF_UNSPEC
	binary.NativeEndian.PutUint32(msg[4:], uint32(index))
	binary.NativeEndian.PutUint32(msg[8:], unix.IFF_UP)  // flags
	binary.NativeEndian.PutUint32(msg[12:], unix.IFF_UP) // change
	return netlinkRequest(unix.RTM_NEWLINK, 0, msg)
}

func rtattr(kind uint16, data []byte) []byte {
	n := unix.SizeofRtAttr + len(data)
	b := make([]byte, (n+3)&^3)
	binary.NativeEndian.PutUint16(b[0:], uint16(n))
	binary.NativeEndian.PutUint16(b[2:], kind)
	copy(b[unix.SizeofRtAttr:], data)
	return b
}

// netlinkRequest sends one rtnetlink request and reads its acknowledgement.
func netlinkRequest(kind uint16, flags uint16, body []byte) error {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_ROUTE)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return err
	}
	msg := make([]byte, unix.SizeofNlMsghdr, unix.SizeofNlMsghdr+len(body))
	binary.NativeEndian.PutUint32(msg[0:], uint32(unix.SizeofNlMsghdr+len(body)))
	binary.NativeEndian.PutUint16(msg[4:], kind)
	binary.NativeEndian.PutUint16(msg[6:], unix.NLM_F_REQUEST|unix.NLM_F_ACK|flags)
	binary.NativeEndian.PutUint32(msg[8:], 1) // sequence
	msg = append(msg, body...)
	if err := unix.Sendto(fd, msg, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return err
	}
	buf := make([]byte, 4096)
	for {
		n, _, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			return err
		}
		msgs, err := syscall.ParseNetlinkMessage(buf[:n])
		if err != nil {
			return err
		}
		for _, m := range msgs {
			if m.Header.Type == unix.NLMSG_ERROR && len(m.Data) >= 4 {
				if code := int32(binary.NativeEndian.Uint32(m.Data[:4])); code != 0 {
					return unix.Errno(-code)
				}
				return nil
			}
		}
	}
}

// apiOnLoopback puts the host of hostPort (the daemon's address on the
// fabric) on the container's lo, says so (ready: the runner then adds the
// proxy device listening there), and waits, up to limit, until the proxy
// answers.
func apiOnLoopback(hostPort, ready string, limit time.Duration) error {
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return err
	}
	a, err := netip.ParseAddr(host)
	if err != nil || !a.Is4() {
		return fmt.Errorf("%q is no IPv4 address", host)
	}
	lo, err := net.InterfaceByName("lo")
	if err != nil {
		return err
	}
	if err := addAddress(lo.Index, netip.PrefixFrom(a, 32)); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}
	if err := setLinkUp(lo.Index); err != nil {
		return err
	}
	if err := os.WriteFile(ready, []byte(hostPort+"\n"), 0o644); err != nil {
		return err
	}
	for deadline := time.Now().Add(limit); ; time.Sleep(200 * time.Millisecond) {
		if c, err := net.DialTimeout("tcp", hostPort, time.Second); err == nil {
			c.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no proxy at %s after %s", hostPort, limit)
		}
	}
}
