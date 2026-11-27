//go:build linux

package fabric

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// LinkCommand is `kairo __kairo-fabric-link OP ARGS…` (LinkArg): the link's work done by this
// process for another (the Windows agent, in its wslc VM: fabric_windows.go).
// What it reads (a key) is on standard input; what it says is JSON.
//
//	ensure NAME ADDRESS BITS PORT    the interface, its key on stdin
//	peers NAME                       {key: {"handshake_unix", "endpoint"}}
//	set-peer NAME KEY ADDRESS [ENDPOINT]
//	remove-peer NAME KEY
//	underlay                         {"interface", "address", "mtu"} of the default route
//	set-mtu INTERFACE MTU
//	probe HOST:PORT                  {"rtt_ms"} (null: no connect)
func LinkCommand(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("fabric-link: no operation")
	}
	l := nlLink{}
	need := func(n int) error {
		if len(args)-1 < n {
			return fmt.Errorf("fabric-link %s: %d arguments needed", args[0], n)
		}
		return nil
	}
	say := func(v any) error { return json.NewEncoder(stdout).Encode(v) }
	switch args[0] {
	case "ensure":
		if err := need(4); err != nil {
			return err
		}
		bits, err1 := strconv.Atoi(args[3])
		port, err2 := strconv.Atoi(args[4])
		if err := errors.Join(err1, err2); err != nil {
			return err
		}
		return l.ensure(ctx, args[1], args[2], bits, "-", port)
	case "peers":
		if err := need(1); err != nil {
			return err
		}
		peers, err := l.peers(ctx, args[1])
		if err != nil {
			return err
		}
		out := map[string]vmPeer{}
		for k, p := range peers {
			v := vmPeer{Endpoint: p.endpoint}
			if !p.handshake.IsZero() {
				v.HandshakeUnix = p.handshake.Unix()
			}
			out[k] = v
		}
		return say(out)
	case "set-peer":
		if err := need(3); err != nil {
			return err
		}
		endpoint := ""
		if len(args) > 4 {
			endpoint = args[4]
		}
		return l.setPeer(ctx, args[1], args[2], args[3], endpoint)
	case "remove-peer":
		if err := need(2); err != nil {
			return err
		}
		return l.removePeer(ctx, args[1], args[2])
	case "underlay":
		u, err := underlay(ctx)
		if err != nil {
			return err
		}
		return say(u)
	case "set-mtu":
		if err := need(2); err != nil {
			return err
		}
		if _, err := strconv.Atoi(args[2]); err != nil {
			return err
		}
		_, err := run(ctx, "ip", "link", "set", "dev", args[1], "mtu", args[2])
		return err
	case "probe":
		if err := need(1); err != nil {
			return err
		}
		return say(vmProbe{RTTMillis: probe(ctx, args[1])})
	}
	return fmt.Errorf("fabric-link: unknown operation %q", args[0])
}

// underlay is the interface of the default IPv4 route: its first IPv4
// address and its MTU.
func underlay(ctx context.Context) (vmUnderlay, error) {
	out, err := run(ctx, "ip", "-4", "-o", "route", "show", "default")
	if err != nil {
		return vmUnderlay{}, err
	}
	f := strings.Fields(string(out))
	dev := ""
	for i := 0; i+1 < len(f); i++ {
		if f[i] == "dev" {
			dev = f[i+1]
			break
		}
	}
	if dev == "" {
		return vmUnderlay{}, errors.New("no default route")
	}
	iface, err := net.InterfaceByName(dev)
	if err != nil {
		return vmUnderlay{}, err
	}
	u := vmUnderlay{Interface: dev, MTU: iface.MTU}
	addrs, _ := iface.Addrs()
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && n.IP.To4() != nil {
			u.Address = n.IP.String()
			break
		}
	}
	return u, nil
}
