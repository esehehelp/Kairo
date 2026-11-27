//go:build linux

package fabric

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// cmdLink is the interface through ip (iproute2) and wg (wireguard-tools).
type cmdLink struct{}

func newLink() link { return cmdLink{} }

func run(ctx context.Context, name string, args ...string) ([]byte, error) {
	var out, errOut bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(errOut.String()))
	}
	return out.Bytes(), nil
}

func (cmdLink) ensure(ctx context.Context, name, address string, bits int, keyFile string, port int) error {
	if _, err := run(ctx, "ip", "link", "show", "dev", name); err != nil {
		if _, err := run(ctx, "ip", "link", "add", "dev", name, "type", "wireguard"); err != nil {
			return err
		}
	}
	if _, err := run(ctx, "wg", "set", name, "listen-port", strconv.Itoa(port), "private-key", keyFile); err != nil {
		return err
	}
	if _, err := run(ctx, "ip", "-4", "address", "replace", address+"/"+strconv.Itoa(bits), "dev", name); err != nil {
		return err
	}
	_, err := run(ctx, "ip", "link", "set", "dev", name, "mtu", strconv.Itoa(MTU), "up")
	return err
}

// peers reads wg show NAME latest-handshakes ("key<TAB>unix seconds") and
// endpoints ("key<TAB>host:port", "(none)" when unknown).
func (cmdLink) peers(ctx context.Context, name string) (map[string]peerSeen, error) {
	out, err := run(ctx, "wg", "show", name, "latest-handshakes")
	if err != nil {
		return nil, err
	}
	peers := map[string]peerSeen{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		var seen peerSeen
		if s, err := strconv.ParseInt(f[1], 10, 64); err == nil && s > 0 {
			seen.handshake = time.Unix(s, 0)
		}
		peers[f[0]] = seen
	}
	out, err = run(ctx, "wg", "show", name, "endpoints")
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 || f[1] == "(none)" {
			continue
		}
		seen := peers[f[0]]
		seen.endpoint = f[1]
		peers[f[0]] = seen
	}
	return peers, nil
}

func (cmdLink) setPeer(ctx context.Context, name, publicKey, address, endpoint string) error {
	_, err := run(ctx, "wg", "set", name, "peer", publicKey, "allowed-ips", address+"/32", "endpoint", endpoint, "persistent-keepalive", "25")
	return err
}

func (cmdLink) removePeer(ctx context.Context, name, publicKey string) error {
	_, err := run(ctx, "wg", "set", name, "peer", publicKey, "remove")
	return err
}
