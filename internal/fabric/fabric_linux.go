//go:build linux

package fabric

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// nlLink is the interface through ip (iproute2) and WireGuard's netlink
// (wgctrl): wireguard-tools are not needed (a wslc VM and some cloud images
// have none).
type nlLink struct{}

const hostOnFabric = true

func newLink() link { return nlLink{} }

func run(ctx context.Context, name string, args ...string) ([]byte, error) {
	var out, errOut bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(errOut.String()))
	}
	return out.Bytes(), nil
}

// keepalive: every peer is sent a keepalive this often, so that a NAT on
// either side keeps the mapping (a node behind one reaches out itself).
const keepalive = 25 * time.Second

func configure(name string, cfg wgtypes.Config) error {
	c, err := wgctrl.New()
	if err != nil {
		return err
	}
	defer c.Close()
	return c.ConfigureDevice(name, cfg)
}

// readKey reads a private key (base64, as wg reads it) from keyFile ("-":
// standard input, so that it is not on a command line).
func readKey(keyFile string) (wgtypes.Key, error) {
	var b []byte
	var err error
	if keyFile == "-" {
		var buf bytes.Buffer
		_, err = buf.ReadFrom(os.Stdin)
		b = buf.Bytes()
	} else {
		b, err = os.ReadFile(keyFile)
	}
	if err != nil {
		return wgtypes.Key{}, err
	}
	return wgtypes.ParseKey(strings.TrimSpace(string(b)))
}

func (nlLink) ensure(ctx context.Context, name, address string, bits int, keyFile string, port int) error {
	if _, err := run(ctx, "ip", "link", "show", "dev", name); err != nil {
		if _, err := run(ctx, "ip", "link", "add", "dev", name, "type", "wireguard"); err != nil {
			return err
		}
	}
	key, err := readKey(keyFile)
	if err != nil {
		return fmt.Errorf("fabric key: %w", err)
	}
	if err := configure(name, wgtypes.Config{PrivateKey: &key, ListenPort: &port}); err != nil {
		return fmt.Errorf("wireguard %s: %w", name, err)
	}
	if _, err := run(ctx, "ip", "-4", "address", "replace", address+"/"+strconv.Itoa(bits), "dev", name); err != nil {
		return err
	}
	_, err = run(ctx, "ip", "link", "set", "dev", name, "mtu", strconv.Itoa(MTU), "up")
	return err
}

func (nlLink) peers(ctx context.Context, name string) (map[string]peerSeen, error) {
	c, err := wgctrl.New()
	if err != nil {
		return nil, err
	}
	defer c.Close()
	d, err := c.Device(name)
	if err != nil {
		return nil, fmt.Errorf("wireguard %s: %w", name, err)
	}
	peers := map[string]peerSeen{}
	for _, p := range d.Peers {
		var seen peerSeen
		if p.LastHandshakeTime.Unix() > 0 {
			seen.handshake = p.LastHandshakeTime
		}
		if p.Endpoint != nil {
			seen.endpoint = p.Endpoint.String()
		}
		peers[p.PublicKey.String()] = seen
	}
	return peers, nil
}

func (nlLink) setPeer(ctx context.Context, name, publicKey, address, endpoint string) error {
	key, err := wgtypes.ParseKey(publicKey)
	if err != nil {
		return fmt.Errorf("peer key %q: %w", publicKey, err)
	}
	ip := net.ParseIP(address).To4()
	if ip == nil {
		return fmt.Errorf("peer address %q: not IPv4", address)
	}
	interval := keepalive
	peer := wgtypes.PeerConfig{PublicKey: key, PersistentKeepaliveInterval: &interval, ReplaceAllowedIPs: true,
		AllowedIPs: []net.IPNet{{IP: ip, Mask: net.CIDRMask(32, 32)}}}
	if endpoint != "" {
		if peer.Endpoint, err = net.ResolveUDPAddr("udp", endpoint); err != nil {
			return fmt.Errorf("peer endpoint %q: %w", endpoint, err)
		}
	}
	return configure(name, wgtypes.Config{Peers: []wgtypes.PeerConfig{peer}})
}

func (nlLink) removePeer(ctx context.Context, name, publicKey string) error {
	key, err := wgtypes.ParseKey(publicKey)
	if err != nil {
		return err
	}
	return configure(name, wgtypes.Config{Peers: []wgtypes.PeerConfig{{PublicKey: key, Remove: true}}})
}

// segmentDevice is what a container joins segment vni by: its bridge (an
// Incus bridged NIC's parent).
func segmentDevice(names string, vni int) string {
	bridge, _ := segmentDevices(names, vni)
	return bridge
}
