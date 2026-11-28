//go:build windows

package fabric

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/windows"

	"kairo/internal/store"
)

// On Windows, attempts run in wslc containers (container_wslc.go), so the
// fabric's interface is made where they are: in the wslc session's VM, as
// root there (`wslc system session run`), by the Linux build of this binary
// (kairo-linux beside it, copied into the VM: LinkArg). The
// agent's own sockets are not on the fabric (HostOnFabric).
//
// The VM takes no connection from outside (the Hyper-V firewall) and its
// datagrams leave with another source port: a node behind a NAT. It is
// given no endpoints and reaches out to its peers itself (keepalive).

const hostOnFabric = false

const (
	vmDir    = "/var/lib/kairo/fabric"
	vmBinary = vmDir + "/kairo"
)

type vmLink struct {
	mu        sync.Mutex
	installed bool
}

func newLink() link { return &vmLink{} }

func wslcExe() string {
	if p, err := exec.LookPath("wslc"); err == nil {
		return p
	}
	return filepath.Join(os.Getenv("ProgramFiles"), "WSL", "wslc.exe")
}

// errNotThere: the command is not in the VM (a VM made anew).
var errNotThere = errors.New("not in the VM")

// sessionRun runs argv in the wslc session's VM. Its standard input is
// always a pipe: wslc fails on none.
func sessionRun(ctx context.Context, stdin io.Reader, argv ...string) ([]byte, error) {
	if stdin == nil {
		stdin = strings.NewReader("")
	}
	cmd := exec.CommandContext(ctx, wslcExe(), append([]string{"system", "session", "run"}, argv...)...)
	cmd.Stdin = stdin
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NO_WINDOW}
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errOut.String() + " " + out.String())
		if strings.Contains(msg, "Errno = 2") || strings.Contains(msg, "not found") {
			return nil, fmt.Errorf("%w: %s", errNotThere, msg)
		}
		return nil, fmt.Errorf("wslc session run %s: %w: %s", argv[0], err, msg)
	}
	return out.Bytes(), nil
}

// linuxBinary is kairo-linux beside this binary.
func linuxBinary() (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(self), "kairo-linux"), nil
}

// install copies kairo-linux into the VM unless it is there already.
func (v *vmLink) install(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.installed {
		return nil
	}
	bin, err := linuxBinary()
	if err != nil {
		return err
	}
	f, err := os.Open(bin)
	if err != nil {
		return fmt.Errorf("the fabric in the wslc VM needs the Linux build beside the agent: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	there, _ := sessionRun(ctx, nil, "sh", "-c", "sha256sum "+vmBinary+" 2>/dev/null")
	if !strings.HasPrefix(string(there), sum) {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if _, err := sessionRun(ctx, f, "sh", "-c", "mkdir -p "+vmDir+" && cat > "+vmBinary+".new && chmod 755 "+vmBinary+".new && mv -f "+vmBinary+".new "+vmBinary); err != nil {
			return fmt.Errorf("kairo-linux into the wslc VM: %w", err)
		}
		there, err = sessionRun(ctx, nil, "sh", "-c", "sha256sum "+vmBinary)
		if err != nil || !strings.HasPrefix(string(there), sum) {
			return fmt.Errorf("kairo-linux in the wslc VM is not what was copied: %v", err)
		}
	}
	v.installed = true
	return nil
}

// do runs `kairo __kairo-fabric-link ARGS…` in the VM (copying the binary there
// first, and again when the VM was made anew).
func (v *vmLink) do(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	for attempt := 0; ; attempt++ {
		if err := v.install(ctx); err != nil {
			return nil, err
		}
		out, err := sessionRun(ctx, bytes.NewReader(stdin), append([]string{vmBinary, LinkArg}, args...)...)
		if errors.Is(err, errNotThere) && attempt == 0 {
			v.mu.Lock()
			v.installed = false
			v.mu.Unlock()
			continue
		}
		return out, err
	}
}

// matchUnderlay sets the VM's interface to the MTU of the Windows adapter
// that has the same address: the VM's is less (1280, as a 1280 adapter
// elsewhere on the machine), and a peer's frames over it never arrive.
func (v *vmLink) matchUnderlay(ctx context.Context) error {
	out, err := v.do(ctx, nil, "underlay")
	if err != nil {
		return err
	}
	var u vmUnderlay
	if err := json.Unmarshal(out, &u); err != nil {
		return fmt.Errorf("underlay: %w", err)
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return err
	}
	for _, iface := range ifaces {
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.String() == u.Address && iface.MTU > u.MTU {
				_, err := v.do(ctx, nil, "set-mtu", u.Interface, strconv.Itoa(iface.MTU))
				return err
			}
		}
	}
	return nil
}

func (v *vmLink) ensure(ctx context.Context, name, address string, bits int, keyFile string, port int) error {
	if err := v.matchUnderlay(ctx); err != nil {
		return fmt.Errorf("the wslc VM's MTU: %w", err)
	}
	key, err := os.ReadFile(keyFile)
	if err != nil {
		return err
	}
	_, err = v.do(ctx, key, "ensure", name, address, strconv.Itoa(bits), strconv.Itoa(port))
	return err
}

func (v *vmLink) peers(ctx context.Context, name string) (map[string]peerSeen, error) {
	out, err := v.do(ctx, nil, "peers", name)
	if err != nil {
		return nil, err
	}
	var got map[string]vmPeer
	if err := json.Unmarshal(out, &got); err != nil {
		return nil, fmt.Errorf("peers: %w", err)
	}
	peers := map[string]peerSeen{}
	for k, p := range got {
		seen := peerSeen{endpoint: p.Endpoint}
		if p.HandshakeUnix > 0 {
			seen.handshake = time.Unix(p.HandshakeUnix, 0)
		}
		peers[k] = seen
	}
	return peers, nil
}

func (v *vmLink) setPeer(ctx context.Context, name, publicKey, address, endpoint string) error {
	args := []string{"set-peer", name, publicKey, address}
	if endpoint != "" {
		args = append(args, endpoint)
	}
	_, err := v.do(ctx, nil, args...)
	return err
}

func (v *vmLink) removePeer(ctx context.Context, name, publicKey string) error {
	_, err := v.do(ctx, nil, "remove-peer", name, publicKey)
	return err
}

func (v *vmLink) probeFrom(ctx context.Context, addr string) *float64 {
	out, err := v.do(ctx, nil, "probe", addr)
	if err != nil {
		return nil
	}
	var p vmProbe
	if json.Unmarshal(out, &p) != nil {
		return nil
	}
	return p.RTTMillis
}

// sampleFrom fetches a peer's sample (or sends it one) from the VM, where
// the fabric is.
func (v *vmLink) sampleFrom(ctx context.Context, url string, send bool) (int64, time.Duration, error) {
	op := "sample"
	if send {
		op = "send"
	}
	out, err := v.do(ctx, nil, op, url)
	if err != nil {
		return 0, 0, err
	}
	var s vmSample
	if err := json.Unmarshal(out, &s); err != nil {
		return 0, 0, fmt.Errorf("sample: %w", err)
	}
	return s.Bytes, time.Duration(s.Seconds * float64(time.Second)), nil
}

// LinkCommand: the fabric-link command is Linux's (linkcmd_linux.go), run
// in the VM.
func LinkCommand(context.Context, []string, io.Writer) error {
	return errors.New("fabric-link runs on Linux (in the wslc VM, kairo-linux)")
}

// wslcCommand runs wslc.exe args (a pipe for its standard input, as wslc
// needs one).
func wslcCommand(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, wslcExe(), args...)
	cmd.Stdin = strings.NewReader("")
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NO_WINDOW}
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("wslc %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errOut.String()+" "+out.String()))
	}
	return out.Bytes(), nil
}

// wslcNetworks are the names of the session's networks.
func wslcNetworks(ctx context.Context) ([]string, error) {
	out, err := wslcCommand(ctx, "network", "list")
	if err != nil {
		return nil, err
	}
	var names []string
	for i, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Fields(line)
		if i == 0 || len(f) < 2 {
			continue // the header
		}
		names = append(names, f[1])
	}
	return names, nil
}

// On a wslc node a segment's bridge is a wslc network's (a container joins
// the network to be on the segment): made with that bridge's name, no
// address on it (inhibit_ipv4: the VM is not on the segment), internal
// (nothing routed out of it). The VXLAN device is made in the VM and put
// on that bridge.

func (v *vmLink) segments(ctx context.Context, names string) ([]int, error) {
	out, err := v.do(ctx, nil, "segments", names)
	if err != nil {
		return nil, err
	}
	var vnis []int
	if err := json.Unmarshal(out, &vnis); err != nil {
		return nil, fmt.Errorf("segments: %w", err)
	}
	have := map[int]bool{}
	for _, vni := range vnis {
		have[vni] = true
	}
	// networks whose VXLAN device is gone (a VM made anew, a removal half
	// done) are segments here too
	nets, err := wslcNetworks(ctx)
	if err != nil {
		return nil, err
	}
	for _, n := range nets {
		if s, ok := strings.CutPrefix(n, names+"-seg-"); ok {
			if vni, err := strconv.Atoi(s); err == nil && !have[vni] {
				have[vni] = true
				vnis = append(vnis, vni)
			}
		}
	}
	return vnis, nil
}

func (v *vmLink) ensureSegment(ctx context.Context, names string, seg store.FabricSegment, local string, port int) error {
	network := SegmentNetwork(names, seg.VNI)
	bridge, _ := segmentDevices(names, seg.VNI)
	nets, err := wslcNetworks(ctx)
	if err != nil {
		return err
	}
	if !slices.Contains(nets, network) {
		// docker keeps a gateway address out of what it hands containers
		// (the first host unless told): the subnet's last, so that gang
		// members have theirs from the first on (store/gang.go)
		subnet, err := netip.ParsePrefix(seg.Subnet)
		if err != nil || !subnet.Addr().Is4() || subnet.Bits() > 30 {
			return fmt.Errorf("segment %d subnet %q", seg.VNI, seg.Subnet)
		}
		last := subnet.Masked().Addr().As4()
		hosts := uint32(1)<<(32-subnet.Bits()) - 2
		n := binary.BigEndian.Uint32(last[:]) + hosts
		binary.BigEndian.PutUint32(last[:], n)
		if _, err := wslcCommand(ctx, "network", "create", "-d", "bridge", "-o", "com.docker.network.bridge.name="+bridge,
			"-o", "com.docker.network.driver.mtu="+strconv.Itoa(seg.MTU), "-o", "com.docker.network.bridge.inhibit_ipv4=true",
			"--internal", "--subnet", seg.Subnet, "--gateway", netip.AddrFrom4(last).String(), network); err != nil {
			return err
		}
	}
	_, err = v.do(ctx, nil, "segment-ensure", names, strconv.Itoa(seg.VNI), strconv.Itoa(seg.MTU), local, strconv.Itoa(port))
	return err
}

func (v *vmLink) removeSegment(ctx context.Context, names string, vni int) error {
	if _, err := v.do(ctx, nil, "segment-remove", names, strconv.Itoa(vni)); err != nil {
		if strings.Contains(err.Error(), errSegmentInUse.Error()) {
			return errSegmentInUse
		}
		return err
	}
	network := SegmentNetwork(names, vni)
	nets, err := wslcNetworks(ctx)
	if err != nil || !slices.Contains(nets, network) {
		return err
	}
	_, err = wslcCommand(ctx, "network", "remove", network)
	return err
}

// vmRouter is FRR in the VM (docker, on the VM's network: dockerRouter).
type vmRouter struct {
	v              *vmLink
	name, hostname string
}

func (v *vmLink) router(name, hostname, _ string) router {
	return vmRouter{v: v, name: name, hostname: hostname}
}

func (r vmRouter) apply(ctx context.Context, files map[string]string) error {
	b, err := json.Marshal(files)
	if err != nil {
		return err
	}
	_, err = r.v.do(ctx, b, "router-apply", r.name, r.hostname)
	return err
}

func (r vmRouter) remove(ctx context.Context) error {
	_, err := r.v.do(ctx, nil, "router-remove", r.name)
	return err
}

func (r vmRouter) show(ctx context.Context, command string) ([]byte, error) {
	return r.v.do(ctx, nil, "router-show", r.name, command)
}

// segmentDevice is what a container joins segment vni by: its wslc
// network.
func segmentDevice(names string, vni int) string { return SegmentNetwork(names, vni) }
