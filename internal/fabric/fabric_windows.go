//go:build windows

package fabric

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
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

// LinkCommand: the fabric-link command is Linux's (linkcmd_linux.go), run
// in the VM.
func LinkCommand(context.Context, []string, io.Writer) error {
	return errors.New("fabric-link runs on Linux (in the wslc VM, kairo-linux)")
}
