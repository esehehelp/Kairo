package fabric

import "context"

// LinkArg is the hidden command (`kairo __kairo-fabric-link OP …`) that does
// a link's work for another process: the Windows agent's, in its wslc VM.
const LinkArg = "__kairo-fabric-link"

// vmRouterDir holds, in a wslc VM, each FRR's /etc/frr (by its name).
const vmRouterDir = "/var/lib/kairo/fabric/frr"

// What `kairo __kairo-fabric-link` says (linkcmd_linux.go) and the Windows agent
// reads (fabric_windows.go).

type vmPeer struct {
	HandshakeUnix int64  `json:"handshake_unix,omitempty"`
	Endpoint      string `json:"endpoint,omitempty"`
}

type vmUnderlay struct {
	Interface string `json:"interface"`
	Address   string `json:"address"`
	MTU       int    `json:"mtu"`
}

type vmProbe struct {
	RTTMillis *float64 `json:"rtt_ms"`
}

// vmSample is what the link command says of a sample it fetched.
type vmSample struct {
	Bytes   int64   `json:"bytes"`
	Seconds float64 `json:"seconds"`
}

// prober is a link whose round trips are timed where it is (the wslc VM),
// not from this process.
type prober interface {
	probeFrom(ctx context.Context, addr string) *float64
}

// HostOnFabric: this process's own sockets are on the fabric (Linux). On
// Windows the fabric's interface is in the wslc VM (fabric_windows.go): the
// agent's API and transfers stay off it.
func HostOnFabric() bool { return hostOnFabric }
