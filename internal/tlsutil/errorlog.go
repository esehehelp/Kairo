package tlsutil

import (
	"bytes"
	"log"
)

// ServerErrorLog is an http.Server's ErrorLog that leaves out connections
// closed before their TLS handshake: the fabric's round-trip probes connect
// and close (issue #47), a few times a minute a peer. Everything else goes
// to the standard logger, as with no ErrorLog.
func ServerErrorLog() *log.Logger { return log.New(probeFilter{}, "", 0) }

type probeFilter struct{}

func (probeFilter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("TLS handshake error from")) && bytes.HasSuffix(bytes.TrimSpace(p), []byte(": EOF")) {
		return len(p), nil
	}
	if err := log.Output(2, string(p)); err != nil {
		return 0, err
	}
	return len(p), nil
}
