package executor

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"kairo/internal/tlsutil"
)

// Packs travel between agents over HTTPS with HTTP/1.1 only. The client's
// TLS settings are a copy of those the agent reaches the daemon with, which
// that connection's HTTP/2 support has filled with an "h2" protocol offer:
// kept, the pack server would pick HTTP/2 and a client transport that does
// not speak it reads its frames as a malformed response (2026-11-07: every
// copy from another node failed so). So the client offers HTTP/1.1 alone,
// and the server, for clients built before this, never picks HTTP/2.

// PackHTTPClient is the client an agent fetches packs from other agents
// with, trusting what tlsConfig trusts: a connection, its handshake and the
// answer's head each have a bound (a body that stops coming is given up by
// the executor, stallTimeout).
func PackHTTPClient(tlsConfig *tls.Config) *http.Client {
	clientTLS := tlsConfig.Clone()
	clientTLS.NextProtos = []string{"http/1.1"}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLS,
		DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout: 30 * time.Second, ResponseHeaderTimeout: time.Minute, IdleConnTimeout: 90 * time.Second}}
}

// PackHTTPServer is the server an agent hands its packs over with, at addr
// with cert: no write timeout (a pack takes as long as it takes), an idle
// connection closed, HTTP/1.1 only.
func PackHTTPServer(addr string, cert tls.Certificate, handler http.Handler) *http.Server {
	serverTLS := tlsutil.ServerConfig()
	serverTLS.Certificates = []tls.Certificate{cert}
	serverTLS.NextProtos = []string{"http/1.1"}
	return &http.Server{Addr: addr, TLSConfig: serverTLS, ReadHeaderTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute, Handler: handler,
		TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){}}
}
