package executor

import (
	"io"
	"net"
	"net/http"
	"strconv"
)

// FabricSamplePath is where a node's transfer endpoint serves a sample to a
// peer on the fabric that is asked to time the link (issue #47, phase 5:
// fabric.Manager samples it, store/fabric_evidence.go asks for it).
const FabricSamplePath = "/fabric/sample"

// FabricSampleBytes is a sample's size: enough to time a link past its
// first round trips.
const FabricSampleBytes = 32 << 20

// fabricSampleConns bounds the samples served at once.
const fabricSampleConns = 2

// sampleBlock is what a sample is made of: not compressible, the same each
// time.
var sampleBlock = func() []byte {
	b := make([]byte, 1<<20)
	x := uint32(2463534242)
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}()

// serveFabricSample writes FabricSampleBytes to a peer on the fabric (GET),
// or reads as many from it (POST: the peer times the link from it, its
// transfer endpoint being off the fabric).
func (p *PackServer) serveFabricSample(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || p.FabricPeer == nil || !p.FabricPeer(host) {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p.mu.Lock()
	full := p.samples >= fabricSampleConns
	if !full {
		p.samples++
	}
	p.mu.Unlock()
	if full {
		http.Error(w, "too many samples at once", http.StatusServiceUnavailable)
		return
	}
	defer func() {
		p.mu.Lock()
		p.samples--
		p.mu.Unlock()
	}()
	if r.Method == http.MethodPost {
		if _, err := io.Copy(io.Discard, io.LimitReader(r.Body, FabricSampleBytes)); err != nil {
			http.Error(w, "sample not read", http.StatusBadRequest)
		}
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(FabricSampleBytes))
	if r.Method == http.MethodHead {
		return
	}
	for sent := 0; sent < FabricSampleBytes; sent += len(sampleBlock) {
		if _, err := w.Write(sampleBlock); err != nil {
			return
		}
	}
}
