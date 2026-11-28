package fabric

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"kairo/internal/store"
)

// Samples (issue #47, phase 5). The daemon asks a node for the bandwidth of
// a link to it it has no measurement of (store/fabric_evidence.go: a gang
// waits for one): the node fetches a sample from the peer's transfer
// endpoint over the fabric (executor.FabricSamplePath) and reports it as a
// transfer received (Received), as any large fetch is.

// samplePath is executor.FabricSamplePath (not imported: the executor is
// above the fabric).
const samplePath = "/fabric/sample"

// sampleAgain: a peer sampled this recently is not sampled again for the
// same ask.
const sampleAgain = 10 * time.Minute

// sampler fetches a sample, or sends one (send), where the fabric is: in
// the wslc VM on Windows (vmLink); in-process (sampleHTTP) where the host
// is on the fabric.
type sampler interface {
	sampleFrom(ctx context.Context, url string, send bool) (int64, time.Duration, error)
}

// sampleSize is what a sample sent is (executor.FabricSampleBytes).
const sampleSize = 32 << 20

// sampleHTTP fetches url (or, send, posts a sample to it) and times it.
// The sample's bytes are not looked at and the link is WireGuard's
// (authenticated, encrypted): the transfer endpoint's certificate is not
// checked, so that a node needs no CA for it.
func sampleHTTP(ctx context.Context, url string, send bool) (int64, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	method, n := http.MethodGet, int64(0)
	var body io.Reader
	if send {
		method, n = http.MethodPost, sampleSize
		body = io.LimitReader(sampleBytes{}, n)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return 0, 0, err
	}
	req.ContentLength = n
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // see above
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("sample %s %s: %s", method, url, resp.Status)
	}
	got, err := io.Copy(io.Discard, resp.Body)
	if send {
		got = n
	}
	return got, time.Since(start), err
}

// sampleBytes reads as bytes that do not compress.
type sampleBytes struct{}

func (sampleBytes) Read(p []byte) (int, error) {
	x := uint32(2463534242)
	for i := range p {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		p[i] = byte(x)
	}
	return len(p), nil
}

// sampleAsked fetches a sample from each peer view asks for that was not
// sampled lately, in the background.
func (m *Manager) sampleAsked(measure []store.FabricMeasure) {
	for _, s := range measure {
		// by peer and way: a node times both ways with a peer that serves
		// samples when it serves none
		key := s.Node
		if s.Send {
			key += " send"
		}
		m.mu.Lock()
		if m.sampled == nil {
			m.sampled = map[string]time.Time{}
		}
		if at, ok := m.sampled[key]; ok && m.now().Sub(at) < sampleAgain {
			m.mu.Unlock()
			continue
		}
		m.sampled[key] = m.now()
		m.mu.Unlock()
		addr := net.JoinHostPort(s.Address, strconv.Itoa(s.Port))
		url := "https://" + addr + samplePath
		go func() {
			fetch := sampleHTTP
			if sp, ok := m.currentLink().(sampler); ok {
				fetch = sp.sampleFrom
			}
			bytes, took, err := fetch(context.Background(), url, s.Send)
			if err != nil {
				m.logger().Warn("fabric sample not timed", "peer", s.Node, "send", s.Send, "error", err)
				return
			}
			m.logger().Info("fabric sample timed", "peer", s.Node, "send", s.Send, "bytes", bytes, "seconds", took.Seconds())
			if !s.Send {
				m.Received(addr, bytes, took)
				return
			}
			m.mu.Lock()
			m.sent = append(m.sent, store.FabricReceived{From: s.Node, Bytes: bytes, Seconds: took.Seconds()})
			m.mu.Unlock()
		}()
	}
}
