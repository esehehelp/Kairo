package tlsutil

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

// A connection closed before its handshake (a round-trip probe) is not
// logged; any other server error is.
func TestProbesAreLeftOutOfTheServerErrorLog(t *testing.T) {
	var out bytes.Buffer
	prev, flags := log.Writer(), log.Flags()
	log.SetOutput(&out)
	log.SetFlags(0)
	defer func() { log.SetOutput(prev); log.SetFlags(flags) }()
	l := ServerErrorLog()
	l.Printf("http: TLS handshake error from 10.66.0.4:16304: EOF")
	l.Printf("http: TLS handshake error from 10.66.0.4:16305: remote error: tls: bad certificate")
	got := out.String()
	if strings.Contains(got, ": EOF") || !strings.Contains(got, "bad certificate") {
		t.Fatalf("logged %q", got)
	}
}
