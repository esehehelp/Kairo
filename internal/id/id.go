package id

import (
	"crypto/rand"
	"encoding/hex"
)

// New returns a random, sortable-by-time-independent identifier. Kairo treats
// identifiers as opaque strings; the prefix only makes logs easier to read.
func New(prefix string) string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(err)
	}
	return prefix + "_" + hex.EncodeToString(raw[:])
}
