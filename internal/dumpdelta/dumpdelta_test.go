package dumpdelta

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"
)

func randomBytes(seed int64, n int) []byte {
	r := rand.New(rand.NewSource(seed))
	b := make([]byte, n)
	r.Read(b)
	return b
}

// A dump is made again from its base and its delta alone; what it shares
// with the base (around an insert, a deletion, a change) is taken from the
// base, so the delta holds about what changed; the cuts of a dump are the
// same each time.
func TestADumpIsMadeAgainFromItsBaseAndDelta(t *testing.T) {
	base := randomBytes(1, 3<<20)
	var baseCopy bytes.Buffer
	chunks, baseSum, baseBytes, err := Base(bytes.NewReader(base), &baseCopy)
	if err != nil || !bytes.Equal(baseCopy.Bytes(), base) || baseBytes != int64(len(base)) || len(chunks) < 20 {
		t.Fatalf("base: %d chunks, %d bytes, %v", len(chunks), baseBytes, err)
	}
	again, _, _, _ := Base(bytes.NewReader(base), &bytes.Buffer{})
	if len(again) != len(chunks) || again[len(again)/2] != chunks[len(chunks)/2] {
		t.Fatal("the same dump was cut otherwise")
	}
	decoded, err := DecodeChunks(EncodeChunks(chunks))
	if err != nil || len(decoded) != len(chunks) || decoded[3] != chunks[3] {
		t.Fatalf("chunk table round trip: %v", err)
	}
	// the next dump: 100 KiB inserted, 200 KiB taken out, 4 KiB changed, a
	// tail added
	next := append([]byte{}, base[:1<<20]...)
	next = append(next, randomBytes(2, 100<<10)...)
	next = append(next, base[1<<20:2<<20]...)
	next = append(next, base[2<<20+200<<10:]...)
	copy(next[2<<20+50<<10:], randomBytes(3, 4<<10))
	next = append(next, randomBytes(4, 300<<10)...)
	var fresh bytes.Buffer
	rec, err := Delta(bytes.NewReader(next), decoded, baseSum, baseBytes, &fresh)
	if err != nil {
		t.Fatal(err)
	}
	if rec.NewBytes != int64(fresh.Len()) || rec.NewBytes > 1<<20 || rec.NewBytes < 400<<10 {
		t.Fatalf("a delta of %d new bytes for about 400 KiB changed", rec.NewBytes)
	}
	var text bytes.Buffer
	if err := rec.Write(&text); err != nil {
		t.Fatal(err)
	}
	read, err := ReadRecipe(strings.NewReader(text.String()))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := Rebuild(read, bytes.NewReader(base), int64(len(base)), bytes.NewReader(fresh.Bytes()), &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), next) {
		t.Fatal("rebuilt another dump")
	}
	// another base is refused
	other := append([]byte{}, base...)
	other[10] ^= 1
	if err := Rebuild(read, bytes.NewReader(other), int64(len(other)), bytes.NewReader(fresh.Bytes()), &bytes.Buffer{}); err == nil {
		t.Fatal("rebuilt on another base")
	}
}
