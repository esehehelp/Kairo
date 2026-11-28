// Package dumpdelta keeps a database dump as the difference from an
// earlier one (README: Kairo keeps itself). A dump is cut into chunks where
// its content says (a rolling hash: the cuts move with the bytes, not with
// their offsets, so an insert shifts no later chunk), each named by its
// sha256. A base is a whole dump and the table of its chunks (where each
// is in it); a delta is the dump's recipe, ranges of the base and of its
// own new bytes, and those new bytes: the chunks the base does not have.
// The dump is rebuilt from the base and the delta alone (Rebuild).
package dumpdelta

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Chunk sizes: none shorter than minChunk (but the last), none longer than
// maxChunk, about avgChunk (measured on Kairo's own dumps: an average of
// 40 KB made a delta of 2.4% where one of 165 KB made 4.1%, its table 1 MB
// for a dump of 900 MB).
const (
	minChunk = 4 << 10
	avgChunk = 16 << 10
	maxChunk = 64 << 10
	// a cut where the hash's low bits are all zero
	cutMask = 1<<14 - 1
)

// gear is the rolling hash's table: fixed (the cuts of a dump must be the
// same whenever it is cut).
var gear = func() [256]uint64 {
	var t [256]uint64
	x := uint64(0x9e3779b97f4a7c15)
	for i := range t {
		// splitmix64
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		t[i] = z ^ (z >> 31)
	}
	return t
}()

// Cut reads r to its end and calls fn with each chunk in order (the slice
// is fn's only until it returns).
func Cut(r io.Reader, fn func(chunk []byte) error) error {
	br := bufio.NewReaderSize(r, 1<<20)
	buf := make([]byte, 0, maxChunk)
	var h uint64
	for {
		b, err := br.ReadByte()
		if errors.Is(err, io.EOF) {
			if len(buf) > 0 {
				return fn(buf)
			}
			return nil
		}
		if err != nil {
			return err
		}
		buf = append(buf, b)
		h = h<<1 + gear[b]
		if len(buf) >= maxChunk || len(buf) >= minChunk && h&cutMask == 0 {
			if err := fn(buf); err != nil {
				return err
			}
			buf, h = buf[:0], 0
		}
	}
}

// Chunk is where a chunk is in a dump, and its sha256.
type Chunk struct {
	Hash [32]byte
	Off  int64
	Len  int64
}

// chunkRecord is a chunk's encoded size: its hash, offset and length.
const chunkRecord = 32 + 8 + 4

// EncodeChunks is a chunk table as bytes (a base's, kept with it).
func EncodeChunks(chunks []Chunk) []byte {
	out := make([]byte, 0, len(chunks)*chunkRecord)
	for _, c := range chunks {
		out = append(out, c.Hash[:]...)
		out = binary.BigEndian.AppendUint64(out, uint64(c.Off))
		out = binary.BigEndian.AppendUint32(out, uint32(c.Len))
	}
	return out
}

// DecodeChunks reads what EncodeChunks wrote.
func DecodeChunks(b []byte) ([]Chunk, error) {
	if len(b)%chunkRecord != 0 {
		return nil, fmt.Errorf("a chunk table of %d bytes", len(b))
	}
	out := make([]Chunk, 0, len(b)/chunkRecord)
	for len(b) > 0 {
		var c Chunk
		copy(c.Hash[:], b[:32])
		c.Off = int64(binary.BigEndian.Uint64(b[32:40]))
		c.Len = int64(binary.BigEndian.Uint32(b[40:44]))
		out = append(out, c)
		b = b[chunkRecord:]
	}
	return out, nil
}

// Base copies the dump r to w whole and returns its chunk table, sha256
// (hex) and size: a base for later deltas.
func Base(r io.Reader, w io.Writer) (chunks []Chunk, sum string, n int64, err error) {
	h := sha256.New()
	err = Cut(r, func(c []byte) error {
		if _, err := w.Write(c); err != nil {
			return err
		}
		h.Write(c)
		chunks = append(chunks, Chunk{Hash: sha256.Sum256(c), Off: n, Len: int64(len(c))})
		n += int64(len(c))
		return nil
	})
	return chunks, hex.EncodeToString(h.Sum(nil)), n, err
}

// Op is a step of a recipe: Len bytes from Off of the base (FromBase) or of
// the delta's new bytes.
type Op struct {
	FromBase bool
	Off, Len int64
}

// Recipe is how a dump is made again: of the base (BaseSHA256, BaseBytes)
// and new bytes, by its ops; the dump made is SHA256, Bytes long.
type Recipe struct {
	BaseSHA256 string
	BaseBytes  int64
	SHA256     string
	Bytes      int64
	NewBytes   int64
	Ops        []Op
}

// Delta reads the dump r, writing to newOut the chunks base (its table) does
// not have (each once), and returns its recipe.
func Delta(r io.Reader, base []Chunk, baseSHA256 string, baseBytes int64, newOut io.Writer) (Recipe, error) {
	inBase := make(map[[32]byte]Chunk, len(base))
	for _, c := range base {
		if _, ok := inBase[c.Hash]; !ok {
			inBase[c.Hash] = c
		}
	}
	rec := Recipe{BaseSHA256: baseSHA256, BaseBytes: baseBytes}
	added := map[[32]byte]int64{}
	h := sha256.New()
	add := func(op Op) {
		if n := len(rec.Ops); n > 0 {
			last := &rec.Ops[n-1]
			if last.FromBase == op.FromBase && last.Off+last.Len == op.Off {
				last.Len += op.Len
				return
			}
		}
		rec.Ops = append(rec.Ops, op)
	}
	err := Cut(r, func(c []byte) error {
		h.Write(c)
		rec.Bytes += int64(len(c))
		sum := sha256.Sum256(c)
		if b, ok := inBase[sum]; ok {
			add(Op{FromBase: true, Off: b.Off, Len: b.Len})
			return nil
		}
		if off, ok := added[sum]; ok {
			add(Op{Off: off, Len: int64(len(c))})
			return nil
		}
		if _, err := newOut.Write(c); err != nil {
			return err
		}
		added[sum] = rec.NewBytes
		add(Op{Off: rec.NewBytes, Len: int64(len(c))})
		rec.NewBytes += int64(len(c))
		return nil
	})
	rec.SHA256 = hex.EncodeToString(h.Sum(nil))
	return rec, err
}

// recipeVersion begins a recipe's text.
const recipeVersion = "kairo dump delta 1"

// Write writes the recipe as text: a header, then an op a line ("b OFF
// LEN" of the base, "n OFF LEN" of the new bytes).
func (r Recipe) Write(w io.Writer) error {
	bw := bufio.NewWriter(w)
	fmt.Fprintf(bw, "%s\nbase %s %d\ndump %s %d\nnew %d\n", recipeVersion, r.BaseSHA256, r.BaseBytes, r.SHA256, r.Bytes, r.NewBytes)
	for _, op := range r.Ops {
		kind := "n"
		if op.FromBase {
			kind = "b"
		}
		fmt.Fprintf(bw, "%s %d %d\n", kind, op.Off, op.Len)
	}
	return bw.Flush()
}

// ReadRecipe reads what Recipe.Write wrote.
func ReadRecipe(r io.Reader) (Recipe, error) {
	var rec Recipe
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, 4096), 1<<20)
	line := 0
	bad := func(msg string) (Recipe, error) {
		return Recipe{}, fmt.Errorf("dump delta recipe line %d: %s", line, msg)
	}
	for s.Scan() {
		line++
		f := strings.Fields(s.Text())
		switch {
		case line == 1:
			if s.Text() != recipeVersion {
				return bad("not a recipe: " + s.Text())
			}
		case line == 2 && len(f) == 3 && f[0] == "base":
			rec.BaseSHA256 = f[1]
			rec.BaseBytes, _ = strconv.ParseInt(f[2], 10, 64)
		case line == 3 && len(f) == 3 && f[0] == "dump":
			rec.SHA256 = f[1]
			rec.Bytes, _ = strconv.ParseInt(f[2], 10, 64)
		case line == 4 && len(f) == 2 && f[0] == "new":
			rec.NewBytes, _ = strconv.ParseInt(f[1], 10, 64)
		case line > 4 && len(f) == 3 && (f[0] == "b" || f[0] == "n"):
			off, err1 := strconv.ParseInt(f[1], 10, 64)
			n, err2 := strconv.ParseInt(f[2], 10, 64)
			if err1 != nil || err2 != nil || off < 0 || n <= 0 {
				return bad("an op that is not one")
			}
			rec.Ops = append(rec.Ops, Op{FromBase: f[0] == "b", Off: off, Len: n})
		default:
			return bad("unexpected: " + s.Text())
		}
	}
	if err := s.Err(); err != nil {
		return Recipe{}, err
	}
	if line < 4 {
		return bad("cut short")
	}
	return rec, nil
}

// Rebuild writes the dump rec makes of base and newBytes to w, checking the
// base's and the dump's sha256 and sizes.
func Rebuild(rec Recipe, base io.ReaderAt, baseBytes int64, newBytes io.ReaderAt, w io.Writer) error {
	if baseBytes != rec.BaseBytes {
		return fmt.Errorf("the base is %d bytes, the recipe's %d", baseBytes, rec.BaseBytes)
	}
	bh := sha256.New()
	if _, err := io.Copy(bh, io.NewSectionReader(base, 0, baseBytes)); err != nil {
		return err
	}
	if got := hex.EncodeToString(bh.Sum(nil)); got != rec.BaseSHA256 {
		return fmt.Errorf("the base's sha256 is %s, the recipe's %s", got, rec.BaseSHA256)
	}
	h := sha256.New()
	out := io.MultiWriter(w, h)
	var n int64
	for _, op := range rec.Ops {
		from := newBytes
		limit := rec.NewBytes
		if op.FromBase {
			from, limit = base, rec.BaseBytes
		}
		if op.Off+op.Len > limit {
			return fmt.Errorf("an op past its source (%d+%d of %d)", op.Off, op.Len, limit)
		}
		if _, err := io.Copy(out, io.NewSectionReader(from, op.Off, op.Len)); err != nil {
			return err
		}
		n += op.Len
	}
	if n != rec.Bytes {
		return fmt.Errorf("rebuilt %d bytes, the recipe says %d", n, rec.Bytes)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != rec.SHA256 {
		return fmt.Errorf("rebuilt a dump of sha256 %s, the recipe says %s", got, rec.SHA256)
	}
	return nil
}
