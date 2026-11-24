package loader

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// A format is how a file's bytes are records: its framing and, before it,
// its compression.
//
//   - text: each line, without its "\n" (nor a "\r" before it)
//   - jsonl: each line; with a field, that string field of the line's JSON
//     object (a line without it is an error)
//   - lenbin: each record a 4-byte little-endian length, then that many bytes
//   - file: the whole file, one record
//
// Compression is a suffix of the format (jsonl.zst), else the file name's
// extension: .zst, .gz, .bz2, or none.
const (
	FramingText   = "text"
	FramingJSONL  = "jsonl"
	FramingLenBin = "lenbin"
	FramingFile   = "file"
)

// maxRecordBytes bounds one record.
const maxRecordBytes = 256 << 20

// format is a parsed format: its framing, its compression ("" to take the
// file's extension) and the JSON field (jsonl).
type format struct {
	framing     string
	compression string
	field       string
}

func parseFormat(s, field string) (format, error) {
	f := format{field: field}
	name, compression, _ := strings.Cut(s, ".")
	switch name {
	case FramingText, FramingJSONL, FramingLenBin, FramingFile:
		f.framing = name
	default:
		return f, fmt.Errorf("format %q: the framing is text, jsonl, lenbin or file", s)
	}
	switch compression {
	case "", "zst", "gz", "bz2":
		f.compression = compression
	default:
		return f, fmt.Errorf("format %q: compression is zst, gz or bz2", s)
	}
	if field != "" && f.framing != FramingJSONL {
		return f, fmt.Errorf("format %q: a field is of jsonl only", s)
	}
	return f, nil
}

// CountKey is what a file's record count is kept under: its framing, the
// compression it is read with (the format's, else its name's) and the field.
func CountKey(format, field, fileName string) (string, error) {
	f, err := parseFormat(format, field)
	if err != nil {
		return "", err
	}
	return f.framing + "|" + f.compressionOf(fileName) + "|" + field, nil
}

// compressionOf is the compression of a file of this format named name.
func (f format) compressionOf(name string) string {
	if f.compression != "" {
		return f.compression
	}
	for _, c := range []string{"zst", "gz", "bz2"} {
		if strings.HasSuffix(name, "."+c) {
			return c
		}
	}
	return ""
}

// decompress is r decompressed as compression says; close releases it.
func decompress(r io.Reader, compression string) (io.Reader, func(), error) {
	switch compression {
	case "zst":
		d, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, nil, err
		}
		return d, d.Close, nil
	case "gz":
		g, err := gzip.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		return g, func() { g.Close() }, nil
	case "bz2":
		return bzip2.NewReader(r), func() {}, nil
	}
	return r, func() {}, nil
}

// recordReader gives a file's records in order, io.EOF after the last.
type recordReader interface {
	next() ([]byte, error)
}

// newRecordReader reads the records of a file named name from r (its bytes as
// stored): close releases the decompressor.
func newRecordReader(r io.Reader, name string, f format) (recordReader, func(), error) {
	plain, closeFn, err := decompress(r, f.compressionOf(name))
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", name, err)
	}
	b := bufio.NewReaderSize(plain, 1<<20)
	switch f.framing {
	case FramingText:
		return &lineReader{r: b, name: name}, closeFn, nil
	case FramingJSONL:
		return &jsonlReader{lines: lineReader{r: b, name: name}, field: f.field}, closeFn, nil
	case FramingLenBin:
		return &lenBinReader{r: b, name: name}, closeFn, nil
	}
	return &wholeReader{r: b, name: name}, closeFn, nil
}

type lineReader struct {
	r    *bufio.Reader
	name string
	line int64
	buf  []byte
}

func (l *lineReader) next() ([]byte, error) {
	l.buf = l.buf[:0]
	for {
		part, err := l.r.ReadSlice('\n')
		l.buf = append(l.buf, part...)
		if len(l.buf) > maxRecordBytes {
			return nil, fmt.Errorf("%s line %d: more than %d bytes", l.name, l.line+1, maxRecordBytes)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err == io.EOF && len(l.buf) > 0 {
			err = nil // a last line without "\n"
		}
		if err != nil {
			return nil, err
		}
		l.line++
		out := bytes.TrimSuffix(l.buf, []byte("\n"))
		out = bytes.TrimSuffix(out, []byte("\r"))
		return append([]byte(nil), out...), nil
	}
}

type jsonlReader struct {
	lines lineReader
	field string
}

func (j *jsonlReader) next() ([]byte, error) {
	line, err := j.lines.next()
	if err != nil || j.field == "" {
		return line, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(line, &object); err != nil {
		return nil, fmt.Errorf("%s line %d: %w", j.lines.name, j.lines.line, err)
	}
	raw, ok := object[j.field]
	var value string
	if ok {
		if raw = bytes.TrimSpace(raw); len(raw) == 0 || raw[0] != '"' {
			ok = false
		} else {
			err = json.Unmarshal(raw, &value)
		}
	}
	if !ok || err != nil {
		return nil, fmt.Errorf("%s line %d: no string field %q", j.lines.name, j.lines.line, j.field)
	}
	return []byte(value), nil
}

type lenBinReader struct {
	r     *bufio.Reader
	name  string
	count int64
}

func (l *lenBinReader) next() ([]byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(l.r, size[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("%s record %d: cut short", l.name, l.count+1)
		}
		return nil, err
	}
	n := binary.LittleEndian.Uint32(size[:])
	if n > maxRecordBytes {
		return nil, fmt.Errorf("%s record %d: %d bytes, more than %d", l.name, l.count+1, n, maxRecordBytes)
	}
	out := make([]byte, n)
	if _, err := io.ReadFull(l.r, out); err != nil {
		return nil, fmt.Errorf("%s record %d: cut short", l.name, l.count+1)
	}
	l.count++
	return out, nil
}

type wholeReader struct {
	r    *bufio.Reader
	name string
	done bool
}

func (w *wholeReader) next() ([]byte, error) {
	if w.done {
		return nil, io.EOF
	}
	w.done = true
	out, err := io.ReadAll(io.LimitReader(w.r, maxRecordBytes+1))
	if err != nil {
		return nil, err
	}
	if len(out) > maxRecordBytes {
		return nil, fmt.Errorf("%s: more than %d bytes", w.name, maxRecordBytes)
	}
	return out, nil
}
