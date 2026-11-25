package loader

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
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
//   - csv, tsv: each row after the header row; with a field, that column's
//     value; without, the row as a JSON object (by the header's names)
//   - parquet: each row, row group by row group; with a field, that
//     top-level string or binary column's value; without, the row as a JSON
//     object
//   - arrow: each row of an Arrow IPC file or stream (what Hugging Face
//     datasets saves), batch by batch; a field as for parquet
//   - webdataset: each sample of a tar: the consecutive members of one key
//     (the member's path up to the first dot of its name), encoded as a
//     sample (EncodeSample); with a field, the bytes of that member alone
//     (the field names its extension, as "jpg" or "json")
//
// Compression is a suffix of the format (jsonl.zst), else the file name's
// extension: .zst, .gz, .bz2, or none. Parquet and Arrow files are read at
// random (they compress inside), so they take none.
const (
	FramingText       = "text"
	FramingJSONL      = "jsonl"
	FramingLenBin     = "lenbin"
	FramingFile       = "file"
	FramingCSV        = "csv"
	FramingTSV        = "tsv"
	FramingParquet    = "parquet"
	FramingArrow      = "arrow"
	FramingWebDataset = "webdataset"
)

// Framings are the framings a format names.
var Framings = []string{FramingText, FramingJSONL, FramingLenBin, FramingFile, FramingCSV, FramingTSV, FramingParquet, FramingArrow, FramingWebDataset}

// RandomAccess: files of this format are read at random (their index is at
// their end), not as a stream.
func RandomAccess(format string) bool {
	framing, _, _ := strings.Cut(format, ".")
	return framing == FramingParquet || framing == FramingArrow
}

// fieldFramings are the framings a field is of.
var fieldFramings = map[string]bool{FramingJSONL: true, FramingCSV: true, FramingTSV: true, FramingParquet: true, FramingArrow: true, FramingWebDataset: true}

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
	known := false
	for _, framing := range Framings {
		known = known || name == framing
	}
	if !known {
		return f, fmt.Errorf("format %q: the framing is one of %s", s, strings.Join(Framings, ", "))
	}
	f.framing = name
	switch compression {
	case "", "zst", "gz", "bz2":
		f.compression = compression
	default:
		return f, fmt.Errorf("format %q: compression is zst, gz or bz2", s)
	}
	if compression != "" && RandomAccess(name) {
		return f, fmt.Errorf("format %q: %s files compress inside and are read at random: no compression around them", s, name)
	}
	if field != "" && !fieldFramings[f.framing] {
		return f, fmt.Errorf("format %q: a field is of jsonl, csv, tsv, parquet, arrow or webdataset only", s)
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

// CountRecords is how many records r (a file named name, its bytes as
// stored) holds read as format and field say.
func CountRecords(r io.Reader, name, format, field string) (int64, error) {
	f, err := parseFormat(format, field)
	if err != nil {
		return 0, err
	}
	records, closeFn, err := newRecordReader(r, name, f)
	if err != nil {
		return 0, err
	}
	defer closeFn()
	var n int64
	for {
		if _, err := records.next(); err == io.EOF {
			return n, nil
		} else if err != nil {
			return n, err
		}
		n++
	}
}

// RecordsKey is what a SourceSpec's Records keeps a file's count under: its
// sha256 and the CountKey of how the source reads it.
func RecordsKey(sha256, format, field, fileName string) (string, error) {
	key, err := CountKey(format, field, fileName)
	if err != nil {
		return "", err
	}
	return sha256 + "|" + key, nil
}

// compressionOf is the compression of a file of this format named name.
func (f format) compressionOf(name string) string {
	if RandomAccess(f.framing) {
		return ""
	}
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
	case FramingCSV, FramingTSV:
		return newCSVReader(b, name, f), closeFn, nil
	case FramingWebDataset:
		return &webDatasetReader{t: tar.NewReader(b), name: name, field: f.field}, closeFn, nil
	case FramingParquet, FramingArrow:
		closeFn()
		return nil, nil, fmt.Errorf("%s: a %s file is read at random", name, f.framing)
	}
	return &wholeReader{r: b, name: name}, closeFn, nil
}

// newRecordReaderAt reads the records of a file read at random (parquet,
// arrow) named name, of size bytes; close releases it.
func newRecordReaderAt(r io.ReaderAt, size int64, name string, f format) (recordReader, func(), error) {
	switch f.framing {
	case FramingParquet:
		p, err := newParquetReader(r, size, name, f.field)
		if err != nil {
			return nil, nil, err
		}
		return p, p.close, nil
	case FramingArrow:
		a, err := newArrowBatches(r, size, name)
		if err != nil {
			return nil, nil, err
		}
		return &rowsReader{name: name, field: f.field, batches: a}, a.close, nil
	}
	return nil, nil, fmt.Errorf("%s: a %s file is read as a stream", name, f.framing)
}

// CountRecordsAt is how many records the file r (named name, size bytes,
// read at random) holds read as format and field say: a Parquet file's
// count is its metadata's.
func CountRecordsAt(r io.ReaderAt, size int64, name, format, field string) (int64, error) {
	f, err := parseFormat(format, field)
	if err != nil {
		return 0, err
	}
	if !RandomAccess(f.framing) {
		return CountRecords(io.NewSectionReader(r, 0, size), name, format, field)
	}
	if f.framing == FramingParquet && field == "" {
		return parquetRows(r, size)
	}
	records, closeFn, err := newRecordReaderAt(r, size, name, f)
	if err != nil {
		return 0, err
	}
	defer closeFn()
	var n int64
	for {
		if _, err := records.next(); err == io.EOF {
			return n, nil
		} else if err != nil {
			return n, err
		}
		n++
	}
}

// csvReader gives the rows of a CSV (or TSV) file after its header row.
type csvReader struct {
	r      *csv.Reader
	name   string
	field  string
	header []string
	column int
	row    int64
}

func newCSVReader(r io.Reader, name string, f format) *csvReader {
	c := csv.NewReader(r)
	if f.framing == FramingTSV {
		c.Comma, c.LazyQuotes = '\t', true
	}
	c.ReuseRecord = true
	return &csvReader{r: c, name: name, field: f.field, column: -1}
}

func (c *csvReader) next() ([]byte, error) {
	if c.header == nil {
		header, err := c.r.Read()
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, fmt.Errorf("%s header: %w", c.name, err)
		}
		c.header = append([]string(nil), header...)
		if c.field != "" {
			for i, name := range c.header {
				if name == c.field {
					c.column = i
				}
			}
			if c.column < 0 {
				return nil, fmt.Errorf("%s: no column %q in its header", c.name, c.field)
			}
		}
	}
	row, err := c.r.Read()
	if err != nil {
		if err != io.EOF {
			err = fmt.Errorf("%s row %d: %w", c.name, c.row+1, err)
		}
		return nil, err
	}
	c.row++
	if c.column >= 0 {
		return []byte(row[c.column]), nil
	}
	var b bytes.Buffer
	b.WriteByte('{')
	for i, name := range c.header {
		if i > 0 {
			b.WriteByte(',')
		}
		key, _ := json.Marshal(name)
		value, _ := json.Marshal(row[i])
		b.Write(key)
		b.WriteByte(':')
		b.Write(value)
	}
	b.WriteByte('}')
	if b.Len() > maxRecordBytes {
		return nil, fmt.Errorf("%s row %d: more than %d bytes", c.name, c.row, maxRecordBytes)
	}
	return b.Bytes(), nil
}

// webDatasetReader gives the samples of a tar: the consecutive members of
// one key.
type webDatasetReader struct {
	t       *tar.Reader
	name    string
	field   string
	pending *sampleMember // the first member of the next sample
	samples int64
	ended   bool
}

type sampleMember struct {
	key, ext string
	data     []byte
}

// sampleKey splits a member's path into its key (up to the first dot of
// its name) and its extension (after it).
func sampleKey(p string) (string, string) {
	dir, base := path.Split(p)
	stem, ext, _ := strings.Cut(base, ".")
	return dir + stem, ext
}

func (w *webDatasetReader) member() (*sampleMember, error) {
	for {
		h, err := w.t.Next()
		if err != nil {
			return nil, err
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		if h.Size > maxRecordBytes {
			return nil, fmt.Errorf("%s: member %s: more than %d bytes", w.name, h.Name, maxRecordBytes)
		}
		data, err := io.ReadAll(w.t)
		if err != nil {
			return nil, fmt.Errorf("%s: member %s: %w", w.name, h.Name, err)
		}
		key, ext := sampleKey(h.Name)
		return &sampleMember{key: key, ext: ext, data: data}, nil
	}
}

func (w *webDatasetReader) next() ([]byte, error) {
	if w.ended {
		return nil, io.EOF
	}
	first := w.pending
	w.pending = nil
	if first == nil {
		m, err := w.member()
		if err == io.EOF {
			w.ended = true
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		first = m
	}
	members := []sampleMember{*first}
	for {
		m, err := w.member()
		if err == io.EOF {
			w.ended = true
			break
		}
		if err != nil {
			return nil, err
		}
		if m.key != first.key {
			w.pending = m
			break
		}
		members = append(members, *m)
	}
	w.samples++
	if w.field != "" {
		for _, m := range members {
			if m.ext == w.field {
				return m.data, nil
			}
		}
		return nil, fmt.Errorf("%s sample %s: no member .%s", w.name, first.key, w.field)
	}
	parts := map[string][]byte{"__key__": []byte(first.key)}
	for _, m := range members {
		parts[m.ext] = m.data
	}
	out := EncodeSample(parts)
	if len(out) > maxRecordBytes {
		return nil, fmt.Errorf("%s sample %s: more than %d bytes", w.name, first.key, maxRecordBytes)
	}
	return out, nil
}

// EncodeSample is a sample's members as one record: a 4-byte little-endian
// count, then each member (by name, sorted) as a 4-byte length and its
// name, a 4-byte length and its bytes. Members are "__key__" and each
// extension. (The SDK's streams.sample decodes it.)
func EncodeSample(parts map[string][]byte) []byte {
	names := make([]string, 0, len(parts))
	size := 4
	for name, data := range parts {
		names = append(names, name)
		size += 8 + len(name) + len(data)
	}
	sort.Strings(names)
	out := make([]byte, 0, size)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(names)))
	for _, name := range names {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(name)))
		out = append(out, name...)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(parts[name])))
		out = append(out, parts[name]...)
	}
	return out
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
