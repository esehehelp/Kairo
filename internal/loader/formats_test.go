package loader

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/parquet-go/parquet-go"
)

// records reads every record of data (a file named name) as format and field say.
func records(t *testing.T, data []byte, name, format, field string) []string {
	t.Helper()
	f, err := parseFormat(format, field)
	if err != nil {
		t.Fatal(err)
	}
	var r recordReader
	var closeFn func()
	if RandomAccess(format) {
		r, closeFn, err = newRecordReaderAt(bytes.NewReader(data), int64(len(data)), name, f)
	} else {
		r, closeFn, err = newRecordReader(bytes.NewReader(data), name, f)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	var out []string
	for {
		rec, err := r.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(rec))
	}
	if RandomAccess(format) {
		n, err := CountRecordsAt(bytes.NewReader(data), int64(len(data)), name, format, field)
		if err != nil || n != int64(len(out)) {
			t.Fatalf("%s counted %d (%v), read %d", name, n, err, len(out))
		}
	}
	return out
}

type textRow struct {
	Text string `parquet:"text"`
	N    int64  `parquet:"n"`
}

func parquetOf(t *testing.T, rows []textRow, perGroup int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[textRow](&buf, parquet.MaxRowsPerRowGroup(perGroup))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func arrowOf(t *testing.T, rows []textRow, file bool) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}, {Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	var buf bytes.Buffer
	var w interface {
		Write(arrow.RecordBatch) error
		Close() error
	}
	if file {
		fw, err := ipc.NewFileWriter(&buf, ipc.WithSchema(schema))
		if err != nil {
			t.Fatal(err)
		}
		w = fw
	} else {
		w = ipc.NewWriter(&buf, ipc.WithSchema(schema))
	}
	for start := 0; start < len(rows); start += 3 { // batches of 3
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for _, r := range rows[start:min(start+3, len(rows))] {
			b.Field(0).(*array.StringBuilder).Append(r.Text)
			b.Field(1).(*array.Int64Builder).Append(r.N)
		}
		rec := b.NewRecordBatch()
		if err := w.Write(rec); err != nil {
			t.Fatal(err)
		}
		rec.Release()
		b.Release()
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func someRows(n int) []textRow {
	rows := make([]textRow, n)
	for i := range rows {
		rows[i] = textRow{Text: fmt.Sprintf("row %d, \"quoted\"", i), N: int64(i)}
	}
	return rows
}

// Each format gives its records in order: a field's value, or the row (the
// sample) whole.
func TestFormatsGiveTheirRecordsInOrder(t *testing.T) {
	rows := someRows(10)
	texts := make([]string, len(rows))
	objects := make([]string, len(rows))
	for i, r := range rows {
		texts[i] = r.Text
		b, _ := json.Marshal(map[string]any{"n": r.N, "text": r.Text})
		objects[i] = string(b)
	}
	// parquet: in row groups of 4, a column alone or rows whole
	pq := parquetOf(t, rows, 4)
	if got := records(t, pq, "x.parquet", "parquet", "text"); !reflect.DeepEqual(got, texts) {
		t.Fatalf("parquet text: %q", got)
	}
	if got := records(t, pq, "x.parquet", "parquet", ""); !reflect.DeepEqual(got, objects) {
		t.Fatalf("parquet rows: %q\nwant %q", got, objects)
	}
	f, _ := parseFormat("parquet", "n")
	if _, _, err := newRecordReaderAt(bytes.NewReader(pq), int64(len(pq)), "x.parquet", f); err == nil {
		t.Fatal("a number column as a field")
	}
	// arrow: a stream and a file, in batches of 3
	for _, file := range []bool{false, true} {
		data := arrowOf(t, rows, file)
		if got := records(t, data, "x.arrow", "arrow", "text"); !reflect.DeepEqual(got, texts) {
			t.Fatalf("arrow (file %v) text: %q", file, got)
		}
		got := records(t, data, "x.arrow", "arrow", "")
		for i := range got {
			var a, b map[string]any
			json.Unmarshal([]byte(got[i]), &a)
			json.Unmarshal([]byte(objects[i]), &b)
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("arrow (file %v) row %d: %s, want %s", file, i, got[i], objects[i])
			}
		}
	}
	// csv and tsv, after the header row
	var csvText, tsvText bytes.Buffer
	csvText.WriteString("n,text\n")
	tsvText.WriteString("n\ttext\n")
	for _, r := range rows {
		fmt.Fprintf(&csvText, "%d,\"%s\"\n", r.N, bytes.ReplaceAll([]byte(r.Text), []byte(`"`), []byte(`""`)))
		fmt.Fprintf(&tsvText, "%d\t%s\n", r.N, r.Text)
	}
	if got := records(t, csvText.Bytes(), "x.csv", "csv", "text"); !reflect.DeepEqual(got, texts) {
		t.Fatalf("csv text: %q", got)
	}
	if got := records(t, tsvText.Bytes(), "x.tsv", "tsv", "text"); !reflect.DeepEqual(got, texts) {
		t.Fatalf("tsv text: %q", got)
	}
	got := records(t, csvText.Bytes(), "x.csv", "csv", "")
	if want := fmt.Sprintf(`{"n":"0","text":%q}`, rows[0].Text); got[0] != want || len(got) != len(rows) {
		t.Fatalf("csv rows: %s, want %s", got[0], want)
	}
	// webdataset: the members of one key are one sample
	var tarred bytes.Buffer
	tw := tar.NewWriter(&tarred)
	put := func(name, body string) {
		tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg})
		tw.Write([]byte(body))
	}
	put("shard/000.jpg", "image 0")
	put("shard/000.json", `{"label":0}`)
	put("shard/001.jpg", "image 1")
	put("shard/001.json", `{"label":1}`)
	put("shard/002.seg.png", "mask 2")
	tw.Close()
	wf, _ := parseFormat("webdataset", "json")
	wr, wclose, _ := newRecordReader(bytes.NewReader(tarred.Bytes()), "x.tar", wf)
	var labels []string
	var werr error
	for {
		rec, err := wr.next()
		if err != nil {
			werr = err
			break
		}
		labels = append(labels, string(rec))
	}
	wclose()
	// a sample without the field's member fails the file
	if !reflect.DeepEqual(labels, []string{`{"label":0}`, `{"label":1}`}) || werr == nil || werr == io.EOF {
		t.Fatalf("webdataset json: %q, then %v", labels, werr)
	}
	samples := records(t, tarred.Bytes(), "x.tar", "webdataset", "")
	if len(samples) != 3 {
		t.Fatalf("%d samples", len(samples))
	}
	if got := decodeSample(t, []byte(samples[1])); !reflect.DeepEqual(got, map[string]string{"__key__": "shard/001", "jpg": "image 1", "json": `{"label":1}`}) {
		t.Fatalf("sample 1: %v", got)
	}
	if got := decodeSample(t, []byte(samples[2])); got["seg.png"] != "mask 2" {
		t.Fatalf("sample 2: %v", got)
	}
}

func decodeSample(t *testing.T, b []byte) map[string]string {
	t.Helper()
	out := map[string]string{}
	n := binary.LittleEndian.Uint32(b)
	b = b[4:]
	for i := uint32(0); i < n; i++ {
		l := binary.LittleEndian.Uint32(b)
		name := string(b[4 : 4+l])
		b = b[4+l:]
		l = binary.LittleEndian.Uint32(b)
		out[name] = string(b[4 : 4+l])
		b = b[4+l:]
	}
	if len(b) != 0 {
		t.Fatalf("%d bytes after the sample", len(b))
	}
	return out
}

// A stream of a Parquet source reads its files at random, chunk by checked
// chunk, and resumes at any position.
func TestAParquetSourceIsStreamedAndResumed(t *testing.T) {
	dir := t.TempDir()
	var files []File
	var want []string
	for i := 0; i < 2; i++ {
		rows := someRows(7)
		for j := range rows {
			rows[j].Text = fmt.Sprintf("f%d-%d", i, j)
			want = append(want, rows[j].Text)
		}
		data := parquetOf(t, rows, 3)
		name := fmt.Sprintf("p-%d.parquet", i)
		os.WriteFile(filepath.Join(dir, name), data, 0o644)
		f, err := Identify(bytes.NewReader(data), name, 256) // many chunks
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	spec := Spec{Sources: []SourceSpec{{Name: "p", Weight: 1, Format: "parquet", Field: "text", Files: files}}, Seed: 3, Block: 1, Epochs: 1}
	full := collect(t, spec, dir, 1, nil)[0]
	if len(full) != len(want) {
		t.Fatalf("%d items, want %d", len(full), len(want))
	}
	for i, item := range full {
		if item != fmt.Sprintf("%d.0:%s", i+1, want[i]) {
			t.Fatalf("item %d: %s, want %s", i, item, want[i])
		}
	}
	from := collect(t, spec, dir, 1, []Position{{Record: 9}})[0]
	if !reflect.DeepEqual(from, full[9:]) {
		t.Fatalf("from record 9: %v", from)
	}
}
