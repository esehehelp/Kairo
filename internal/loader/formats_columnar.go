package loader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/parquet-go/parquet-go"
)

// columnarBatchRows is how many rows a columnar file is read at a time.
const columnarBatchRows = 1024

// batchReader is an Arrow file's record batches in order, each read once.
type batchReader interface {
	next() (arrow.RecordBatch, error) // io.EOF after the last
	close()
}

// rowsReader gives an Arrow file's rows as records: with a field, that
// string (or binary) column's value; without, the row as a JSON object.
type rowsReader struct {
	name    string
	field   string
	batches batchReader
	rows    [][]byte
	row     int64
}

func (r *rowsReader) next() ([]byte, error) {
	for len(r.rows) == 0 {
		batch, err := r.batches.next()
		if err != nil {
			return nil, err
		}
		if r.rows, err = r.records(batch); err != nil {
			return nil, err
		}
	}
	out := r.rows[0]
	r.rows = r.rows[1:]
	r.row++
	return out, nil
}

// records are a batch's rows as records.
func (r *rowsReader) records(batch arrow.RecordBatch) ([][]byte, error) {
	n := int(batch.NumRows())
	out := make([][]byte, 0, n)
	if r.field == "" {
		body, err := batch.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("%s row %d: %w", r.name, r.row+1, err)
		}
		var rows []json.RawMessage
		if err := json.Unmarshal(body, &rows); err != nil {
			return nil, fmt.Errorf("%s row %d: %w", r.name, r.row+1, err)
		}
		for i, row := range rows {
			if len(row) > maxRecordBytes {
				return nil, fmt.Errorf("%s row %d: more than %d bytes", r.name, r.row+int64(i)+1, maxRecordBytes)
			}
			out = append(out, []byte(row))
		}
		return out, nil
	}
	index := batch.Schema().FieldIndices(r.field)
	if len(index) != 1 {
		return nil, fmt.Errorf("%s: no column %q (one, at the top level)", r.name, r.field)
	}
	column := batch.Column(index[0])
	value := func(i int) ([]byte, bool) {
		switch c := column.(type) {
		case *array.String:
			return []byte(c.Value(i)), true
		case *array.LargeString:
			return []byte(c.Value(i)), true
		case *array.StringView:
			return []byte(c.Value(i)), true
		case *array.Binary:
			return c.Value(i), true
		case *array.LargeBinary:
			return c.Value(i), true
		case *array.BinaryView:
			return c.Value(i), true
		}
		return nil, false
	}
	for i := 0; i < n; i++ {
		if column.IsNull(i) {
			return nil, fmt.Errorf("%s row %d: column %q is null", r.name, r.row+int64(i)+1, r.field)
		}
		v, ok := value(i)
		if !ok {
			return nil, fmt.Errorf("%s: column %q is %s, not a string or binary column", r.name, r.field, column.DataType())
		}
		if len(v) > maxRecordBytes {
			return nil, fmt.Errorf("%s row %d: more than %d bytes", r.name, r.row+int64(i)+1, maxRecordBytes)
		}
		out = append(out, bytes.Clone(v))
	}
	return out, nil
}

// arrowMagic opens an Arrow IPC file (random access); without it, a file
// is an Arrow IPC stream (what Hugging Face datasets saves).
var arrowMagic = []byte("ARROW1")

// arrowBatches reads an Arrow IPC file or stream's record batches in order.
type arrowBatches struct {
	file   *ipc.FileReader
	at     int
	stream *ipc.Reader
}

func newArrowBatches(r io.ReaderAt, size int64, name string) (*arrowBatches, error) {
	head := make([]byte, len(arrowMagic))
	if _, err := r.ReadAt(head, 0); err != nil && err != io.EOF {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if bytes.Equal(head, arrowMagic) {
		f, err := ipc.NewFileReader(io.NewSectionReader(r, 0, size), ipc.WithAllocator(memory.DefaultAllocator))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		return &arrowBatches{file: f}, nil
	}
	s, err := ipc.NewReader(io.NewSectionReader(r, 0, size), ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return &arrowBatches{stream: s}, nil
}

func (a *arrowBatches) next() (arrow.RecordBatch, error) {
	if a.file != nil {
		if a.at >= a.file.NumRecords() {
			return nil, io.EOF
		}
		batch, err := a.file.RecordBatch(a.at)
		if err != nil {
			return nil, err
		}
		a.at++
		return batch, nil
	}
	if a.stream.Next() {
		return a.stream.RecordBatch(), nil
	}
	if err := a.stream.Err(); err != nil && err != io.EOF {
		return nil, err
	}
	return nil, io.EOF
}

func (a *arrowBatches) close() {
	if a.file != nil {
		a.file.Close()
	} else {
		a.stream.Release()
	}
}

// parquetReader gives a Parquet file's rows in order, row group by row
// group: with a field, that top-level string (or binary) column's values,
// read alone; without, each row as a JSON object.
type parquetReader struct {
	name  string
	file  *parquet.File
	field string
	// whole rows
	rows *parquet.GenericReader[any]
	buf  []any
	// one column
	leaf   parquet.LeafColumn
	group  int
	pages  parquet.Pages
	values parquet.ValueReader
	page   parquet.Page
	vals   []parquet.Value
	row    int64
}

func newParquetReader(r io.ReaderAt, size int64, name, field string) (*parquetReader, error) {
	f, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	p := &parquetReader{name: name, file: f, field: field}
	if field == "" {
		p.rows = parquet.NewGenericReader[any](r, f.Schema())
		return p, nil
	}
	leaf, ok := f.Schema().Lookup(field)
	if !ok {
		return nil, fmt.Errorf("%s: no column %q", name, field)
	}
	if leaf.MaxRepetitionLevel > 0 || leaf.Node.Type().Kind() != parquet.ByteArray {
		return nil, fmt.Errorf("%s: column %q is not a top-level string or binary column", name, field)
	}
	p.leaf = leaf
	return p, nil
}

func (p *parquetReader) next() ([]byte, error) {
	if p.rows != nil {
		if cap(p.buf) == 0 {
			p.buf = make([]any, 1)
		}
		n, err := p.rows.Read(p.buf[:1])
		if n == 0 {
			if err == nil {
				err = io.EOF
			}
			return nil, err
		}
		p.row++
		out, merr := json.Marshal(p.buf[0])
		if merr != nil {
			return nil, fmt.Errorf("%s row %d: %w", p.name, p.row, merr)
		}
		if len(out) > maxRecordBytes {
			return nil, fmt.Errorf("%s row %d: more than %d bytes", p.name, p.row, maxRecordBytes)
		}
		return out, nil
	}
	for {
		if len(p.vals) > 0 {
			v := p.vals[0]
			p.vals = p.vals[1:]
			p.row++
			if v.IsNull() {
				return nil, fmt.Errorf("%s row %d: column %q is null", p.name, p.row, p.field)
			}
			b := v.ByteArray()
			if len(b) > maxRecordBytes {
				return nil, fmt.Errorf("%s row %d: more than %d bytes", p.name, p.row, maxRecordBytes)
			}
			return bytes.Clone(b), nil
		}
		if p.values != nil {
			buf := make([]parquet.Value, columnarBatchRows)
			n, err := p.values.ReadValues(buf)
			p.vals = buf[:n]
			if n > 0 {
				continue
			}
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("%s: %w", p.name, err)
			}
			parquet.Release(p.page)
			p.values, p.page = nil, nil
		}
		if p.pages != nil {
			page, err := p.pages.ReadPage()
			if err == io.EOF {
				p.pages.Close()
				p.pages = nil
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("%s: %w", p.name, err)
			}
			p.page, p.values = page, page.Values()
			continue
		}
		groups := p.file.RowGroups()
		if p.group >= len(groups) {
			return nil, io.EOF
		}
		p.pages = groups[p.group].ColumnChunks()[p.leaf.ColumnIndex].Pages()
		p.group++
	}
}

func (p *parquetReader) close() {
	if p.rows != nil {
		p.rows.Close()
	}
	if p.page != nil {
		parquet.Release(p.page)
	}
	if p.pages != nil {
		p.pages.Close()
	}
}

// parquetRows is how many rows a Parquet file says it holds.
func parquetRows(r io.ReaderAt, size int64) (int64, error) {
	f, err := parquet.OpenFile(r, size)
	if err != nil {
		return 0, err
	}
	return f.NumRows(), nil
}
