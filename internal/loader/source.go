package loader

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// SourceSpec is a source of a stream, its files resolved: the raw data (or
// data object) named, its weight in the mix, its format, its files in
// declared order and, when known, each file's record count read so (by
// RecordsKey), which lets a stream start mid-way without reading what comes
// before.
type SourceSpec struct {
	Name    string           `json:"name"`
	Weight  int              `json:"weight"`
	Format  string           `json:"format"`
	Field   string           `json:"field,omitempty"`
	Files   []File           `json:"files"`
	Records map[string]int64 `json:"records,omitempty"`
}

// sourceStream gives a source's records, epoch after epoch: in each, its
// files in declared order, or permuted by (seed, source, epoch) when the
// stream shuffles files.
type sourceStream struct {
	spec    SourceSpec
	index   int
	format  format
	fetch   *Fetcher
	shuffle bool
	seed    uint64
	// learned hears a file's record count once it was read to its end;
	// identified a file's identity once it was read the first time.
	learned    func(source int, f File, records int64)
	identified func(source int, f File)

	// endEpoch: this source's records end where its epoch endEpoch would
	// begin (0: they do not); nothing of that epoch is opened
	endEpoch int64

	epoch   int64
	order   []int
	file    int // the position in order of the file being read
	reader  recordReader
	closeFn func()
	body    io.Closer
	inFile  int64 // records given from that file
	read    int64 // records given since the source's first
}

func newSourceStream(spec SourceSpec, index int, fetch *Fetcher, shuffle bool, seed uint64, learned func(int, File, int64), identified func(int, File)) (*sourceStream, error) {
	f, err := parseFormat(spec.Format, spec.Field)
	if err != nil {
		return nil, fmt.Errorf("source %s: %w", spec.Name, err)
	}
	if len(spec.Files) == 0 {
		return nil, fmt.Errorf("source %s has no files", spec.Name)
	}
	for _, file := range spec.Files {
		if !file.Identified() {
			continue // identified the first time it is read
		}
		if err := file.check(); err != nil {
			return nil, fmt.Errorf("source %s: %w", spec.Name, err)
		}
	}
	spec.Files = append([]File(nil), spec.Files...)
	records := make(map[string]int64, len(spec.Records))
	for k, v := range spec.Records {
		records[k] = v
	}
	spec.Records = records
	return &sourceStream{spec: spec, index: index, format: f, fetch: fetch, shuffle: shuffle, seed: seed, learned: learned, identified: identified}, nil
}

// orderOf is the order of the files in epoch e.
func (s *sourceStream) orderOf(e int64) []int {
	if s.shuffle {
		return permutation(len(s.spec.Files), seedOf(s.seed, "files", uint64(s.index), uint64(e)))
	}
	order := make([]int, len(s.spec.Files))
	for i := range order {
		order[i] = i
	}
	return order
}

// count is the record count of file i, when known.
func (s *sourceStream) count(i int) (int64, bool) {
	n, ok := s.spec.Records[s.recordsKey(s.spec.Files[i])]
	return n, ok
}

// recordsKey is what a file's record count is kept under: its sha256 and
// how this source reads it (RecordsKey).
func (s *sourceStream) recordsKey(f File) string {
	key, _ := RecordsKey(f.SHA256, s.spec.Format, s.spec.Field, f.Name)
	return key
}

func (s *sourceStream) closeFile() {
	if s.closeFn != nil {
		s.closeFn()
		s.closeFn = nil
	}
	if s.body != nil {
		s.body.Close()
		s.body = nil
	}
	s.reader = nil
}

func (s *sourceStream) Close() { s.closeFile() }

func (s *sourceStream) openFile(ctx context.Context) error {
	f := s.spec.Files[s.order[s.file]]
	body, err := s.fetch.Open(ctx, f, 0)
	if err != nil {
		return fmt.Errorf("source %s: %w", s.spec.Name, err)
	}
	reader, closeFn, err := newRecordReader(body, f.Name, s.format)
	if err != nil {
		body.Close()
		return fmt.Errorf("source %s: %w", s.spec.Name, err)
	}
	s.reader, s.closeFn, s.body, s.inFile = reader, closeFn, body, 0
	return nil
}

// seek puts the source before its record n (counted from its first): whole
// files of known record count are passed over; the rest are read.
func (s *sourceStream) seek(ctx context.Context, n int64) error {
	s.closeFile()
	s.epoch, s.file, s.read = 0, 0, 0
	known, perEpoch := true, int64(0)
	for i := range s.spec.Files {
		c, ok := s.count(i)
		known, perEpoch = known && ok, perEpoch+c
	}
	if known && perEpoch == 0 {
		return fmt.Errorf("source %s has no records", s.spec.Name)
	}
	if known {
		s.epoch = n / perEpoch
		if s.endEpoch > 0 && s.epoch >= s.endEpoch {
			s.epoch = s.endEpoch - 1
		}
		s.read = s.epoch * perEpoch
	}
	s.order = s.orderOf(s.epoch)
	emptyEpoch := true
	for s.read < n {
		if s.file == len(s.order) {
			if emptyEpoch {
				return fmt.Errorf("source %s has no records", s.spec.Name)
			}
			if s.endEpoch > 0 && s.epoch+1 >= s.endEpoch {
				return nil // at the end: next says so
			}
			s.epoch, s.file, s.order, emptyEpoch = s.epoch+1, 0, s.orderOf(s.epoch+1), true
			continue
		}
		if c, ok := s.count(s.order[s.file]); ok && s.read+c <= n {
			s.read += c
			s.file++
			emptyEpoch = emptyEpoch && c == 0
			continue
		}
		if s.reader == nil {
			if err := s.openFile(ctx); err != nil {
				return err
			}
		}
		_, err := s.reader.next()
		if errors.Is(err, io.EOF) {
			s.endFile()
			continue
		}
		if err != nil {
			return fmt.Errorf("source %s: %w", s.spec.Name, err)
		}
		s.inFile++
		s.read++
		emptyEpoch = false
	}
	return nil
}

// errSourceEnded: the source's last epoch gave its last record.
var errSourceEnded = errors.New("source ended")

// endFile: the file being read gave its last record.
func (s *sourceStream) endFile() {
	if r, ok := s.body.(interface{ Identity() (File, bool) }); ok {
		if f, done := r.Identity(); done {
			was := s.spec.Files[s.order[s.file]]
			s.spec.Files[s.order[s.file]] = f
			if s.identified != nil && !was.Identified() {
				s.identified(s.index, f)
			}
		}
	}
	f := s.spec.Files[s.order[s.file]]
	if _, ok := s.count(s.order[s.file]); !ok && s.learned != nil {
		s.learned(s.index, f, s.inFile)
	}
	if s.spec.Records == nil {
		s.spec.Records = map[string]int64{}
	}
	s.spec.Records[s.recordsKey(f)] = s.inFile
	s.closeFile()
	s.file++
}

// next is the source's next record and the epoch it is of.
func (s *sourceStream) next(ctx context.Context) ([]byte, int64, error) {
	if s.order == nil {
		s.order = s.orderOf(s.epoch)
	}
	emptyEpoch := s.file == 0 && s.inFile == 0
	for {
		if s.file == len(s.order) {
			if emptyEpoch {
				return nil, 0, fmt.Errorf("source %s has no records", s.spec.Name)
			}
			if s.endEpoch > 0 && s.epoch+1 >= s.endEpoch {
				return nil, 0, errSourceEnded
			}
			s.epoch, s.file, s.order, emptyEpoch = s.epoch+1, 0, s.orderOf(s.epoch+1), true
			continue
		}
		if s.reader == nil {
			if err := s.openFile(ctx); err != nil {
				return nil, 0, err
			}
		}
		rec, err := s.reader.next()
		if errors.Is(err, io.EOF) {
			s.endFile()
			continue
		}
		if err != nil {
			return nil, 0, fmt.Errorf("source %s: %w", s.spec.Name, err)
		}
		s.inFile++
		s.read++
		return rec, s.epoch, nil
	}
}
