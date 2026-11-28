package loader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// File is one file of raw data as Kairo identifies it: where it is fetched
// from, its size and sha256, and the sha256 of each ChunkBytes of it (the
// last chunk the rest). No byte of a chunk is used before the chunk is
// checked.
type File struct {
	Name       string   `json:"name"`
	URL        string   `json:"url,omitempty"`
	Size       int64    `json:"size"`
	SHA256     string   `json:"sha256"`
	ChunkBytes int64    `json:"chunk_bytes"`
	Chunks     []string `json:"chunks"`
	// PublishedSHA256 and PublishedSize are what the file was declared
	// with (a Hugging Face listing's sha256 and size; PublishedSize nil when
	// none was), kept beside its identity: a file whose identity is not
	// what was published is changed.
	PublishedSHA256 string `json:"published_sha256,omitempty"`
	PublishedSize   *int64 `json:"published_size,omitempty"`
}

// AsPublished reports why f's identity is not what was published of it
// ("" when it is, or nothing was).
func (f File) AsPublished() string {
	switch {
	case f.PublishedSHA256 != "" && f.PublishedSHA256 != f.SHA256:
		return fmt.Sprintf("%s has sha256 %s, published as %s", f.Name, f.SHA256, f.PublishedSHA256)
	case f.PublishedSize != nil && *f.PublishedSize != f.Size:
		return fmt.Sprintf("%s has %d bytes, published as %d", f.Name, f.Size, *f.PublishedSize)
	}
	return ""
}

// DefaultChunkBytes is the chunk a file is checked by.
const DefaultChunkBytes = 4 << 20

var hexSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)

// check: f's chunks cover its size, each a sha256.
func (f File) check() error {
	if f.Name == "" || f.Size < 0 || f.ChunkBytes < 1 || !hexSHA256.MatchString(f.SHA256) {
		return fmt.Errorf("file %q: a name, a size, a chunk size and a sha256 are needed", f.Name)
	}
	if want := (f.Size + f.ChunkBytes - 1) / f.ChunkBytes; int64(len(f.Chunks)) != want {
		return fmt.Errorf("file %s: %d chunk hashes for %d bytes in chunks of %d (%d)", f.Name, len(f.Chunks), f.Size, f.ChunkBytes, want)
	}
	for i, c := range f.Chunks {
		if !hexSHA256.MatchString(c) {
			return fmt.Errorf("file %s: chunk %d's hash is not a sha256", f.Name, i)
		}
	}
	return nil
}

// Identified reports whether f has its identity (each chunk's sha256).
func (f File) Identified() bool { return f.ChunkBytes > 0 }

// chunkSize is the size of chunk i of f.
func (f File) chunkSize(i int) int64 {
	if rest := f.Size - int64(i)*f.ChunkBytes; rest < f.ChunkBytes {
		return rest
	}
	return f.ChunkBytes
}

// ErrChanged: the bytes of a file are not those its identity lists (fetched
// twice): what it was made from is not there any more.
var ErrChanged = errors.New("raw data changed")

// Origin gives the bytes of a file as stored, from a byte offset on
// (unchecked: Fetch checks them).
type Origin interface {
	ReadFrom(ctx context.Context, f File, offset int64) (io.ReadCloser, error)
	String() string
}

// Fetcher reads files from an origin, checking each chunk, and reading again
// what failed: an error of the origin is tried again after a growing pause
// (Problem says what), bytes not matching their hash are fetched once more
// before ErrChanged.
type Fetcher struct {
	Origin Origin
	// Retry is the first pause after a failure (default 1 s), doubling up to
	// MaxRetry (default 5 min).
	Retry, MaxRetry time.Duration
	// Problem, when set, hears each failure and, with a nil error, that the
	// file is being read again.
	Problem func(f File, err error)
	// GiveUp, when set, ends retrying after that long of failures in a row,
	// with the last failure (0: retried until the context ends).
	GiveUp time.Duration
	// reask asks Resolve again (set on a fetcher Resolve made): where to
	// read a file may have changed (kept on the node since)
	reask func(ctx context.Context) (File, Origin, error)
	// Resolve, when set, is asked for each file before it is read: the file
	// as identified and where to read it from (the node's raw cache,
	// agent.go). A file is then never read unidentified.
	Resolve func(ctx context.Context, f File) (File, Origin, error)
}

// Open reads f from chunk on. A file never read before (no chunk hashes:
// Identified) is read from its start, its identity made as it is read; its
// sha256 and size, when published, are checked at its end.
func (x *Fetcher) Open(ctx context.Context, f File, chunk int) (io.ReadCloser, error) {
	if x.Resolve != nil {
		resolved, origin, err := x.Resolve(ctx, f)
		if err != nil {
			return nil, err
		}
		if !resolved.Identified() {
			return nil, fmt.Errorf("file %s was resolved without its identity", f.Name)
		}
		via := *x
		via.Resolve, via.Origin = nil, origin
		via.reask = func(ctx context.Context) (File, Origin, error) { return x.Resolve(ctx, f) }
		return via.Open(ctx, resolved, chunk)
	}
	if !f.Identified() {
		if f.Name == "" || chunk != 0 || f.SHA256 != "" && !hexSHA256.MatchString(f.SHA256) {
			return nil, fmt.Errorf("file %q, not identified yet, is read from its start", f.Name)
		}
		return &identifyingReader{ctx: ctx, x: x, f: f, whole: sha256.New(), chunk: sha256.New()}, nil
	}
	if err := f.check(); err != nil {
		return nil, err
	}
	if chunk < 0 || chunk > len(f.Chunks) {
		return nil, fmt.Errorf("file %s has no chunk %d", f.Name, chunk)
	}
	return &checkedReader{ctx: ctx, x: x, f: f, chunk: chunk}, nil
}

// checkedReader gives a file's bytes a checked chunk at a time.
type checkedReader struct {
	ctx      context.Context
	x        *Fetcher
	f        File
	chunk    int    // the next chunk to read
	buf      []byte // the checked chunk being given
	body     io.ReadCloser
	mismatch int // chunk read again for not matching, +1
}

func (c *checkedReader) Read(p []byte) (int, error) {
	for len(c.buf) == 0 {
		if c.chunk >= len(c.f.Chunks) {
			return 0, io.EOF
		}
		if err := c.fill(); err != nil {
			return 0, err
		}
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

// fill reads and checks the next chunk, reading again until it is had,
// the bytes are not the file's, or the context ends.
func (c *checkedReader) fill() error {
	pause := c.x.Retry
	if pause <= 0 {
		pause = time.Second
	}
	maxPause := c.x.MaxRetry
	if maxPause <= 0 {
		maxPause = 5 * time.Minute
	}
	failed := false
	var failingSince time.Time
	for {
		data, err := c.readChunk()
		if errors.Is(err, errLength) {
			// shorter or longer than identified: as bytes not matching
			data, err = nil, nil
		}
		if err == nil {
			sum := sha256.Sum256(data)
			matches := data != nil && hex.EncodeToString(sum[:]) == c.f.Chunks[c.chunk]
			end := true
			if matches && c.chunk+1 == len(c.f.Chunks) {
				// a read failing there tells nothing of the length: read again
				if end, err = c.atEnd(); err != nil {
					err = fmt.Errorf("%s past its last chunk from %s: %w", c.f.Name, c.x.Origin, err)
				}
			}
			if err == nil && matches && end {
				if failed && c.x.Problem != nil {
					c.x.Problem(c.f, nil)
				}
				c.buf, c.chunk, c.mismatch = data, c.chunk+1, 0
				return nil
			}
			if err == nil {
				c.closeBody()
				if c.mismatch == c.chunk+1 {
					return fmt.Errorf("%w: %s chunk %d from %s is not what was identified (its bytes or the file's length)", ErrChanged, c.f.Name, c.chunk, c.x.Origin)
				}
				c.mismatch = c.chunk + 1
				err = fmt.Errorf("%s chunk %d from %s does not match its sha256: reading it again", c.f.Name, c.chunk, c.x.Origin)
			}
		}
		c.closeBody()
		if !failed {
			failingSince = time.Now()
		}
		failed = true
		if c.x.Problem != nil {
			c.x.Problem(c.f, err)
		}
		if c.x.GiveUp > 0 && time.Since(failingSince) > c.x.GiveUp {
			return err
		}
		if c.x.reask != nil && time.Since(failingSince) > reaskAfter {
			// no progress from here: asked again where to read it
			if again, origin, rerr := c.x.reask(c.ctx); rerr == nil && again.SHA256 == c.f.SHA256 && again.Size == c.f.Size && equalChunks(again.Chunks, c.f.Chunks) {
				via := *c.x
				via.Origin = origin
				c.x, failingSince = &via, time.Now()
			}
		}
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-time.After(pause):
		}
		if pause *= 2; pause > maxPause {
			pause = maxPause
		}
	}
}

// reaskAfter is how long a file read from somewhere makes no progress
// before the fetcher's Resolve is asked again where to read it.
const reaskAfter = 3 * time.Minute

func equalChunks(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// readChunk reads the next chunk's bytes from the origin, opening it there
// when no body is open.
func (c *checkedReader) readChunk() ([]byte, error) {
	if c.body == nil {
		body, err := c.x.Origin.ReadFrom(c.ctx, c.f, int64(c.chunk)*c.f.ChunkBytes)
		if err != nil {
			return nil, err // errLength among them
		}
		c.body = body
	}
	data := make([]byte, c.f.chunkSize(c.chunk))
	if _, err := io.ReadFull(c.body, data); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: %s ends before its chunk %d", errLength, c.f.Name, c.chunk)
		}
		return nil, fmt.Errorf("%s chunk %d from %s: %w", c.f.Name, c.chunk, c.x.Origin, err)
	}
	return data, nil
}

// atEnd reports whether the body has nothing past the file's last chunk:
// its end read (io.EOF), not a read failing (err).
func (c *checkedReader) atEnd() (bool, error) {
	var one [1]byte
	for {
		n, err := c.body.Read(one[:])
		switch {
		case n > 0:
			return false, nil
		case errors.Is(err, io.EOF):
			return true, nil
		case err != nil:
			return false, err
		}
	}
}

// errLength: what was read is not as long as the file was identified.
var errLength = errors.New("length not as identified")

func (c *checkedReader) closeBody() {
	if c.body != nil {
		c.body.Close()
		c.body = nil
	}
}

// Identity is the file as identified (what was checked).
func (c *checkedReader) Identity() (File, bool) { return c.f, true }

func (c *checkedReader) Close() error {
	c.closeBody()
	return nil
}

// identifyingReader reads a file the first time, making its identity: the
// sha256 of the whole and of each DefaultChunkBytes. Its bytes are not
// checked as they are given (there is nothing yet to check them against),
// only at the end against a published sha256 and size.
type identifyingReader struct {
	ctx      context.Context
	x        *Fetcher
	f        File
	body     io.ReadCloser
	offset   int64
	whole    hash.Hash
	chunk    hash.Hash
	inChunk  int64
	chunks   []string
	identity *File
	broken   error
	// failingSince is when reading last stopped making progress (zero:
	// it makes progress)
	failingSince time.Time
}

func (r *identifyingReader) Read(p []byte) (int, error) {
	pause := r.x.Retry
	if pause <= 0 {
		pause = time.Second
	}
	maxPause := r.x.MaxRetry
	if maxPause <= 0 {
		maxPause = 5 * time.Minute
	}
	failed := false
	for {
		if r.identity != nil {
			return 0, io.EOF
		}
		if r.broken != nil {
			return 0, r.broken
		}
		if r.body == nil {
			body, err := r.x.Origin.ReadFrom(r.ctx, r.f, r.offset)
			if err == nil {
				r.body = body
				if failed && r.x.Problem != nil {
					r.x.Problem(r.f, nil)
				}
				failed = false
				continue
			}
			failed = true
			if r.x.Problem != nil {
				r.x.Problem(r.f, err)
			}
			if r.givingUp() {
				return 0, err
			}
			select {
			case <-r.ctx.Done():
				return 0, r.ctx.Err()
			case <-time.After(pause):
			}
			if pause *= 2; pause > maxPause {
				pause = maxPause
			}
			continue
		}
		n, err := r.body.Read(p)
		r.take(p[:n])
		if n > 0 {
			r.failingSince = time.Time{}
		}
		if err == io.EOF {
			return n, r.finish()
		}
		if err != nil {
			r.body.Close()
			r.body = nil
			if r.x.Problem != nil {
				r.x.Problem(r.f, err)
			}
			if r.offset > 0 {
				// another response from here may be of another version of
				// the file: what is identified is read in one piece
				r.broken = fmt.Errorf("%s broke off at byte %d the first time it was read (it is read again from its start): %w", r.f.URL, r.offset, err)
				return n, nil
			}
			failed = true
			if n > 0 {
				return n, nil
			}
			if r.givingUp() {
				return 0, err
			}
			select {
			case <-r.ctx.Done():
				return 0, r.ctx.Err()
			case <-time.After(pause):
			}
			if pause *= 2; pause > maxPause {
				pause = maxPause
			}
			continue
		}
		return n, nil
	}
}

// givingUp: reading made no progress for the fetcher's GiveUp (a failing
// stretch starts now when none is on).
func (r *identifyingReader) givingUp() bool {
	if r.failingSince.IsZero() {
		r.failingSince = time.Now()
	}
	return r.x.GiveUp > 0 && time.Since(r.failingSince) > r.x.GiveUp
}

// take hashes what was read.
func (r *identifyingReader) take(b []byte) {
	r.whole.Write(b)
	r.offset += int64(len(b))
	for len(b) > 0 {
		room := DefaultChunkBytes - r.inChunk
		part := b
		if int64(len(part)) > room {
			part = part[:room]
		}
		r.chunk.Write(part)
		r.inChunk += int64(len(part))
		b = b[len(part):]
		if r.inChunk == DefaultChunkBytes {
			r.chunks = append(r.chunks, hex.EncodeToString(r.chunk.Sum(nil)))
			r.chunk.Reset()
			r.inChunk = 0
		}
	}
}

// finish makes the identity once the file was read to its end, checked
// against what was published of it.
func (r *identifyingReader) finish() error {
	if r.inChunk > 0 {
		r.chunks = append(r.chunks, hex.EncodeToString(r.chunk.Sum(nil)))
	}
	f := r.f
	sum := hex.EncodeToString(r.whole.Sum(nil))
	switch {
	case f.SHA256 != "" && f.SHA256 != sum:
		return fmt.Errorf("%w: %s has sha256 %s, published as %s", ErrChanged, f.Name, sum, f.SHA256)
	case f.Size > 0 && f.Size != r.offset:
		return fmt.Errorf("%w: %s has %d bytes, published as %d", ErrChanged, f.Name, r.offset, f.Size)
	}
	f.Size, f.SHA256, f.ChunkBytes, f.Chunks = r.offset, sum, DefaultChunkBytes, r.chunks
	if f.Chunks == nil {
		f.Chunks = []string{}
	}
	r.identity = &f
	return io.EOF
}

// Identity is the file's identity once it was read to its end.
func (r *identifyingReader) Identity() (File, bool) {
	if r.identity == nil {
		return File{}, false
	}
	return *r.identity, true
}

func (r *identifyingReader) Close() error {
	if r.body != nil {
		r.body.Close()
		r.body = nil
	}
	return nil
}

// DirOrigin reads files under a directory, by name.
type DirOrigin struct{ Root string }

func (d DirOrigin) ReadFrom(_ context.Context, f File, offset int64) (io.ReadCloser, error) {
	if !filepath.IsLocal(filepath.FromSlash(f.Name)) {
		return nil, fmt.Errorf("file %q is not under %s", f.Name, d.Root)
	}
	file, err := os.Open(filepath.Join(d.Root, filepath.FromSlash(f.Name)))
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

func (d DirOrigin) String() string { return d.Root }

// HTTPOrigin reads files from their URL with a range request; a body that
// sends nothing for Idle (default 2 minutes) while read is given up.
type HTTPOrigin struct {
	Client *http.Client
	Idle   time.Duration
}

func (h HTTPOrigin) ReadFrom(ctx context.Context, f File, offset int64) (_ io.ReadCloser, err error) {
	if !strings.HasPrefix(f.URL, "https://") && !strings.HasPrefix(f.URL, "http://") {
		return nil, fmt.Errorf("file %s has no http(s) URL", f.Name)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()
	idle := h.Idle
	if idle <= 0 {
		idle = 2 * time.Minute
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.URL, nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}
	client := h.Client
	if client == nil {
		client = HTTPClient
	}
	waited := time.AfterFunc(idle, cancel) // the answer's headers, redirects and all
	resp, err := client.Do(req)
	if !waited.Stop() && err == nil {
		resp.Body.Close()
		err = fmt.Errorf("%s: no answer within %s", f.URL, idle)
	}
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusPartialContent && offset > 0:
		var start, end, total int64
		if _, err := fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total); err != nil || start != offset {
			resp.Body.Close()
			return nil, fmt.Errorf("%s: asked from byte %d, answered %q", f.URL, offset, resp.Header.Get("Content-Range"))
		}
		if f.Identified() && total != f.Size {
			resp.Body.Close()
			return nil, fmt.Errorf("%w: %s has %d bytes, identified as %d", errLength, f.URL, total, f.Size)
		}
		return newIdleBody(resp.Body, idle, cancel), nil
	case resp.StatusCode == http.StatusOK:
		if f.Identified() && resp.ContentLength >= 0 && resp.ContentLength != f.Size {
			resp.Body.Close()
			return nil, fmt.Errorf("%w: %s has %d bytes, identified as %d", errLength, f.URL, resp.ContentLength, f.Size)
		}
		body := newIdleBody(resp.Body, idle, cancel)
		if _, err := io.CopyN(io.Discard, body, offset); err != nil { // no ranges there
			body.Close()
			return nil, err
		}
		return body, nil
	}
	resp.Body.Close()
	return nil, fmt.Errorf("%s: %s", f.URL, resp.Status)
}

func (h HTTPOrigin) String() string { return "upstream" }

// HTTPClient is the client of an HTTPOrigin without one (and of Kairo's
// other reads of upstream): an answer's headers waited for 2 minutes at
// most.
var HTTPClient = func() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.ResponseHeaderTimeout = 2 * time.Minute
	return &http.Client{Transport: t}
}()

// idleBody is a response body given up (its request canceled) when a read
// of it waits idle for a byte.
type idleBody struct {
	body   io.ReadCloser
	idle   time.Duration
	cancel context.CancelFunc
	timer  *time.Timer
}

func newIdleBody(body io.ReadCloser, idle time.Duration, cancel context.CancelFunc) *idleBody {
	b := &idleBody{body: body, idle: idle, cancel: cancel, timer: time.AfterFunc(idle, cancel)}
	b.timer.Stop()
	return b
}

func (b *idleBody) Read(p []byte) (int, error) {
	b.timer.Reset(b.idle)
	n, err := b.body.Read(p)
	if !b.timer.Stop() && err != nil {
		err = fmt.Errorf("nothing came for %s: %w", b.idle, err)
	}
	return n, err
}

func (b *idleBody) Close() error {
	b.timer.Stop()
	b.cancel()
	return b.body.Close()
}

// Identify reads r to its end and makes the identity of a file of that
// content: its size, sha256 and chunk hashes.
func Identify(r io.Reader, name string, chunkBytes int64) (File, error) {
	if chunkBytes < 1 {
		chunkBytes = DefaultChunkBytes
	}
	f := File{Name: name, ChunkBytes: chunkBytes}
	whole := sha256.New()
	buf := make([]byte, chunkBytes)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			sum := sha256.Sum256(buf[:n])
			f.Chunks = append(f.Chunks, hex.EncodeToString(sum[:]))
			whole.Write(buf[:n])
			f.Size += int64(n)
		}
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return File{}, err
		}
	}
	f.SHA256 = hex.EncodeToString(whole.Sum(nil))
	return f, nil
}
