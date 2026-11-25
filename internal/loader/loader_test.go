package loader

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// The permutations are this package's own: the same for a seed for good.
func TestPermutationsAreFixed(t *testing.T) {
	got := permutation(10, 42)
	want := goldenPermutation
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("permutation(10, 42) = %v, fixed as %v: an order made before would not be made again", got, want)
	}
	first, second := seedOf(1, "block", 0), seedOf(1, "block", 1)
	if first == second || first != seedOf(1, "block", 0) {
		t.Fatal("seeds of different blocks the same, or of one block different")
	}
	seen := map[int]bool{}
	for _, v := range permutation(1000, 7) {
		seen[v] = true
	}
	if len(seen) != 1000 {
		t.Fatalf("not a permutation: %d distinct", len(seen))
	}
}

// A mix's picks repeat every sum of weights; what each source gave before
// any input is known without replaying.
func TestMixerInterleavesByWeight(t *testing.T) {
	m, err := newMixer([]int{3, 1})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.pattern, []int{0, 0, 1, 0}) {
		t.Fatalf("pattern %v", m.pattern)
	}
	m, err = newMixer([]int{5, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	counts := make([]int64, 3)
	for n := int64(0); n < 1000; n++ {
		if got := m.before(n); !reflect.DeepEqual(got, counts) {
			t.Fatalf("before(%d) = %v, counted %v", n, got, counts)
		}
		counts[m.source(n)]++
	}
	if counts[0] != 500 || counts[1] != 200 || counts[2] != 300 {
		t.Fatalf("shares %v", counts)
	}
	for _, bad := range [][]int{nil, {0}, {1, -1}, {maxMixPeriod, 1}} {
		if _, err := newMixer(bad); err == nil {
			t.Fatalf("weights %v taken", bad)
		}
	}
}

func readAll(t *testing.T, data []byte, name, f, field string) []string {
	t.Helper()
	fm, err := parseFormat(f, field)
	if err != nil {
		t.Fatal(err)
	}
	r, closeFn, err := newRecordReader(bytes.NewReader(data), name, fm)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	var out []string
	for {
		rec, err := r.next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(rec))
	}
}

func zstdOf(t *testing.T, data []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w, err := zstd.NewWriter(&b)
	if err != nil {
		t.Fatal(err)
	}
	w.Write(data)
	w.Close()
	return b.Bytes()
}

func gzipOf(data []byte) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(data)
	w.Close()
	return b.Bytes()
}

// Each framing gives its records, decompressed by the format's suffix or
// the file's extension.
func TestFormatsFrameRecords(t *testing.T) {
	if got := readAll(t, []byte("a\r\nb\n\nc"), "x.txt", "text", ""); !reflect.DeepEqual(got, []string{"a", "b", "", "c"}) {
		t.Fatalf("text %q", got)
	}
	jsonl := []byte(`{"text":"一","n":1}` + "\n" + `{"text":"二"}` + "\n")
	if got := readAll(t, zstdOf(t, jsonl), "x.jsonl.zst", "jsonl", "text"); !reflect.DeepEqual(got, []string{"一", "二"}) {
		t.Fatalf("jsonl.zst by extension %q", got)
	}
	if got := readAll(t, gzipOf(jsonl), "x", "jsonl.gz", ""); len(got) != 2 || !strings.Contains(got[0], `"n":1`) {
		t.Fatalf("jsonl.gz by format, whole lines %q", got)
	}
	fm, _ := parseFormat("jsonl", "title")
	r, closeFn, _ := newRecordReader(bytes.NewReader(jsonl), "x.jsonl", fm)
	defer closeFn()
	if _, err := r.next(); err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("a line without the field: %v", err)
	}
	var lb bytes.Buffer
	for _, rec := range []string{"xy", "", "zzz"} {
		binary.Write(&lb, binary.LittleEndian, uint32(len(rec)))
		lb.WriteString(rec)
	}
	if got := readAll(t, lb.Bytes(), "x.bin", "lenbin", ""); !reflect.DeepEqual(got, []string{"xy", "", "zzz"}) {
		t.Fatalf("lenbin %q", got)
	}
	if got := readAll(t, []byte("whole\nfile"), "x", "file", ""); !reflect.DeepEqual(got, []string{"whole\nfile"}) {
		t.Fatalf("file %q", got)
	}
	for _, bad := range []struct{ f, field string }{{"csv", ""}, {"text.xz", ""}, {"text", "x"}} {
		if _, err := parseFormat(bad.f, bad.field); err == nil {
			t.Fatalf("format %+v taken", bad)
		}
	}
}

// flakyOrigin fails its first reads, and gives bad bytes for chunk bad.
type flakyOrigin struct {
	DirOrigin
	mu      sync.Mutex
	fail    int
	bad     int64 // offset of a chunk given wrong (-1 none)
	badOnce bool  // wrong only the first time
	badSeen int
}

func (o *flakyOrigin) ReadFrom(ctx context.Context, f File, offset int64) (io.ReadCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fail > 0 {
		o.fail--
		return nil, errors.New("connection refused")
	}
	body, err := o.DirOrigin.ReadFrom(ctx, f, offset)
	if err != nil || o.bad < 0 || offset > o.bad || o.badOnce && o.badSeen > 0 {
		return body, err
	}
	o.badSeen++
	data, _ := io.ReadAll(body)
	body.Close()
	data[o.bad-offset] ^= 0xff
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Every chunk is checked: a failing origin is read again after a pause, a
// chunk not matching is fetched once more, and twice is the data changed.
func TestFetcherChecksEachChunk(t *testing.T) {
	dir := t.TempDir()
	data := bytes.Repeat([]byte("0123456789"), 1000)
	os.WriteFile(filepath.Join(dir, "f"), data, 0o644)
	f, err := Identify(bytes.NewReader(data), "f", 4096)
	if err != nil || f.Size != 10000 || len(f.Chunks) != 3 {
		t.Fatalf("identity %+v %v", f, err)
	}
	var problems []string
	read := func(o Origin, chunk int) ([]byte, error) {
		x := &Fetcher{Origin: o, Retry: time.Millisecond, Problem: func(_ File, err error) { problems = append(problems, fmt.Sprint(err)) }}
		r, err := x.Open(context.Background(), f, chunk)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	}
	got, err := read(&flakyOrigin{DirOrigin: DirOrigin{dir}, fail: 2, bad: 5000, badOnce: true}, 0)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read again: %v (%d bytes)", err, len(got))
	}
	// two refusals, read again; a bad chunk, fetched again
	if len(problems) != 5 || problems[2] != "<nil>" || !strings.Contains(problems[3], "does not match") || problems[4] != "<nil>" {
		t.Fatalf("problems %q", problems)
	}
	if got, err = read(DirOrigin{dir}, 2); err != nil || !bytes.Equal(got, data[8192:]) {
		t.Fatalf("from chunk 2: %v", err)
	}
	if _, err = read(&flakyOrigin{DirOrigin: DirOrigin{dir}, bad: 9000}, 0); !errors.Is(err, ErrChanged) {
		t.Fatalf("bytes changed: %v", err)
	}
}

// testData writes two sources' files and returns the spec reading them.
func testData(t *testing.T) (Spec, string) {
	t.Helper()
	dir := t.TempDir()
	var a, b []File
	for i := 0; i < 4; i++ {
		var buf bytes.Buffer
		for j := 0; j < 5+i*3; j++ {
			fmt.Fprintf(&buf, "{\"text\":\"a%d-%d\"}\n", i, j)
		}
		data := zstdOf(t, buf.Bytes())
		name := fmt.Sprintf("a/part-%d.jsonl.zst", i)
		os.MkdirAll(filepath.Join(dir, "a"), 0o755)
		os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), data, 0o644)
		f, err := Identify(bytes.NewReader(data), name, 64)
		if err != nil {
			t.Fatal(err)
		}
		a = append(a, f)
	}
	for i := 0; i < 2; i++ {
		var buf bytes.Buffer
		for j := 0; j < 4; j++ {
			fmt.Fprintf(&buf, "b%d-%d\n", i, j)
		}
		name := fmt.Sprintf("b-%d.txt", i)
		os.WriteFile(filepath.Join(dir, name), buf.Bytes(), 0o644)
		f, _ := Identify(bytes.NewReader(buf.Bytes()), name, 16)
		b = append(b, f)
	}
	return Spec{
		Sources: []SourceSpec{
			{Name: "a", Weight: 3, Format: "jsonl", Field: "text", Files: a},
			{Name: "b", Weight: 1, Format: "text", Files: b},
		},
		Seed: 99, Block: 7, ShuffleFiles: true, Epochs: 2,
	}, dir
}

// collect takes every rank's items from a loader made from positions.
func collect(t *testing.T, spec Spec, dir string, world int, positions []Position) [][]string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{Loader: &Loader{Spec: spec, Fetch: &Fetcher{Origin: DirOrigin{dir}}}, Positions: positions, World: world}
	go srv.Serve(ctx, ln)
	out := make([][]string, world)
	var wg sync.WaitGroup
	errs := make(chan error, world)
	for r := 0; r < world; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			c, err := Dial(ctx, "tcp", ln.Addr().String(), r, world, nil)
			if err != nil {
				errs <- err
				return
			}
			defer c.Close()
			for {
				item, err := c.Next()
				if err == io.EOF {
					return
				}
				if err != nil {
					errs <- err
					return
				}
				out[r] = append(out[r], fmt.Sprintf("%d.%d:%s", c.Position.Record, c.Position.Sub, item))
			}
		}(r)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	return out
}

// A stream is one fixed order: the same twice, each source's epochs whole
// (every record once per epoch), and from any position the same as what
// followed it from the start, whether file record counts are known or not.
func TestAStreamIsTheSameFromAnyPosition(t *testing.T) {
	spec, dir := testData(t)
	full := collect(t, spec, dir, 2, nil)
	if again := collect(t, spec, dir, 2, nil); !reflect.DeepEqual(full, again) {
		t.Fatal("two runs gave two orders")
	}
	total := len(full[0]) + len(full[1])
	seen := map[string]int{}
	for _, rank := range full {
		for _, it := range rank {
			seen[it[strings.Index(it, ":")+1:]]++
		}
	}
	// source a: 5+8+11+14 = 38 records, 2 epochs, every one twice
	for i := 0; i < 4; i++ {
		for j := 0; j < 5+i*3; j++ {
			if n := seen[fmt.Sprintf("a%d-%d", i, j)]; n != 2 {
				t.Fatalf("a%d-%d given %d times in 2 epochs", i, j, n)
			}
		}
	}
	if total != 76+76/3 {
		t.Fatalf("%d records: 76 of a and, at 3 to 1, %d of b", total, 76/3)
	}
	known := spec
	known.Sources = append([]SourceSpec(nil), spec.Sources...)
	for s := range known.Sources {
		known.Sources[s].Records = map[string]int64{}
		for i, f := range known.Sources[s].Files {
			if s == 0 {
				key, _ := RecordsKey(f.SHA256, known.Sources[s].Format, known.Sources[s].Field, f.Name)
				known.Sources[s].Records[key] = int64(5 + i*3)
			} else {
				key, _ := RecordsKey(f.SHA256, known.Sources[s].Format, known.Sources[s].Field, f.Name)
				known.Sources[s].Records[key] = 4
			}
		}
	}
	for _, at := range [][2]int64{{0, 1}, {3, 3}, {6, 9}, {20, 13}, {30, 29}} {
		positions := []Position{{Rank: 0, Record: at[0]}, {Rank: 1, Record: at[1]}}
		for _, s := range []Spec{spec, known} {
			got := collect(t, s, dir, 2, positions)
			for r := range got {
				if !reflect.DeepEqual(got[r], full[r][at[r]:]) {
					t.Fatalf("rank %d from record %d: %v\nfrom the start: %v", r, at[r], got[r], full[r][at[r]:])
				}
			}
		}
	}
}

// The test binary is a stage when KAIRO_LOADER_TEST_STAGE is set: each
// record's outputs are it in upper case, twice if it ends in 7, none if it
// ends in 3; after KAIRO_LOADER_TEST_CRASH_AFTER records it fails once
// (the file KAIRO_LOADER_TEST_CRASHED marks that it did).
func TestMain(m *testing.M) {
	if os.Getenv("KAIRO_LOADER_TEST_STAGE") != "" {
		os.Exit(testStage())
	}
	os.Exit(m.Run())
}

func testStage() int {
	crashAfter := -1
	fmt.Sscan(os.Getenv("KAIRO_LOADER_TEST_CRASH_AFTER"), &crashAfter)
	marker := os.Getenv("KAIRO_LOADER_TEST_CRASHED")
	if _, err := os.Stat(marker); marker == "" || err == nil {
		crashAfter = -1
	}
	in := bufioReader(os.Stdin)
	out := bufioWriter(os.Stdout)
	defer out.Flush()
	for n := 0; ; n++ {
		if n == crashAfter {
			os.WriteFile(marker, nil, 0o644)
			out.Flush()
			return 3
		}
		var size [4]byte
		if _, err := io.ReadFull(in, size[:]); err != nil {
			return 0
		}
		rec := make([]byte, binary.LittleEndian.Uint32(size[:]))
		io.ReadFull(in, rec)
		emit := func(b []byte) {
			binary.LittleEndian.PutUint32(size[:], uint32(len(b)))
			out.Write(size[:])
			out.Write(b)
		}
		up := bytes.ToUpper(rec)
		switch {
		case bytes.HasSuffix(rec, []byte("3")):
		case bytes.HasSuffix(rec, []byte("7")):
			emit(up)
			emit(up)
		default:
			emit(up)
		}
		binary.LittleEndian.PutUint32(size[:], endOfOutputs)
		out.Write(size[:])
		if n%5 == 4 {
			out.Flush()
		}
	}
}

// Records go through the stages in order, however many workers, outputs
// none or many; a stage that fails is started again and given what it had
// not answered; a position mid-record starts at that output.
func TestStagesKeepTheOrder(t *testing.T) {
	spec, dir := testData(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	plain := collect(t, spec, dir, 1, nil)[0]
	stage := Stage{Argv: []string{exe}, Env: []string{"KAIRO_LOADER_TEST_STAGE=1"}, Workers: 3}
	staged := spec
	staged.Stages = []Stage{stage}
	got := collect(t, staged, dir, 1, nil)[0]
	var want []string
	for _, it := range plain {
		rec := it[strings.Index(it, ":")+1:]
		up := strings.ToUpper(rec)
		switch {
		case strings.HasSuffix(rec, "3"):
		case strings.HasSuffix(rec, "7"):
			want = append(want, up, up)
		default:
			want = append(want, up)
		}
	}
	items := func(list []string) []string {
		var out []string
		for _, it := range list {
			out = append(out, it[strings.Index(it, ":")+1:])
		}
		return out
	}
	if !reflect.DeepEqual(items(got), want) {
		t.Fatalf("staged %v\nwant %v", items(got), want)
	}
	// a crash midway changes nothing
	crashing := staged
	crashing.Stages = []Stage{{Argv: stage.Argv, Workers: 2, Env: []string{"KAIRO_LOADER_TEST_STAGE=1", "KAIRO_LOADER_TEST_CRASH_AFTER=6",
		"KAIRO_LOADER_TEST_CRASHED=" + filepath.Join(t.TempDir(), "crashed")}}}
	if again := collect(t, crashing, dir, 1, nil)[0]; !reflect.DeepEqual(again, got) {
		t.Fatalf("after a crash %v\nwant %v", items(again), items(got))
	}
	// from the second output of a record that gives two
	for i, it := range got {
		if strings.HasPrefix(it[strings.Index(it, ".")+1:], "1:") {
			var k int64
			fmt.Sscan(it[:strings.Index(it, ".")], &k)
			// the position after it: record k's first output taken
			from := collect(t, staged, dir, 1, []Position{{Record: k, Sub: 1}})[0]
			if !reflect.DeepEqual(from, got[i+1:]) {
				t.Fatalf("from record %d sub 1: %v\nwant %v", k, items(from), items(got[i+1:]))
			}
			return
		}
	}
	t.Fatal("no record gave two outputs")
}

// A taker that comes back where it stands goes on from there, wherever the
// loader had got to; one coming back behind makes the stream again.
func TestATakerComesBackWhereItStands(t *testing.T) {
	spec, dir := testData(t)
	full := collect(t, spec, dir, 1, nil)[0]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go (&Server{Loader: &Loader{Spec: spec, Fetch: &Fetcher{Origin: DirOrigin{dir}}}, World: 1}).Serve(ctx, ln)
	take := func(at *Resume, n int) ([]string, Resume) {
		c, err := Dial(ctx, "tcp", ln.Addr().String(), 0, 1, at)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.Window = 4
		var got []string
		for len(got) < n {
			item, err := c.Next()
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, fmt.Sprintf("%d.%d:%s", c.Position.Record, c.Position.Sub, item))
		}
		return got, *c.Resume()
	}
	first, at := take(nil, 10)
	second, at2 := take(&at, 10)
	if !reflect.DeepEqual(append(first, second...), full[:20]) {
		t.Fatalf("taken in two %v %v\nwant %v", first, second, full[:20])
	}
	back := Resume{Session: at.Session, Position: Position{Record: 5}}
	third, _ := take(&back, 5)
	if !reflect.DeepEqual(third, full[5:10]) || at2.Position.Record != 20 {
		t.Fatalf("back at 5: %v want %v", third, full[5:10])
	}
	// a first hello does not say where it starts
	if _, err := Dial(ctx, "tcp", ln.Addr().String(), 0, 1, &Resume{Position: Position{Record: 5}}); !errors.Is(err, ErrStreamFailed) {
		t.Fatalf("a position without the session: %v", err)
	}
}

// goldenPermutation is permutation(10, 42) as first made: fixed for good.
var goldenPermutation = []int{8, 3, 6, 5, 4, 0, 9, 2, 1, 7}

func bufioReader(r io.Reader) *bufio.Reader { return bufio.NewReaderSize(r, 1<<16) }
func bufioWriter(w io.Writer) *bufio.Writer { return bufio.NewWriterSize(w, 1<<16) }

// A file never read before is read from its start, its identity made as
// it is read (the same as Identify makes) and heard once it was read to its
// end; one whose published sha256 is not what was read fails at its end.
func TestAFileIsIdentifiedTheFirstTimeItIsRead(t *testing.T) {
	spec, dir := testData(t)
	want := map[string]File{}
	for s := range spec.Sources {
		for i, f := range spec.Sources[s].Files {
			id, _ := Identify(bytes.NewReader(mustRead(t, filepath.Join(dir, filepath.FromSlash(f.Name)))), f.Name, DefaultChunkBytes)
			want[f.Name] = id
			spec.Sources[s].Files[i] = File{Name: f.Name}
		}
	}
	full := collect(t, spec, dir, 1, nil)
	var mu sync.Mutex
	got := map[string]File{}
	l := &Loader{Spec: spec, Fetch: &Fetcher{Origin: DirOrigin{dir}}, Identified: func(_ int, f File) {
		mu.Lock()
		got[f.Name] = f
		mu.Unlock()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go (&Server{Loader: l, World: 1}).Serve(ctx, ln)
	c, err := Dial(ctx, "tcp", ln.Addr().String(), 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	var items []string
	for {
		item, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, fmt.Sprintf("%d.%d:%s", c.Position.Record, c.Position.Sub, item))
	}
	c.Close()
	if !reflect.DeepEqual(items, full[0]) {
		t.Fatal("read unidentified, another order")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("identified %d of %d files", len(got), len(want))
	}
	for name, w := range want {
		if g := got[name]; g.SHA256 != w.SHA256 || g.Size != w.Size || !reflect.DeepEqual(g.Chunks, w.Chunks) {
			t.Fatalf("%s identified as %+v, want %+v", name, g, w)
		}
	}
	// a published sha256 that is not what is there
	x := &Fetcher{Origin: DirOrigin{dir}}
	r, err := x.Open(context.Background(), File{Name: "b-0.txt", SHA256: strings.Repeat("0", 64)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.ReadAll(r); !errors.Is(err, ErrChanged) {
		t.Fatalf("a published sha256 not matching: %v", err)
	}
}

func mustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// countingOrigin counts the files opened.
type countingOrigin struct {
	DirOrigin
	mu     sync.Mutex
	opened map[string]int
}

func (o *countingOrigin) ReadFrom(ctx context.Context, f File, offset int64) (io.ReadCloser, error) {
	o.mu.Lock()
	o.opened[f.Name]++
	o.mu.Unlock()
	return o.DirOrigin.ReadFrom(ctx, f, offset)
}

// A stream of one epoch ends at its last record, opening nothing of a next
// epoch; a rank coming back after the end is told the end again.
func TestAStreamEndsAtItsLastEpochAndSaysSoAgain(t *testing.T) {
	spec, dir := testData(t)
	spec.Sources = spec.Sources[:1]
	spec.Epochs = 1
	origin := &countingOrigin{DirOrigin: DirOrigin{dir}, opened: map[string]int{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go (&Server{Loader: &Loader{Spec: spec, Fetch: &Fetcher{Origin: origin}}, World: 1}).Serve(ctx, ln)
	c, err := Dial(ctx, "tcp", ln.Addr().String(), 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for {
		if _, err := c.Next(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		n++
	}
	at := c.Resume()
	c.Close()
	if n != 38 {
		t.Fatalf("%d records of one epoch, want 38", n)
	}
	origin.mu.Lock()
	for name, times := range origin.opened {
		if times != 1 {
			t.Fatalf("%s opened %d times: %v", name, times, origin.opened)
		}
	}
	origin.mu.Unlock()
	again, err := Dial(ctx, "tcp", ln.Addr().String(), 0, 1, at)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if _, err := again.Next(); err != io.EOF {
		t.Fatalf("after the end, again: %v", err)
	}
}

// An identified file whose length changed (longer or shorter) is changed;
// a JSON null is not a string field.
func TestALengthChangeIsAChangeAndNullIsNoString(t *testing.T) {
	dir := t.TempDir()
	data := bytes.Repeat([]byte("abc"), 100)
	p := filepath.Join(dir, "f")
	os.WriteFile(p, data, 0o644)
	f, _ := Identify(bytes.NewReader(data), "f", 128)
	for _, changed := range [][]byte{append(append([]byte(nil), data...), 'x'), data[:250]} {
		os.WriteFile(p, changed, 0o644)
		r, err := (&Fetcher{Origin: DirOrigin{dir}, Retry: time.Millisecond}).Open(context.Background(), f, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.ReadAll(r)
		r.Close()
		if !errors.Is(err, ErrChanged) {
			t.Fatalf("%d bytes for %d identified: %v", len(changed), len(data), err)
		}
	}
	fm, _ := parseFormat("jsonl", "text")
	rr, closeFn, _ := newRecordReader(bytes.NewReader([]byte(`{"text":null}`+"\n")), "x.jsonl", fm)
	defer closeFn()
	if _, err := rr.next(); err == nil {
		t.Fatal("a null field taken as a string")
	}
}

// A stream started from a checkpoint does not go back behind it, nor take
// a position past its end for one.
func TestPositionsBehindTheStartOrPastTheEndAreRefused(t *testing.T) {
	spec, dir := testData(t)
	spec.Sources = spec.Sources[:1]
	spec.Epochs = 1 // 38 records
	serve := func(positions []Position) (context.CancelFunc, string) {
		ctx, cancel := context.WithCancel(context.Background())
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		go (&Server{Loader: &Loader{Spec: spec, Fetch: &Fetcher{Origin: DirOrigin{dir}}}, Positions: positions, World: 1}).Serve(ctx, ln)
		return cancel, ln.Addr().String()
	}
	cancel, addr := serve([]Position{{Record: 40}})
	c, err := Dial(context.Background(), "tcp", addr, 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Next(); !errors.Is(err, ErrStreamFailed) {
		t.Fatalf("past the end: %v", err)
	}
	c.Close()
	cancel()
	cancel, addr = serve([]Position{{Record: 10}})
	defer cancel()
	c, err = Dial(context.Background(), "tcp", addr, 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Position.Record != 10 {
		t.Fatalf("started at %+v", c.Position)
	}
	if _, err := c.Next(); err != nil {
		t.Fatal(err)
	}
	session := c.Session
	c.Close()
	if _, err := Dial(context.Background(), "tcp", addr, 0, 1, &Resume{Session: session, Position: Position{Record: 5}}); !errors.Is(err, ErrStreamFailed) {
		t.Fatalf("behind the start: %v", err)
	}
	// past the end, a primary source of known counts: refused at its seek
	cancel2, addr2 := serve([]Position{{Record: 39}})
	defer cancel2()
	c, err = Dial(context.Background(), "tcp", addr2, 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Next(); !errors.Is(err, ErrStreamFailed) {
		t.Fatalf("past the end at 39 of 38: %v", err)
	}
}

// A rank taken before comes back only with its position; a rank away does
// not hold the others up: its items are made again when it comes back.
func TestARankAwayHoldsNoOneUp(t *testing.T) {
	spec, dir := testData(t)
	spec.Sources = spec.Sources[:1]
	spec.Epochs = 300 // 38 records each: 5700 per rank, past a rank's queue
	want := collect(t, spec, dir, 2, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go (&Server{Loader: &Loader{Spec: spec, Fetch: &Fetcher{Origin: DirOrigin{dir}}}, World: 2}).Serve(ctx, ln)
	takeAll := func(c *Client) []string {
		var got []string
		for {
			item, err := c.Next()
			if err == io.EOF {
				return got
			}
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, fmt.Sprintf("%d.%d:%s", c.Position.Record, c.Position.Sub, item))
		}
	}
	c0, err := Dial(ctx, "tcp", ln.Addr().String(), 0, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := c0.Next()
	if err != nil {
		t.Fatal(err)
	}
	resume := c0.Resume()
	c0.Close()
	// taken before: not without its position
	if _, err := Dial(ctx, "tcp", ln.Addr().String(), 0, 2, nil); !errors.Is(err, ErrStreamFailed) {
		t.Fatalf("rank 0 again without its position: %v", err)
	}
	c0, err = Dial(ctx, "tcp", ln.Addr().String(), 0, 2, resume)
	if err != nil {
		t.Fatal(err)
	}
	got0 := append([]string{fmt.Sprintf("%d.%d:%s", resume.Position.Record, resume.Position.Sub, first)}, takeAll(c0)...) // rank 1 never came
	c0.Close()
	c1, err := Dial(ctx, "tcp", ln.Addr().String(), 1, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	got1 := takeAll(c1)
	c1.Close()
	if !reflect.DeepEqual(got0, want[0]) || !reflect.DeepEqual(got1, want[1]) {
		t.Fatalf("rank 0 %d items (want %d), rank 1 %d (want %d)", len(got0), len(want[0]), len(got1), len(want[1]))
	}
}
