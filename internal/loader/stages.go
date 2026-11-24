package loader

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

// Stage is a project process records go through, Workers of it (default 1)
// side by side. Each reads records on its stdin, a 4-byte little-endian
// length then the bytes, and writes for each, in order, its outputs the same
// way (none or more) and then the 4 bytes ff ff ff ff. A stage gives the same
// outputs for the same record: a stage that crashed is started again and
// given again what it had not answered.
type Stage struct {
	Argv    []string `json:"argv"`
	Dir     string   `json:"dir,omitempty"`
	Env     []string `json:"env,omitempty"`
	Workers int      `json:"workers,omitempty"`
}

// endOfOutputs ends a record's outputs on a stage's stdout.
const endOfOutputs = 0xffffffff

// stageRestarts: crashes in a row, answering nothing in between, after
// which a stage's failure is the stream's.
const stageRestarts = 3

// group is records that go through the stages together: the outputs of one
// output of the order (its g), in order.
type group struct {
	g    int64
	recs [][]byte
}

// runStage puts each group of in through st: group i to worker i mod
// Workers, their answers given back in the order of in. The output closes
// after in closes (or on the error sent to errs).
func runStage(ctx context.Context, st Stage, in <-chan group, errs chan<- error, stderr io.Writer) <-chan group {
	workers := st.Workers
	if workers < 1 {
		workers = 1
	}
	out := make(chan group, workers*4)
	ins := make([]chan group, workers)
	outs := make([]chan group, workers)
	for i := range ins {
		ins[i] = make(chan group, 8)
		outs[i] = make(chan group, 8)
		w := &stageWorker{st: st, in: ins[i], out: outs[i], stderr: stderr}
		go func() {
			if err := w.run(ctx); err != nil {
				select {
				case errs <- fmt.Errorf("stage %v: %w", st.Argv, err):
				default:
				}
			}
			close(w.out)
		}()
	}
	// dispatch in order, round robin
	go func() {
		defer func() {
			for _, c := range ins {
				close(c)
			}
		}()
		i := 0
		for g := range in {
			select {
			case ins[i%workers] <- g:
			case <-ctx.Done():
				return
			}
			i++
		}
	}()
	// merge in the same order
	go func() {
		defer close(out)
		for i := 0; ; i++ {
			var g group
			var ok bool
			select {
			case g, ok = <-outs[i%workers]:
			case <-ctx.Done():
				return
			}
			if !ok {
				return
			}
			select {
			case out <- g:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// stageWorker runs one process of a stage, started again when it fails.
type stageWorker struct {
	st     Stage
	in     <-chan group
	out    chan<- group
	stderr io.Writer

	mu      sync.Mutex
	pending []group // written, not answered yet
	closed  bool    // in closed: nothing more to write
}

func (w *stageWorker) run(ctx context.Context) error {
	crashes := 0
	for {
		answered, err := w.runProcess(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if answered > 0 {
			crashes = 0
		}
		crashes++
		if crashes > stageRestarts {
			return fmt.Errorf("failed %d times in a row: %w", crashes, err)
		}
		fmt.Fprintf(w.stderr, "kairo loader: stage %v failed (%v); started again with the %d records it had not answered\n", w.st.Argv, err, w.pendingCount())
		select {
		case <-time.After(time.Duration(crashes) * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *stageWorker) pendingCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, g := range w.pending {
		n += len(g.recs)
	}
	return n
}

// runProcess runs the process until in is done and answered (nil) or it
// fails; answered counts the groups it answered.
func (w *stageWorker) runProcess(ctx context.Context) (answered int, err error) {
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(pctx, w.st.Argv[0], w.st.Argv[1:]...)
	cmd.Dir = w.st.Dir
	if len(w.st.Env) > 0 {
		cmd.Env = append(os.Environ(), w.st.Env...)
	}
	cmd.Stderr = w.stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return 0, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, err
	}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	// sent carries each group written, in order, to the reader
	sent := make(chan group, 64)
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- w.write(pctx, stdin, sent)
	}()
	r := bufio.NewReaderSize(stdout, 1<<20)
	for g := range sent {
		outs, err := readOutputs(r, len(g.recs))
		if err != nil {
			cancel()
			cmd.Wait()
			if werr := <-writeErr; werr != nil && !errors.Is(werr, context.Canceled) && err == io.EOF {
				err = werr
			}
			return answered, err
		}
		w.mu.Lock()
		w.pending = w.pending[1:]
		w.mu.Unlock()
		select {
		case w.out <- group{g: g.g, recs: outs}:
		case <-ctx.Done():
			cancel()
			cmd.Wait()
			return answered, ctx.Err()
		}
		answered++
	}
	if err := <-writeErr; err != nil {
		cmd.Wait()
		return answered, err
	}
	stdin.Close()
	if err := cmd.Wait(); err != nil {
		return answered, err
	}
	return answered, nil
}

// write gives the process what it had not answered, then the groups of in,
// telling the reader each (sent, closed at the end).
func (w *stageWorker) write(ctx context.Context, stdin io.WriteCloser, sent chan<- group) error {
	defer close(sent)
	bw := bufio.NewWriterSize(stdin, 1<<20)
	put := func(g group) error {
		for _, rec := range g.recs {
			var size [4]byte
			binary.LittleEndian.PutUint32(size[:], uint32(len(rec)))
			if _, err := bw.Write(size[:]); err != nil {
				return err
			}
			if _, err := bw.Write(rec); err != nil {
				return err
			}
		}
		if err := bw.Flush(); err != nil {
			return err
		}
		select {
		case sent <- g:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}
	w.mu.Lock()
	again := append([]group(nil), w.pending...)
	w.mu.Unlock()
	for _, g := range again {
		if err := put(g); err != nil {
			return err
		}
	}
	for {
		w.mu.Lock()
		closed := w.closed
		w.mu.Unlock()
		if closed {
			return stdin.Close()
		}
		var g group
		var ok bool
		select {
		case g, ok = <-w.in:
		case <-ctx.Done():
			return ctx.Err()
		}
		if !ok {
			w.mu.Lock()
			w.closed = true
			w.mu.Unlock()
			return stdin.Close()
		}
		w.mu.Lock()
		w.pending = append(w.pending, g)
		w.mu.Unlock()
		if err := put(g); err != nil {
			return err
		}
	}
}

// readOutputs reads the outputs of n records.
func readOutputs(r *bufio.Reader, n int) ([][]byte, error) {
	var outs [][]byte
	for done := 0; done < n; {
		var size [4]byte
		if _, err := io.ReadFull(r, size[:]); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, fmt.Errorf("output cut short")
			}
			return nil, err
		}
		v := binary.LittleEndian.Uint32(size[:])
		if v == endOfOutputs {
			done++
			continue
		}
		if v > maxRecordBytes {
			return nil, fmt.Errorf("an output of %d bytes, more than %d", v, maxRecordBytes)
		}
		rec := make([]byte, v)
		if _, err := io.ReadFull(r, rec); err != nil {
			return nil, fmt.Errorf("output cut short")
		}
		outs = append(outs, rec)
	}
	return outs, nil
}
