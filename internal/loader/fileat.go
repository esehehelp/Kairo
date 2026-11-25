package loader

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// fileAt reads an identified file at random through its fetcher: each
// chunk checked as it is read, a few kept (a columnar reader goes back to
// its index and between columns).
type fileAt struct {
	ctx   context.Context
	fetch *Fetcher
	f     File

	mu     sync.Mutex
	body   io.ReadCloser // positioned at chunk next
	next   int
	cached map[int][]byte
	order  []int // the cached chunks, oldest first
}

// fileAtChunks is how many chunks a fileAt keeps.
const fileAtChunks = 8

// openAt opens f for reading at random: it is resolved (where to read it,
// its identity) once; a file read at random has its identity first.
func (x *Fetcher) openAt(ctx context.Context, f File) (*fileAt, error) {
	via := x
	if x.Resolve != nil {
		resolved, origin, err := x.Resolve(ctx, f)
		if err != nil {
			return nil, err
		}
		v := *x
		v.Resolve, v.Origin = nil, origin
		v.reask = func(ctx context.Context) (File, Origin, error) { return x.Resolve(ctx, f) }
		via, f = &v, resolved
	}
	if !f.Identified() {
		return nil, fmt.Errorf("%s is read at random, so with its identity: read it through the node's raw cache, or identify it first (kairo loader identify)", f.Name)
	}
	if err := f.check(); err != nil {
		return nil, err
	}
	return &fileAt{ctx: ctx, fetch: via, f: f, cached: map[int][]byte{}}, nil
}

// Size is the file's size.
func (a *fileAt) Size() int64 { return a.f.Size }

func (a *fileAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("%s: read at %d", a.f.Name, off)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for n < len(p) {
		at := off + int64(n)
		if at >= a.f.Size {
			return n, io.EOF
		}
		chunk := int(at / a.f.ChunkBytes)
		data, err := a.chunk(chunk)
		if err != nil {
			return n, err
		}
		n += copy(p[n:], data[at-int64(chunk)*a.f.ChunkBytes:])
	}
	return n, nil
}

// chunk is chunk i's checked bytes.
func (a *fileAt) chunk(i int) ([]byte, error) {
	if data, ok := a.cached[i]; ok {
		return data, nil
	}
	if a.body == nil || a.next != i {
		if a.body != nil {
			a.body.Close()
		}
		body, err := a.fetch.Open(a.ctx, a.f, i)
		if err != nil {
			a.body = nil
			return nil, err
		}
		a.body, a.next = body, i
	}
	data := make([]byte, a.f.chunkSize(i))
	if _, err := io.ReadFull(a.body, data); err != nil {
		a.body.Close()
		a.body = nil
		return nil, err
	}
	a.next++
	a.cached[i] = data
	a.order = append(a.order, i)
	if len(a.order) > fileAtChunks {
		delete(a.cached, a.order[0])
		a.order = a.order[1:]
	}
	return data, nil
}

func (a *fileAt) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.body != nil {
		a.body.Close()
		a.body = nil
	}
	a.cached, a.order = nil, nil
	return nil
}
