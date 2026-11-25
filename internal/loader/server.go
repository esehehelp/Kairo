package loader

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// The protocol between a loader and the processes taking its records, over
// a local socket: frames of a 4-byte little-endian length (of what follows),
// a type byte and the payload.
//
//	1 hello  (taker)  JSON {"version":1,"rank":R,"world":W,"position":{"record":K,"sub":S}}
//	                  (position only when coming back: where the taker is)
//	2 ready  (loader) JSON {"position":{"rank":R,"record":K,"sub":S},"world":W}
//	3 credit (taker)  4 bytes: how many more items it takes
//	4 item   (loader) 8 bytes record, 4 bytes sub, 1 byte flags (1: the last of its
//	                  record), then the item's bytes
//	5 skip   (loader) 8 bytes record: it gave no item (takes no credit)
//	6 end    (loader) JSON {"reason":...}: no more items
//	7 error  (loader) JSON {"message":...,"hard":true}: the stream failed
//
// A taker's position after an item is (record+1, 0) if it was the last of
// its record, else (record, sub+1); after a skip (record+1, 0).
const (
	frameHello  = 1
	frameReady  = 2
	frameCredit = 3
	frameItem   = 4
	frameSkip   = 5
	frameEnd    = 6
	frameError  = 7
)

// maxRecord and maxSub bound a position (record*W+rank stays far from
// overflowing).
const (
	maxRecord = int64(1) << 48
	maxSub    = int64(1) << 31
)

// rankReleaseWait is how long a hello waits for the connection its rank
// had to be noticed gone (a handler notices within its take wait).
const rankReleaseWait = 5 * time.Second

// ProtocolVersion is the protocol's version a hello names.
const ProtocolVersion = 1

// maxFrame bounds a frame read from a taker.
const maxFrame = 1 << 20

// Hello is a taker's first frame.
type Hello struct {
	Version  int       `json:"version"`
	Rank     int       `json:"rank"`
	World    int       `json:"world"`
	Position *Position `json:"position,omitempty"`
}

// Ready answers a hello: where the rank starts.
type Ready struct {
	Position Position `json:"position"`
	World    int      `json:"world"`
}

// Server hands a loader's records to its takers, one connection per rank.
type Server struct {
	Loader *Loader
	// Positions are where each rank starts the first time (resuming a run),
	// nil for the start; World the ranks (0: the first hello's).
	Positions []Position
	World     int

	mu      sync.Mutex
	started bool
	taken   map[int]bool
	ctx     context.Context
}

// Serve takes connections on ln until ctx ends.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	s.mu.Lock()
	s.ctx, s.taken = ctx, map[int]bool{}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		ln.Close()
		s.Loader.Stop()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go s.handle(ctx, conn)
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriterSize(conn, 1<<20)
	kind, payload, err := readFrame(r)
	if err != nil {
		return
	}
	var hello Hello
	if kind != frameHello || json.Unmarshal(payload, &hello) != nil {
		writeJSONFrame(w, frameError, map[string]any{"message": "the first frame is a hello", "hard": true})
		return
	}
	rank, err := s.join(hello)
	if err != nil {
		writeJSONFrame(w, frameError, map[string]any{"message": err.Error(), "hard": true})
		return
	}
	defer s.leave(rank)
	start := s.Loader.Delivered()[rank]
	if err := writeJSONFrame(w, frameReady, Ready{Position: start, World: s.Loader.World()}); err != nil {
		return
	}
	credits := make(chan uint32, 16)
	go func() { // the taker's credits; closed when it goes
		defer close(credits)
		for {
			kind, payload, err := readFrame(r)
			if err != nil || kind != frameCredit || len(payload) != 4 {
				return
			}
			select {
			case credits <- binary.LittleEndian.Uint32(payload):
			case <-ctx.Done():
				return
			}
		}
	}()
	var credit uint64
	for {
		for credit == 0 {
			n, ok := <-credits
			if !ok {
				return
			}
			credit += uint64(n)
		}
		it, ok := s.Loader.take(rank, 200*time.Millisecond)
		if !ok {
			if err := w.Flush(); err != nil {
				return
			}
			select { // a taker that went is noticed while it waits
			case n, more := <-credits:
				if !more {
					return
				}
				credit += uint64(n)
			default:
			}
			continue
		}
		switch {
		case it.err != nil:
			writeJSONFrame(w, frameError, map[string]any{"message": it.err.Error(), "hard": true})
			return
		case it.end:
			writeJSONFrame(w, frameEnd, map[string]any{"reason": "the stream ended"})
			return
		case it.skip:
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], uint64(it.record))
			if err := writeFrame(w, frameSkip, b[:]); err != nil {
				return
			}
		default:
			head := make([]byte, 13, 13+len(it.data))
			binary.LittleEndian.PutUint64(head[0:], uint64(it.record))
			binary.LittleEndian.PutUint32(head[8:], uint32(it.sub))
			if it.last {
				head[12] = 1
			}
			if err := writeFrame(w, frameItem, append(head, it.data...)); err != nil {
				return
			}
			credit--
		}
		if credit == 0 || len(credits) > 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

// join admits a taker of rank hello.Rank: the first starts the stream;
// one coming back at a position other than where it was handed to makes the
// stream again from there.
func (s *Server) join(hello Hello) (int, error) {
	if hello.Version != ProtocolVersion {
		return 0, fmt.Errorf("protocol version %d, not %d", hello.Version, ProtocolVersion)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		world := s.World
		if world == 0 {
			world = hello.World
		}
		if err := s.Loader.Start(s.ctx, world, s.Positions); err != nil {
			return 0, err
		}
		s.started = true
	}
	world := s.Loader.World()
	if hello.World != world {
		return 0, fmt.Errorf("the stream is for %d ranks, not %d (a run keeps its ranks)", world, hello.World)
	}
	if hello.Rank < 0 || hello.Rank >= world {
		return 0, fmt.Errorf("no rank %d of %d", hello.Rank, world)
	}
	// a taker coming back right after its connection broke may find the old
	// one not noticed gone yet: it is given a while to go
	for deadline := time.Now().Add(rankReleaseWait); s.taken[hello.Rank]; {
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("%w: rank %d is being taken by another process", ErrRankTaken, hello.Rank)
		}
		s.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		s.mu.Lock()
	}
	s.taken[hello.Rank] = true
	if hello.Position != nil {
		p := *hello.Position
		p.Rank = hello.Rank
		if p.Record < 0 || p.Record > maxRecord || p.Sub < 0 || p.Sub > maxSub {
			delete(s.taken, hello.Rank)
			return 0, fmt.Errorf("position %+v is out of bounds", p)
		}
		s.Loader.resume(s.ctx, hello.Rank, p)
	}
	return hello.Rank, nil
}

func (s *Server) leave(rank int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.taken, rank)
}

func readFrame(r *bufio.Reader) (byte, []byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return 0, nil, err
	}
	n := binary.LittleEndian.Uint32(size[:])
	if n < 1 || n > maxFrame {
		return 0, nil, fmt.Errorf("a frame of %d bytes", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return body[0], body[1:], nil
}

func writeFrame(w *bufio.Writer, kind byte, payload []byte) error {
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(payload)+1))
	if _, err := w.Write(size[:]); err != nil {
		return err
	}
	if err := w.WriteByte(kind); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func writeJSONFrame(w *bufio.Writer, kind byte, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := writeFrame(w, kind, body); err != nil {
		return err
	}
	return w.Flush()
}

// ErrStreamFailed: the loader said the stream failed.
var ErrStreamFailed = errors.New("stream failed")
