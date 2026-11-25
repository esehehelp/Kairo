package loader

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

// Client takes one rank's records from a loader (the protocol in
// server.go), keeping where it stands: Position is the next record and
// output it takes.
type Client struct {
	conn     net.Conn
	r        *bufio.Reader
	w        *bufio.Writer
	Position Position
	World    int
	Session  string
	// Window is how many items it asks for ahead (default 256).
	Window      int
	outstanding int
}

// Resume is where a taker coming back stands, and the session it was given.
type Resume struct {
	Session  string
	Position Position
}

// Resume is where c stands, to come back with.
func (c *Client) Resume() *Resume { return &Resume{Session: c.Session, Position: c.Position} }

// Dial joins the loader at addr (network "unix" or "tcp") as rank of
// world; at, when not nil, is where the taker is (coming back).
func Dial(ctx context.Context, network, addr string, rank, world int, at *Resume) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	c := &Client{conn: conn, r: bufio.NewReaderSize(conn, 1<<20), w: bufio.NewWriter(conn), Window: 256}
	hello := Hello{Version: ProtocolVersion, Rank: rank, World: world}
	if at != nil {
		p := at.Position
		hello.Position, hello.Session = &p, at.Session
	}
	if err := writeJSONFrame(c.w, frameHello, hello); err != nil {
		conn.Close()
		return nil, err
	}
	kind, payload, err := readFrame(c.r)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if kind == frameError {
		conn.Close()
		return nil, fmt.Errorf("%w: %s", ErrStreamFailed, payload)
	}
	var ready Ready
	if kind != frameReady || json.Unmarshal(payload, &ready) != nil {
		conn.Close()
		return nil, fmt.Errorf("the loader answered a hello with a frame of type %d", kind)
	}
	c.Position, c.World, c.Session = ready.Position, ready.World, ready.Session
	return c, nil
}

// Next is the rank's next item; io.EOF once the stream ended.
func (c *Client) Next() ([]byte, error) {
	for {
		if c.outstanding < c.Window/2 {
			var b [4]byte
			n := c.Window - c.outstanding
			binary.LittleEndian.PutUint32(b[:], uint32(n))
			if err := writeFrame(c.w, frameCredit, b[:]); err != nil {
				return nil, err
			}
			if err := c.w.Flush(); err != nil {
				return nil, err
			}
			c.outstanding += n
		}
		kind, payload, err := readFrameAny(c.r)
		if err != nil {
			return nil, err
		}
		switch kind {
		case frameItem:
			if len(payload) < 13 {
				return nil, fmt.Errorf("an item frame of %d bytes", len(payload))
			}
			record := int64(binary.LittleEndian.Uint64(payload))
			sub := int64(binary.LittleEndian.Uint32(payload[8:]))
			if payload[12]&1 != 0 {
				c.Position = Position{Rank: c.Position.Rank, Record: record + 1}
			} else {
				c.Position = Position{Rank: c.Position.Rank, Record: record, Sub: sub + 1}
			}
			c.outstanding--
			return payload[13:], nil
		case frameSkip:
			if len(payload) != 8 {
				return nil, fmt.Errorf("a skip frame of %d bytes", len(payload))
			}
			c.Position = Position{Rank: c.Position.Rank, Record: int64(binary.LittleEndian.Uint64(payload)) + 1}
		case frameEnd:
			return nil, io.EOF
		case frameError:
			return nil, fmt.Errorf("%w: %s", ErrStreamFailed, payload)
		default:
			return nil, fmt.Errorf("a frame of type %d", kind)
		}
	}
}

func (c *Client) Close() error { return c.conn.Close() }

// readFrameAny reads a frame of any size the loader sends (an item may be
// up to maxRecordBytes).
func readFrameAny(r *bufio.Reader) (byte, []byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return 0, nil, err
	}
	n := binary.LittleEndian.Uint32(size[:])
	if n < 1 || n > maxRecordBytes+64 {
		return 0, nil, fmt.Errorf("a frame of %d bytes", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return body[0], body[1:], nil
}
