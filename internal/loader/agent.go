package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// The node's raw cache (executor/rawcache.go): a loader in a container is
// handed the directory a volatile store keeps raw files in, read-only, with
// the agent's socket in it (AgentSocket). It asks the agent for each file
// before reading it (POST /ensure, the file as the loader knows it): the
// agent fetches it from upstream into the directory once, checking it
// against its identity (or against what was published of it, making its
// identity), records the identity with the daemon and answers with the file
// as identified and its name in the directory; the loader reads it from
// there, checking every chunk again. A file larger than the store's room is
// not kept: the agent reads it once only to identify it, and the loader
// reads it from upstream, checked (Fallback). No byte reaches a stream's
// reader before it is checked against an identity.

// AgentSocket is the agent's socket in a raw directory.
const AgentSocket = ".sock"

// EnsureAnswer is the agent's answer to /ensure.
type EnsureAnswer struct {
	File     File   `json:"file"`
	Name     string `json:"name,omitempty"`
	Fallback bool   `json:"fallback,omitempty"`
	Error    string `json:"error,omitempty"`
	// Changed: the file is not what it was identified or published as.
	Changed bool `json:"changed,omitempty"`
}

// Agent asks the agent of its node for raw files, over its socket in Dir.
type Agent struct {
	Dir      string
	Upstream Origin
	Problem  func(f File, err error)
	client   *http.Client
}

// NewAgent is the agent whose raw directory is dir.
func NewAgent(dir string, upstream Origin) *Agent {
	sock := filepath.Join(dir, AgentSocket)
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}
	return &Agent{Dir: dir, Upstream: upstream, client: &http.Client{Transport: transport}}
}

// ErrRefused: the node's agent will not make a file ready for this
// attempt (no file of its streams, a request it does not understand).
var ErrRefused = errors.New("raw file refused")

// Resolve has the agent make f ready: the file as identified and where to
// read it (the attempt's raw view, or upstream when it is not kept). The
// agent being away is waited out; a file not what it was identified or
// declared as is ErrChanged; one the agent refuses is ErrRefused.
func (a *Agent) Resolve(ctx context.Context, f File) (File, Origin, error) {
	pause := time.Second
	for {
		answer, err := a.ensure(ctx, f)
		if err == nil {
			if answer.Fallback {
				return answer.File, a.Upstream, nil
			}
			return answer.File, cachedOrigin{path: filepath.Join(a.Dir, answer.Name)}, nil
		}
		if errors.Is(err, ErrChanged) || errors.Is(err, ErrRefused) || ctx.Err() != nil {
			return File{}, nil, err
		}
		if a.Problem != nil {
			a.Problem(f, err)
		}
		select {
		case <-ctx.Done():
			return File{}, nil, ctx.Err()
		case <-time.After(pause):
		}
		pause = min(pause*2, time.Minute)
	}
}

func (a *Agent) ensure(ctx context.Context, f File) (EnsureAnswer, error) {
	body, err := json.Marshal(map[string]string{"url": f.URL})
	if err != nil {
		return EnsureAnswer{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://agent/ensure", bytes.NewReader(body))
	if err != nil {
		return EnsureAnswer{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return EnsureAnswer{}, fmt.Errorf("the node's agent: %w", err)
	}
	defer resp.Body.Close()
	var answer EnsureAnswer
	if err := json.NewDecoder(io.LimitReader(resp.Body, 256<<20)).Decode(&answer); err != nil {
		if resp.StatusCode/100 == 4 {
			return EnsureAnswer{}, fmt.Errorf("%w: the node's agent: %s", ErrRefused, resp.Status)
		}
		return EnsureAnswer{}, fmt.Errorf("the node's agent: %s: %w", resp.Status, err)
	}
	switch {
	case answer.Changed:
		return EnsureAnswer{}, fmt.Errorf("%w: %s", ErrChanged, answer.Error)
	case resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusBadRequest:
		return EnsureAnswer{}, fmt.Errorf("%w: %s: %s", ErrRefused, f.URL, answer.Error)
	case resp.StatusCode != http.StatusOK || answer.Error != "":
		return EnsureAnswer{}, fmt.Errorf("the node's agent: %s: %s", resp.Status, answer.Error)
	case !answer.File.Identified() || answer.File.URL != f.URL || !answer.Fallback && answer.Name != answer.File.SHA256:
		return EnsureAnswer{}, fmt.Errorf("%w: the node's agent answered %s with %+v", ErrRefused, f.URL, answer)
	}
	// the answer is the file this loader was told to read, as published
	g := answer.File
	pinned := g
	pinned.PublishedSHA256, pinned.PublishedSize = f.PublishedSHA256, f.PublishedSize
	if why := pinned.AsPublished(); why != "" {
		return EnsureAnswer{}, fmt.Errorf("%w: %s", ErrChanged, why)
	}
	if f.Identified() && (f.SHA256 != g.SHA256 || f.Size != g.Size || f.ChunkBytes != g.ChunkBytes || len(f.Chunks) != len(g.Chunks)) || !f.Identified() && f.SHA256 != "" && f.SHA256 != g.SHA256 {
		return EnsureAnswer{}, fmt.Errorf("%w: %s is %s (%d bytes), declared %s (%d bytes)", ErrChanged, f.URL, g.SHA256, g.Size, f.SHA256, f.Size)
	}
	if f.Identified() {
		for i := range f.Chunks {
			if f.Chunks[i] != g.Chunks[i] {
				return EnsureAnswer{}, fmt.Errorf("%w: %s chunk %d is not as declared", ErrChanged, f.URL, i)
			}
		}
	}
	return answer, nil
}

// cachedOrigin reads a file kept in the raw directory.
type cachedOrigin struct{ path string }

func (c cachedOrigin) ReadFrom(_ context.Context, _ File, offset int64) (io.ReadCloser, error) {
	file, err := os.Open(c.path)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

func (c cachedOrigin) String() string { return c.path }
