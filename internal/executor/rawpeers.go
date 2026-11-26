package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"kairo/internal/loader"
	"kairo/internal/store"
)

// Raw files over the LAN (issue #43; store/rawpeers.go). Before a raw file
// identified before and kept nowhere on this node is read from upstream, the
// daemon is asked which other nodes keep it (store.RawSources), each with a
// token for that one read; the file is read from their agents' transfer
// endpoints (GET /raw/<sha256>, PackServer.serveRaw) in turn, checked
// chunk by chunk against its identity like upstream's bytes, and upstream is
// read only when none gave it whole and right.

// rawPeerPrefix is the transfer endpoint's path of a kept raw file.
const rawPeerPrefix = "/raw/"

// rawSources are the other nodes keeping url's file (none when the daemon
// cannot say: upstream is read then).
func (c *rawCache) rawSources(ctx context.Context, url string) []store.RawSource {
	if c.w.PackClient == nil {
		return nil
	}
	sources, err := c.w.Store.RawSources(ctx, c.w.ID, url)
	if err != nil {
		c.w.Logger.Info("other nodes keeping a raw file not asked", "url", url, "error", err)
		return nil
	}
	return sources
}

// openAtPeer reads raw file sha256 from another node's agent; the request is
// given up once nothing has arrived for stallTimeout.
func (c *rawCache) openAtPeer(ctx context.Context, src store.RawSource, sha256 string) (io.ReadCloser, error) {
	rctx, cancel := context.WithCancel(ctx)
	stall := time.AfterFunc(stallTimeout, cancel)
	giveUp := func() {
		stall.Stop()
		cancel()
	}
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, "https://"+src.Addr+rawPeerPrefix+sha256, nil)
	if err != nil {
		giveUp()
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+src.Token)
	resp, err := c.w.PackClient.Do(req)
	if err != nil {
		giveUp()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		text, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		giveUp()
		return nil, fmt.Errorf("raw file from node %s: %s: %s", src.NodeID, resp.Status, strings.TrimSpace(string(text)))
	}
	return &watchedBody{ReadCloser: resp.Body, stall: stall, cancel: cancel}, nil
}

// fetchInto reads a file into a partial of the raw cache: from another node
// keeping it when one does (a file identified before), else from upstream.
// A node whose bytes are not the file's is passed over; running out of the
// room held for it is not (upstream would not fit either).
func (c *rawCache) fetchInto(ctx context.Context, declared, canonical loader.File, found bool, reserved int64) (loader.File, string, error) {
	if found {
		for _, src := range c.rawSources(ctx, declared.URL) {
			identity, partial, err := c.readInto(ctx, declared, canonical, found, reserved, func(ctx context.Context, _ loader.File) (io.ReadCloser, error) {
				return c.openAtPeer(ctx, src, canonical.SHA256)
			})
			if err == nil {
				c.w.Logger.Info("raw file read from another node", "url", declared.URL, "node_id", src.NodeID, "bytes", identity.Size)
				return identity, partial, nil
			}
			if errors.Is(err, syscall.ENOSPC) || errors.Is(err, errPastReserved) || ctx.Err() != nil {
				return loader.File{}, "", err
			}
			c.w.Logger.Warn("raw file not read from another node", "url", declared.URL, "node_id", src.NodeID, "error", err)
		}
	}
	return c.readInto(ctx, declared, canonical, found, reserved, func(ctx context.Context, read loader.File) (io.ReadCloser, error) {
		return c.fetch.Open(ctx, read, 0)
	})
}

// serveRaw hands a raw file this node keeps to another node's raw cache,
// with the token the daemon issued for that read (store.AuthorizeRawRead,
// which names the store keeping it); anything else is 404 or 403.
func (p *PackServer) serveRaw(w http.ResponseWriter, r *http.Request, sha256 string) {
	if !sha256Name.MatchString(sha256) {
		http.NotFound(w, r)
		return
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	read, err := p.Store.AuthorizeRawRead(r.Context(), p.NodeID, sha256, token)
	if err != nil {
		p.warn(sha256, "raw read refused", "remote", r.RemoteAddr, "error", err)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var dir string
	for _, e := range p.Executors {
		for _, s := range e.storeWork().servedStores() {
			if _, isLocal := s.fs.(localStoreFS); isLocal && s.usable && s.decl.ID == read.StoreID && s.decl.Root == read.Root {
				dir = filepath.Join(s.decl.Root, rawDirName)
			}
		}
	}
	if dir == "" {
		http.Error(w, "the raw file's store is not served here", http.StatusNotFound)
		return
	}
	f, err := os.Open(filepath.Join(dir, sha256))
	if err != nil {
		p.warn(sha256, "raw file not opened", "error", err)
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, sha256, fi.ModTime(), f)
}
