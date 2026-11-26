package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"kairo/internal/loader"
)

// loaderServe: kairo loader serve --spec SPEC.json --listen
// unix:PATH|tcp:HOST:PORT [--world N] [--positions POSITIONS.json] [--session S] [--origin
// http|dir:DIR] [--status FILE].
func loaderServe(args []string) error {
	fs := newFlags("loader serve")
	specPath := fs.String("spec", "", "the stream: sources with their files identified, seed, block, epochs, stages (JSON)")
	listen := fs.String("listen", "", "where the ranks connect: unix:PATH or tcp:HOST:PORT")
	world := fs.Int("world", 0, "the ranks the stream is split between (0: the first rank's hello says)")
	positionsPath := fs.String("positions", "", "where each rank starts (JSON list of {rank, record, sub}); none: the start")
	session := fs.String("session", "", "the session takers come back with (the same across this loader's restarts; none: a random one)")
	rawRequests := fs.String("raw-requests", "", "with --raw: ask the node's agent through files in this directory, not its socket (a container in a VM)")
	state := fs.String("state", "", "a directory keeping which ranks were served across this loader's restarts")
	origin := fs.String("origin", "http", "where files are read: http (each file's URL) or dir:DIR (files by name under DIR)")
	statusPath := fs.String("status", "", "a file the stream's status is written to every 5 s (where each rank stands, what is made ahead, rates): the node's agent reports it")
	rawDir := fs.String("raw", "", "the node's raw cache for this attempt (its directory, with the agent's socket): every file made ready, checked and identified there before it is read; without it, files are read from --origin, one never read before identified as it is read")
	if _, err := parseFlags(fs, args); err != nil {
		return err
	}
	if *specPath == "" || *listen == "" {
		return usageError("loader serve needs --spec and --listen")
	}
	var spec loader.Spec
	if err := readJSONFile(*specPath, &spec); err != nil {
		return err
	}
	var positions []loader.Position
	if *positionsPath != "" {
		if err := readJSONFile(*positionsPath, &positions); err != nil {
			return err
		}
		// a run goes on with the ranks its positions are of
		if *world == 0 {
			*world = len(positions)
		}
	}
	var o loader.Origin = loader.HTTPOrigin{}
	if dir, ok := strings.CutPrefix(*origin, "dir:"); ok {
		o = loader.DirOrigin{Root: dir}
	} else if *origin != "http" {
		return usageError("--origin is http or dir:DIR")
	}
	network, addr, ok := strings.Cut(*listen, ":")
	if !ok || network != "unix" && network != "tcp" {
		return usageError("--listen is unix:PATH or tcp:HOST:PORT")
	}
	if network == "unix" {
		os.Remove(addr)
	}
	ln, err := net.Listen(network, addr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	fetch := &loader.Fetcher{Origin: o, Problem: func(f loader.File, err error) {
		if err == nil {
			fmt.Fprintf(os.Stderr, "kairo loader: %s: reading again\n", f.Name)
			return
		}
		fmt.Fprintf(os.Stderr, "kairo loader: %s: %v\n", f.Name, err)
	}}
	if *rawDir != "" {
		agent := loader.NewAgent(*rawDir, o)
		if *rawRequests != "" {
			agent = loader.NewFileAgent(*rawDir, *rawRequests, o)
		}
		agent.Problem = func(f loader.File, err error) {
			fmt.Fprintf(os.Stderr, "kairo loader: %s: %v\n", f.Name, err)
		}
		fetch.Resolve = agent.Resolve
	}
	l := &loader.Loader{Spec: spec, Fetch: fetch, StateDir: *state, Identified: func(_ int, f loader.File) {
		fmt.Fprintf(os.Stderr, "kairo loader: %s identified: %d bytes, sha256 %s\n", f.Name, f.Size, f.SHA256)
	}}
	if *statusPath != "" {
		go l.WriteStatus(ctx, *statusPath, loaderStatusEvery, loaderStatusWindow)
	}
	fmt.Fprintf(os.Stderr, "kairo loader: listening on %s\n", *listen)
	return (&loader.Server{Loader: l, Positions: positions, World: *world, Session: *session}).Serve(ctx, ln)
}

const (
	// loaderStatusEvery spaces the status file's writes; loaderStatusWindow
	// is what its rates are over.
	loaderStatusEvery  = 5 * time.Second
	loaderStatusWindow = 30 * time.Second
)

// loaderIdentify prints the identity of files (local paths or URLs): what
// a stream's spec lists of each.
func loaderIdentify(args []string) error {
	fs := newFlags("loader identify")
	chunk := fs.Int64("chunk-bytes", loader.DefaultChunkBytes, "the chunk each file is checked by")
	name := fs.String("name", "", "the name of a single file (default its base name)")
	paths, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return usageError("loader identify takes files or URLs")
	}
	var out []loader.File
	for _, p := range paths {
		n := *name
		if n == "" || len(paths) > 1 {
			n = p[strings.LastIndexAny(p, `/\`)+1:]
		}
		var f loader.File
		if strings.HasPrefix(p, "https://") || strings.HasPrefix(p, "http://") {
			resp, err := http.Get(p)
			if err != nil {
				return err
			}
			if resp.StatusCode != http.StatusOK {
				resp.Body.Close()
				return fmt.Errorf("%s: %s", p, resp.Status)
			}
			f, err = loader.Identify(resp.Body, n, *chunk)
			resp.Body.Close()
			if err != nil {
				return err
			}
			f.URL = p
		} else {
			file, err := os.Open(p)
			if err != nil {
				return err
			}
			f, err = loader.Identify(file, n, *chunk)
			file.Close()
			if err != nil {
				return err
			}
		}
		out = append(out, f)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func readJSONFile(path string, v any) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}
