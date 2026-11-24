package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"kairo/internal/loader"
	"kairo/internal/store"
)

// loaderServe: kairo loader serve --spec SPEC.json --listen
// unix:PATH|tcp:HOST:PORT [--world N] [--positions POSITIONS.json] [--origin
// http|dir:DIR].
func loaderServe(args []string) error {
	fs := newFlags("loader serve")
	specPath := fs.String("spec", "", "the stream: sources with their files identified, seed, block, epochs, stages (JSON)")
	listen := fs.String("listen", "", "where the ranks connect: unix:PATH or tcp:HOST:PORT")
	world := fs.Int("world", 0, "the ranks the stream is split between (0: the first rank's hello says)")
	positionsPath := fs.String("positions", "", "where each rank starts (JSON list of {rank, record, sub}); none: the start")
	origin := fs.String("origin", "http", "where files are read: http (each file's URL) or dir:DIR (files by name under DIR)")
	report := fs.Bool("report", false, "report what is learned of files (identities, record counts) to the daemon as the attempt (KAIRO_API_URL, KAIRO_ATTEMPT_TOKEN, KAIRO_API_CA)")
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
	}
	var o loader.Origin = loader.HTTPOrigin{Client: http.DefaultClient}
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
	l := &loader.Loader{Spec: spec, Fetch: fetch}
	if *report {
		r, err := newStreamReporter()
		if err != nil {
			return err
		}
		go r.run(ctx)
		l.Learned = func(source int, sha string, records int64) {
			src := spec.Sources[source]
			r.add(store.StreamReport{Records: []store.RecordsCount{{SHA256: sha, Format: src.Format, Field: src.Field, Records: records}}})
		}
		l.Identified = func(_ int, f loader.File) {
			fmt.Fprintf(os.Stderr, "kairo loader: %s identified: %d bytes, sha256 %s\n", f.Name, f.Size, f.SHA256)
			r.add(store.StreamReport{Identities: []loader.File{f}})
		}
	}
	fmt.Fprintf(os.Stderr, "kairo loader: listening on %s\n", *listen)
	return (&loader.Server{Loader: l, Positions: positions, World: *world}).Serve(ctx, ln)
}

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

// streamReporter sends what a loader learned to the daemon, as its attempt.
type streamReporter struct {
	client *http.Client
	url    string
	token  string
	queue  chan store.StreamReport
}

func newStreamReporter() (*streamReporter, error) {
	api, token := os.Getenv("KAIRO_API_URL"), os.Getenv("KAIRO_ATTEMPT_TOKEN")
	if api == "" || token == "" {
		return nil, errors.New("--report needs KAIRO_API_URL and KAIRO_ATTEMPT_TOKEN (an attempt's environment)")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if ca := os.Getenv("KAIRO_API_CA"); ca != "" {
		der, err := base64.StdEncoding.DecodeString(ca)
		if err != nil {
			return nil, fmt.Errorf("KAIRO_API_CA: %w", err)
		}
		certs, err := x509.ParseCertificates(der)
		if err != nil {
			return nil, fmt.Errorf("KAIRO_API_CA: %w", err)
		}
		pool := x509.NewCertPool()
		for _, c := range certs {
			pool.AddCert(c)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &streamReporter{client: &http.Client{Transport: transport, Timeout: time.Minute}, url: strings.TrimRight(api, "/") + "/api/worker/streams", token: token, queue: make(chan store.StreamReport, 1024)}, nil
}

func (r *streamReporter) add(report store.StreamReport) {
	select {
	case r.queue <- report:
	default:
		fmt.Fprintln(os.Stderr, "kairo loader: too much to report; a report dropped")
	}
}

// run sends each report, trying again for a while when the daemon is away.
func (r *streamReporter) run(ctx context.Context) {
	for {
		var report store.StreamReport
		select {
		case report = <-r.queue:
		case <-ctx.Done():
			return
		}
		body, _ := json.Marshal(report)
		pause := time.Second
		for try := 1; ; try++ {
			err := r.send(ctx, body)
			if err == nil {
				break
			}
			if try == 10 || ctx.Err() != nil {
				fmt.Fprintf(os.Stderr, "kairo loader: a report not kept: %v\n", err)
				break
			}
			select {
			case <-time.After(pause):
			case <-ctx.Done():
				return
			}
			pause = min(pause*2, time.Minute)
		}
	}
}

func (r *streamReporter) send(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(msg))
	}
	return nil
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
