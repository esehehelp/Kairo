package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const logsUsage = "usage: kairo logs [--api URL] [--stream stdout|stderr] [--follow] [--tail N] ATTEMPT_ID"

// logChunkLimit is how much one GET /api/attempts/{id}/log asks for.
const logChunkLimit = 1 << 20

// logFollowInterval is how often --follow polls for more log.
var logFollowInterval = 2 * time.Second

// logsCommand prints an attempt's log as the daemon holds it (GET
// /api/attempts/{id}/log): whole, its last --tail bytes, and with --follow
// what is appended after, until interrupted.
func logsCommand(args []string) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	api := apiFlag(fs)
	stream := fs.String("stream", "stderr", "log stream: stdout or stderr")
	follow := fs.Bool("follow", false, "keep printing what is appended, every 2 s, until interrupted")
	tail := fs.Int64("tail", -1, "print only the last N bytes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New(logsUsage)
	}
	if *stream != "stdout" && *stream != "stderr" {
		return fmt.Errorf("--stream must be stdout or stderr, not %q", *stream)
	}
	if *tail < -1 {
		return errors.New("--tail must be a number of bytes")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err = printLog(ctx, client, fs.Arg(0), *stream, *tail, *follow, os.Stdout)
	if *follow && errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// logChunk is one slice of an attempt log: its bytes, the offset that
// follows them and the log's size when it was read.
type logChunk struct {
	data       []byte
	next, size int64
}

func (c *apiClient) attemptLog(attemptID, stream string, offset, limit int64) (logChunk, error) {
	query := url.Values{}
	query.Set("stream", stream)
	query.Set("offset", strconv.FormatInt(offset, 10))
	query.Set("limit", strconv.FormatInt(limit, 10))
	status, header, payload, err := c.send(http.MethodGet, "/api/attempts/"+url.PathEscape(attemptID)+"/log?"+query.Encode(), nil)
	if err != nil {
		return logChunk{}, err
	}
	if status < 200 || status >= 300 {
		return logChunk{}, httpError(status, payload)
	}
	chunk := logChunk{data: payload}
	for _, h := range []struct {
		name   string
		target *int64
	}{{"Kairo-Log-Next-Offset", &chunk.next}, {"Kairo-Log-Size", &chunk.size}} {
		if *h.target, err = strconv.ParseInt(header.Get(h.name), 10, 64); err != nil {
			return logChunk{}, fmt.Errorf("log response without a valid %s header", h.name)
		}
	}
	return chunk, nil
}

// printLog writes the log to out from the start, or from its last tail bytes
// (tail >= 0); with follow it keeps polling until ctx ends.
func printLog(ctx context.Context, client *apiClient, attemptID, stream string, tail int64, follow bool, out io.Writer) error {
	offset := int64(0)
	if tail >= 0 {
		head, err := client.attemptLog(attemptID, stream, 0, 0)
		if err != nil {
			return err
		}
		offset = max(head.size-tail, 0)
	}
	for {
		chunk, err := client.attemptLog(attemptID, stream, offset, logChunkLimit)
		if err != nil {
			return err
		}
		if _, err = out.Write(chunk.data); err != nil {
			return err
		}
		offset = chunk.next
		if offset < chunk.size {
			continue
		}
		if !follow {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(logFollowInterval):
		}
	}
}
