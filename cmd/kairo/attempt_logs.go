package main

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// logChunkLimit is how much one GET /api/attempts/{id}/log of `kairo logs` asks for.
const logChunkLimit = 1 << 20

// logFollowInterval is how often `kairo logs --follow` polls for more log.
var logFollowInterval = 2 * time.Second

// logsCommand: kairo logs ATTEMPT_ID prints an attempt's log as the daemon
// holds it (GET /api/attempts/{id}/log): whole, its last --tail bytes, and
// with --follow what is appended after, until interrupted. `kairo task logs`
// and `kairo execution logs` find the attempt for you.
func logsCommand(args []string) error {
	fs := newFlags("logs")
	api := apiFlag(fs)
	stream := fs.String("stream", "stderr", "log stream: stdout or stderr")
	follow := fs.Bool("follow", false, "keep printing what is appended, every 2 s, until interrupted")
	fs.BoolVar(follow, "f", false, "shorthand for --follow")
	tail := fs.Int64("tail", -1, "print only the last N bytes")
	positional, err := parseFlags(fs, leafArgs(args))
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("logs requires ATTEMPT_ID (kairo task logs PROJECT TASK finds it for you)")
	}
	if *stream != "stdout" && *stream != "stderr" {
		return usageError("--stream must be stdout or stderr, not %q", *stream)
	}
	if *tail < -1 {
		return usageError("--tail must be a number of bytes")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err = printLog(ctx, client, positional[0], *stream, *tail, *follow, os.Stdout)
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
	data, size, start, err := fetchLog(c, attemptID, stream, url.Values{
		"offset": {strconv.FormatInt(offset, 10)},
		"limit":  {strconv.FormatInt(limit, 10)},
	})
	if err != nil {
		return logChunk{}, err
	}
	return logChunk{data: data, next: start + int64(len(data)), size: size}, nil
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
