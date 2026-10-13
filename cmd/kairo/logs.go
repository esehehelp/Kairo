package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"time"
)

// Attempt logs over GET /api/attempts/{id}/log: kairo task logs and
// kairo execution logs.

const (
	maxLogChunk = 8 << 20 // the daemon's limit per call
	// logPollInterval is how often -f asks for new bytes.
	logPollInterval = 2 * time.Second
)

type logOptions struct {
	stream  *string
	tail    *int
	follow  *bool
	attempt *string
	rank    *int
}

func addLogFlags(fs *flag.FlagSet, withRank bool) logOptions {
	o := logOptions{
		stream:  fs.String("stream", "both", "stdout, stderr or both"),
		tail:    fs.Int("tail", 50, "print the last N lines of each stream (0: the whole log)"),
		follow:  fs.Bool("follow", false, "keep printing new output until the attempt has exited (Ctrl+C stops)"),
		attempt: fs.String("attempt", "latest", "latest, all, or an ordinal (task: execution #N; execution: its Nth attempt)"),
	}
	fs.BoolVar(o.follow, "f", false, "shorthand for --follow")
	if withRank {
		o.rank = fs.Int("rank", 0, "gang rank whose attempt to show")
	}
	return o
}

func (o logOptions) streams() ([]string, error) {
	switch *o.stream {
	case "both":
		return []string{"stdout", "stderr"}, nil
	case "stdout", "stderr":
		return []string{*o.stream}, nil
	}
	return nil, usageError("--stream %q: want stdout, stderr or both", *o.stream)
}

func (o logOptions) validate() error {
	if *o.tail < 0 {
		return usageError("--tail must be 0 or more")
	}
	if *o.follow && *o.attempt == "all" {
		return usageError("--follow needs a single attempt (not --attempt all)")
	}
	if *o.attempt != "latest" && *o.attempt != "all" {
		if n, err := strconv.Atoi(*o.attempt); err != nil || n < 1 {
			return usageError("--attempt %q: want latest, all or a number from 1", *o.attempt)
		}
	}
	return nil
}

// logTarget is one attempt whose logs are printed.
type logTarget struct {
	label       string // "#2", "#2 rank 1" ("" for an execution's only attempt)
	executionID string
	attemptID   string
}

// interruptContext is cancelled by Ctrl+C.
func interruptContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
}

// taskLogs: kairo task logs PROJECT [QUEUE] TASK [flags].
func taskLogs(args []string) error {
	fs := newFlags("task logs")
	api := apiFlag(fs)
	opts := addLogFlags(fs, true)
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	ref, err := parseTaskRef("task logs", positional)
	if err != nil {
		return err
	}
	streams, err := opts.streams()
	if err != nil {
		return err
	}
	if err := opts.validate(); err != nil {
		return err
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	ctx, stop := interruptContext()
	defer stop()
	client.ctx = ctx
	resolve := func() ([]logTarget, string, error) {
		task, _, err := fetchTask(client, ref)
		if err != nil {
			return nil, "", err
		}
		targets, err := taskLogTargets(task, *opts.attempt, *opts.rank)
		return targets, "task " + ref.task + " is " + task.State, err
	}
	return showLogs(ctx, client, resolve, streams, opts)
}

// executionLogs: kairo execution logs EXECUTION_ID [flags].
func executionLogs(args []string) error {
	fs := newFlags("execution logs")
	api := apiFlag(fs)
	opts := addLogFlags(fs, false)
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("execution logs requires EXECUTION_ID")
	}
	streams, err := opts.streams()
	if err != nil {
		return err
	}
	if err := opts.validate(); err != nil {
		return err
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	ctx, stop := interruptContext()
	defer stop()
	client.ctx = ctx
	executionID := positional[0]
	resolve := func() ([]logTarget, string, error) {
		attempts, err := executionAttempts(client, executionID)
		if err != nil {
			return nil, "", err
		}
		var targets []logTarget
		for i, a := range attempts {
			label := ""
			if len(attempts) > 1 {
				label = "attempt " + strconv.Itoa(i+1)
			}
			targets = append(targets, logTarget{label: label, executionID: executionID, attemptID: a.ID})
		}
		switch *opts.attempt {
		case "all":
		case "latest":
			if len(targets) > 0 {
				targets = targets[len(targets)-1:]
			}
		default:
			n, _ := strconv.Atoi(*opts.attempt)
			if n > len(targets) {
				return nil, "", fmt.Errorf("execution %s has %d attempt(s), not %d", executionID, len(targets), n)
			}
			targets = targets[n-1 : n]
		}
		return targets, "execution " + executionID + " has no attempt yet", nil
	}
	return showLogs(ctx, client, resolve, streams, opts)
}

// executionAttempts lists an execution's attempts, oldest first.
func executionAttempts(client *apiClient, executionID string) ([]attemptInfo, error) {
	var listed struct {
		Attempts []attemptInfo `json:"attempts"`
	}
	if err := client.getJSON(withQuery("/api/attempts", url.Values{"execution_id": {executionID}, "limit": {"1000"}}), &listed); err != nil {
		return nil, err
	}
	sortAttempts(listed.Attempts)
	return listed.Attempts, nil
}

// sortAttempts orders attempts by start, the unstarted last.
func sortAttempts(attempts []attemptInfo) {
	sort.SliceStable(attempts, func(i, j int) bool {
		a, aok := parseStamp(attempts[i].StartedAt)
		b, bok := parseStamp(attempts[j].StartedAt)
		if aok != bok {
			return aok
		}
		return a.Before(b)
	})
}

// taskLogTargets picks the attempts of `task logs`: of the latest execution,
// of execution #N, or of all; for a gang, the attempt of the given rank.
func taskLogTargets(task *taskDetail, which string, rank int) ([]logTarget, error) {
	executions := task.Executions
	switch which {
	case "all":
	case "latest":
		if len(executions) > 0 {
			executions = executions[len(executions)-1:]
		}
	default:
		n, _ := strconv.Atoi(which)
		var found []taskExecution
		var ordinals []string
		for _, e := range executions {
			ordinals = append(ordinals, "#"+strconv.Itoa(e.Ordinal))
			if e.Ordinal == n {
				found = append(found, e)
			}
		}
		if len(found) == 0 {
			return nil, fmt.Errorf("task %s has no execution #%d (it has: %s)", task.Name, n, orNone(ordinals))
		}
		executions = found
	}
	var targets []logTarget
	for _, e := range executions {
		label := "#" + strconv.Itoa(e.Ordinal)
		if len(e.Gang) > 0 {
			var picked *gangRankInfo
			for i := range e.Gang {
				if e.Gang[i].Rank == rank {
					picked = &e.Gang[i]
				}
			}
			if picked == nil {
				return nil, fmt.Errorf("execution #%d of task %s has no gang rank %d", e.Ordinal, task.Name, rank)
			}
			if picked.AttemptID != nil {
				targets = append(targets, logTarget{label: fmt.Sprintf("%s rank %d", label, rank), executionID: picked.ExecutionID, attemptID: *picked.AttemptID})
			}
			continue
		}
		if rank != 0 {
			return nil, fmt.Errorf("task %s is not a gang: --rank does not apply", task.Name)
		}
		attempts := append([]attemptInfo(nil), e.Attempts...)
		sortAttempts(attempts)
		if len(attempts) == 0 && e.AttemptID != nil {
			attempts = []attemptInfo{{ID: *e.AttemptID}}
		}
		if which != "all" && len(attempts) > 1 {
			attempts = attempts[len(attempts)-1:]
		}
		for _, a := range attempts {
			targets = append(targets, logTarget{label: label, executionID: e.ExecutionID, attemptID: a.ID})
		}
	}
	return targets, nil
}

func orNone(list []string) string {
	if len(list) == 0 {
		return "none"
	}
	return fmt.Sprint(list)
}

// showLogs prints the tails of the targets' logs and, with --follow, the
// output that follows.
func showLogs(ctx context.Context, client *apiClient, resolve func() ([]logTarget, string, error), streams []string, opts logOptions) error {
	targets, why, err := resolve()
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		if !*opts.follow {
			return fmt.Errorf("no attempt to show yet (%s)", why)
		}
		fmt.Fprintf(os.Stderr, "kairo: waiting for an attempt to start (%s)\n", why)
		for len(targets) == 0 {
			if !sleepFunc(ctx, logPollInterval) {
				return nil
			}
			if targets, _, err = resolve(); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
	out := &logWriter{}
	labelled := len(targets) > 1 || *opts.attempt == "all"
	offsets := map[string]int64{}
	for _, target := range targets {
		for _, stream := range streams {
			if len(streams) > 1 || labelled {
				out.header(target, stream, labelled)
			}
			end, err := printTail(client, target.attemptID, stream, *opts.tail, out)
			if err != nil {
				if routeMissing(err) || ctx.Err() != nil {
					return quietCancel(ctx, err)
				}
				fmt.Fprintf(os.Stderr, "kairo: %s of %s: %v\n", stream, target.attemptID, err)
			}
			offsets[stream] = end
		}
	}
	if !*opts.follow {
		return nil
	}
	return followLogs(ctx, client, targets[0], streams, offsets, out)
}

// quietCancel turns an error caused by Ctrl+C into a clean exit.
func quietCancel(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// logWriter writes log bytes to stdout and remembers whether the last byte
// ended a line, so a header never lands in the middle of one.
type logWriter struct {
	partial bool
	stream  string
}

func (w *logWriter) write(stream string, data []byte) {
	if len(data) == 0 {
		return
	}
	_, _ = os.Stdout.Write(data)
	w.partial = data[len(data)-1] != '\n'
	w.stream = stream
}

func (w *logWriter) header(target logTarget, stream string, labelled bool) {
	if w.partial {
		_, _ = os.Stdout.Write([]byte("\n"))
		w.partial = false
	}
	label := ""
	if labelled && target.label != "" {
		label = target.label + " "
	}
	fmt.Fprintf(os.Stdout, "==> %s%s (%s) <==\n", label, stream, target.attemptID)
	w.stream = stream
}

// fetchLog is one GET /api/attempts/{id}/log; it returns the bytes, the
// log's total size and the offset of the first byte returned.
func fetchLog(client *apiClient, attemptID, stream string, params url.Values) ([]byte, int64, int64, error) {
	params.Set("stream", stream)
	status, header, body, err := client.send(http.MethodGet, "/api/attempts/"+url.PathEscape(attemptID)+"/log?"+params.Encode(), nil)
	if err != nil {
		return nil, 0, 0, err
	}
	if status != http.StatusOK {
		return nil, 0, 0, httpError(status, body)
	}
	size, err1 := strconv.ParseInt(header.Get("X-Kairo-Log-Size"), 10, 64)
	offset, err2 := strconv.ParseInt(header.Get("X-Kairo-Log-Offset"), 10, 64)
	if err1 != nil || err2 != nil {
		return nil, 0, 0, errors.New("log response without X-Kairo-Log-Size/X-Kairo-Log-Offset; " + olderDaemonHint())
	}
	return body, size, offset, nil
}

// printTail prints the last n lines of a log (all of it for n == 0) and
// returns the offset just after what it printed.
func printTail(client *apiClient, attemptID, stream string, n int, out *logWriter) (int64, error) {
	if n == 0 {
		var offset int64
		for {
			data, size, start, err := fetchLog(client, attemptID, stream, url.Values{"offset": {strconv.FormatInt(offset, 10)}, "limit": {strconv.Itoa(maxLogChunk)}})
			if err != nil {
				return offset, err
			}
			out.write(stream, data)
			offset = start + int64(len(data))
			if len(data) == 0 || offset >= size {
				return offset, nil
			}
		}
	}
	want := max(64<<10, n*256)
	for {
		want = min(want, maxLogChunk)
		data, _, start, err := fetchLog(client, attemptID, stream, url.Values{"tail": {strconv.Itoa(want)}})
		if err != nil {
			return 0, err
		}
		end := start + int64(len(data))
		lines := lastLines(data, n)
		// The tail holds n whole lines, or it is the whole log, or the cap.
		if len(lines) < len(data) || start == 0 || want >= maxLogChunk {
			out.write(stream, lines)
			return end, nil
		}
		want *= 4
	}
}

// lastLines is the suffix of data holding its last n lines; when data has
// no more than n lines, it is all of data.
func lastLines(data []byte, n int) []byte {
	body := bytes.TrimSuffix(data, []byte("\n"))
	count := 0
	for i := len(body) - 1; i >= 0; i-- {
		if body[i] == '\n' {
			count++
			if count == n {
				return data[i+1:]
			}
		}
	}
	return data
}

// followLogs polls the target's logs from offsets and prints new bytes until
// the attempt has exited and its logs stopped growing, or Ctrl+C.
func followLogs(ctx context.Context, client *apiClient, target logTarget, streams []string, offsets map[string]int64, out *logWriter) error {
	quiet := 0
	failing := false
	for {
		if !sleepFunc(ctx, logPollInterval) {
			return nil
		}
		grew := false
		for _, stream := range streams {
			data, size, start, err := fetchLog(client, target.attemptID, stream, url.Values{"offset": {strconv.FormatInt(offsets[stream], 10)}, "limit": {strconv.Itoa(maxLogChunk)}})
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				if routeMissing(err) {
					return err
				}
				if !notFound(err) && !failing {
					fmt.Fprintf(os.Stderr, "kairo: %s of %s: %v (retrying)\n", stream, target.attemptID, err)
					failing = true
				}
				continue
			}
			failing = false
			if size < offsets[stream] {
				// The log was replaced (shipped anew): start over.
				offsets[stream] = 0
				continue
			}
			if len(data) == 0 {
				continue
			}
			if len(streams) > 1 && out.stream != stream {
				out.header(target, stream, false)
			}
			out.write(stream, data)
			offsets[stream] = start + int64(len(data))
			grew = true
		}
		ended, err := attemptEnded(client, target)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil || !ended || grew {
			quiet = 0
			continue
		}
		// Exited: one more quiet poll lets late (shipped) output arrive.
		if quiet++; quiet >= 2 {
			return nil
		}
	}
}

// attemptEnded asks the daemon whether the target's attempt has exited.
func attemptEnded(client *apiClient, target logTarget) (bool, error) {
	attempts, err := executionAttempts(client, target.executionID)
	if err != nil {
		return false, err
	}
	for _, a := range attempts {
		if a.ID == target.attemptID {
			return a.ended(), nil
		}
	}
	return false, nil
}
