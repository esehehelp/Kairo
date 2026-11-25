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
	"strconv"
	"strings"
	"syscall"
	"time"

	"kairo/internal/api"
)

// Attempt logs over GET /api/attempts/{id}/log: kairo logs ATTEMPT_ID reads
// one attempt's log by bytes; kairo task logs and kairo execution logs find
// the attempt and read by lines.

const (
	maxLogChunk = 8 << 20 // the daemon's limit per call
	// logPollInterval is how often -f asks for new bytes.
	logPollInterval = 2 * time.Second
)

// newInterruptibleClient is newAPIClient for the polling commands: Ctrl+C or
// SIGTERM cancels the returned context and the client's requests.
func newInterruptibleClient(apiURL string) (context.Context, *apiClient, context.CancelFunc, error) {
	client, err := newAPIClient(apiURL)
	if err != nil {
		return nil, nil, nil, err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	client.ctx = ctx
	return ctx, client, stop, nil
}

// logsCommand: kairo logs ATTEMPT_ID prints an attempt's log as the daemon
// holds it: whole, its last --tail bytes, and with --follow what is appended
// after, until interrupted. `kairo task logs` and `kairo execution logs` find
// the attempt for you.
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
	ctx, client, stop, err := newInterruptibleClient(*api)
	if err != nil {
		return err
	}
	defer stop()
	target := logTarget{attemptID: positional[0]}
	out := &logWriter{}
	end, err := printTailBytes(client, target.attemptID, *stream, *tail, out)
	if ctx.Err() != nil {
		return nil
	}
	if err != nil || !*follow {
		return err
	}
	return followLogs(ctx, client, target, []string{*stream}, map[string]int64{*stream: end}, out, false)
}

type logOptions struct {
	stream  *string
	tail    *int
	follow  *bool
	attempt *string
}

func addLogFlags(fs *flag.FlagSet) logOptions {
	o := logOptions{
		stream:  fs.String("stream", "both", "stdout, stderr or both"),
		tail:    fs.Int("tail", 50, "print the last N lines of each stream (0: the whole log)"),
		follow:  fs.Bool("follow", false, "keep printing new output until the attempt has exited (Ctrl+C stops)"),
		attempt: fs.String("attempt", "latest", "latest, all, or an ordinal (task: execution #N; execution: its Nth attempt)"),
	}
	fs.BoolVar(o.follow, "f", false, "shorthand for --follow")
	return o
}

// validate checks the flags and returns the streams to print.
func (o logOptions) validate() ([]string, error) {
	var streams []string
	switch *o.stream {
	case "both":
		streams = []string{"stdout", "stderr"}
	case "stdout", "stderr":
		streams = []string{*o.stream}
	default:
		return nil, usageError("--stream %q: want stdout, stderr or both", *o.stream)
	}
	if *o.tail < 0 {
		return nil, usageError("--tail must be 0 or more")
	}
	if *o.follow && *o.attempt == "all" {
		return nil, usageError("--follow needs a single attempt (not --attempt all)")
	}
	if *o.attempt != "latest" && *o.attempt != "all" {
		if n, err := strconv.Atoi(*o.attempt); err != nil || n < 0 {
			return nil, usageError("--attempt %q: want latest, all or a number", *o.attempt)
		}
	}
	return streams, nil
}

// logTarget is one attempt whose logs are printed.
type logTarget struct {
	label       string // "#2" ("" for an execution's attempt)
	executionID string // "" when not known (kairo logs)
	attemptID   string
}

// taskLogs: kairo task logs PROJECT [QUEUE] TASK [flags].
func taskLogs(args []string) error {
	fs := newFlags("task logs")
	api := apiFlag(fs)
	opts := addLogFlags(fs)
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	ref, err := parseTaskRef("task logs", positional)
	if err != nil {
		return err
	}
	streams, err := opts.validate()
	if err != nil {
		return err
	}
	ctx, client, stop, err := newInterruptibleClient(*api)
	if err != nil {
		return err
	}
	defer stop()
	resolve := func() ([]logTarget, string, error) {
		task, _, err := fetchTask(client, ref)
		if err != nil {
			return nil, "", err
		}
		targets, err := taskLogTargets(task, *opts.attempt)
		return targets, "task " + ref.task + " is " + task.State, err
	}
	// Until the attempt exists, -f polls the task's status, not its detail.
	started := func() (bool, error) {
		resp, err := fetchProject(client, ref.project, projectQuery{tasks: []string{ref.task}})
		if err != nil {
			return false, err
		}
		for _, t := range resp.Project.Tasks {
			if t.Name == ref.task {
				e := t.execution(*opts.attempt)
				return e != nil && e.AttemptID != nil, nil
			}
		}
		return false, nil
	}
	return showLogs(ctx, client, resolve, started, streams, opts)
}

// executionLogs: kairo execution logs EXECUTION_ID [flags].
func executionLogs(args []string) error {
	fs := newFlags("execution logs")
	api := apiFlag(fs)
	opts := addLogFlags(fs)
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("execution logs requires EXECUTION_ID")
	}
	streams, err := opts.validate()
	if err != nil {
		return err
	}
	ctx, client, stop, err := newInterruptibleClient(*api)
	if err != nil {
		return err
	}
	defer stop()
	executionID := positional[0]
	resolve := func() ([]logTarget, string, error) {
		attempt, err := executionAttempt(client, executionID)
		if err != nil {
			return nil, "", err
		}
		var targets []logTarget
		if attempt != nil {
			targets = []logTarget{{executionID: executionID, attemptID: attempt.ID}}
		}
		if n, err := strconv.Atoi(*opts.attempt); err == nil && (n < 1 || n > len(targets)) {
			// An execution has at most one attempt.
			return nil, "", fmt.Errorf("execution %s has %d attempt(s): --attempt counts them from 1, not %d", executionID, len(targets), n)
		}
		return targets, "execution " + executionID + " has no attempt yet", nil
	}
	return showLogs(ctx, client, resolve, nil, streams, opts)
}

// executionAttempt is the attempt of an execution (it has at most one), nil
// before it starts.
func executionAttempt(client *apiClient, executionID string) (*attemptInfo, error) {
	var listed struct {
		Attempts []attemptInfo `json:"attempts"`
	}
	if err := client.callJSON(http.MethodGet, withQuery("/api/attempts", url.Values{"execution_id": {executionID}}), nil, &listed); err != nil {
		return nil, err
	}
	if len(listed.Attempts) == 0 {
		return nil, nil
	}
	return &listed.Attempts[0], nil
}

// taskLogTargets picks the attempts of `task logs`: of the latest execution,
// of execution #N, or of all.
func taskLogTargets(task *taskDetail, which string) ([]logTarget, error) {
	executions := task.Executions
	if which != "all" {
		e := task.execution(which)
		switch {
		case e != nil:
			executions = []taskExecution{*e}
		case which == "latest":
			executions = nil
		default:
			var ordinals []string
			for _, e := range executions {
				ordinals = append(ordinals, "#"+strconv.Itoa(e.Ordinal))
			}
			n, _ := strconv.Atoi(which)
			return nil, fmt.Errorf("task %s has no execution #%d (it has: %s)", task.Name, n, orNone(ordinals))
		}
	}
	var targets []logTarget
	for _, e := range executions {
		label := "#" + strconv.Itoa(e.Ordinal)
		if id := e.attemptID(); id != "" {
			targets = append(targets, logTarget{label: label, executionID: e.ExecutionID, attemptID: id})
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
// output that follows. With --follow and no attempt yet it waits for one,
// asking started (when given) before resolving the targets again.
func showLogs(ctx context.Context, client *apiClient, resolve func() ([]logTarget, string, error), started func() (bool, error), streams []string, opts logOptions) error {
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
			ready := true
			if started != nil {
				ready, err = started()
			}
			if ready && err == nil {
				targets, _, err = resolve()
			}
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}
	out := &logWriter{}
	labelled := len(targets) > 1 || *opts.attempt == "all"
	offsets := map[string]int64{}
	var logErr error
	for _, target := range targets {
		clear(offsets)
		var reason, missingStream string
		missing := 0
		for _, stream := range streams {
			header := len(streams) > 1 || labelled
			end, err := printTailLines(client, target.attemptID, stream, *opts.tail, func(data []byte) {
				if header {
					out.header(target, stream, labelled)
					header = false
				}
				out.write(stream, data)
			})
			if ctx.Err() != nil {
				return nil
			}
			if routeMissing(err) {
				return err
			}
			if why := noLogReason(err); why != "" {
				reason, missingStream = why, stream
				missing++
			} else if err != nil {
				logErr = errors.Join(logErr, fmt.Errorf("%s of %s: %w", stream, target.attemptID, err))
			} else {
				offsets[stream] = end
			}
		}
		if missing > 0 {
			if missing == len(streams) {
				missingStream = ""
			}
			out.noLog(target, missingStream, reason)
		}
	}
	if logErr != nil || !*opts.follow || len(offsets) == 0 {
		return logErr
	}
	return followLogs(ctx, client, targets[0], streams, offsets, out, true)
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

func (w *logWriter) endLine() {
	if w.partial {
		_, _ = os.Stdout.Write([]byte("\n"))
		w.partial = false
	}
}

func (w *logWriter) noLog(target logTarget, stream, reason string) {
	w.endLine()
	if stream != "" {
		stream += " "
	}
	reason = strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ").Replace(reason)
	fmt.Fprintf(os.Stdout, "attempt %s wrote no %slog: %s\n", target.attemptID, stream, reason)
}

func (w *logWriter) header(target logTarget, stream string, labelled bool) {
	w.endLine()
	label := ""
	if labelled && target.label != "" {
		label = target.label + " "
	}
	fmt.Fprintf(os.Stdout, "==> %s%s (%s) <==\n", label, stream, target.attemptID)
	w.stream = stream
}

// logChunk is one slice of an attempt log: its bytes, the offset that
// follows them and the log's size when it was read.
type logChunk struct {
	data       []byte
	next, size int64
}

// start is the offset of the chunk's first byte.
func (c logChunk) start() int64 { return c.next - int64(len(c.data)) }

// fetchLog is one GET /api/attempts/{id}/log.
func fetchLog(client *apiClient, attemptID, stream string, params url.Values) (logChunk, error) {
	params.Set("stream", stream)
	status, header, body, err := client.send(http.MethodGet, "/api/attempts/"+url.PathEscape(attemptID)+"/log?"+params.Encode(), nil)
	if err != nil {
		return logChunk{}, err
	}
	if status != http.StatusOK {
		return logChunk{}, httpError(status, body)
	}
	size, err1 := strconv.ParseInt(header.Get("Kairo-Log-Size"), 10, 64)
	next, err2 := strconv.ParseInt(header.Get("Kairo-Log-Next-Offset"), 10, 64)
	if err1 != nil || err2 != nil {
		return logChunk{}, errors.New("log response without Kairo-Log-Size/Kairo-Log-Next-Offset; " + olderDaemonHint())
	}
	return logChunk{data: body, next: next, size: size}, nil
}

func noLogReason(err error) string {
	var ae *apiError
	if errors.As(err, &ae) && ae.Status == http.StatusConflict && ae.Code == api.CodeNoLog {
		return ae.Message
	}
	return ""
}

// fetchTail is the last n bytes of a log (at most maxLogChunk) in one call.
func fetchTail(client *apiClient, attemptID, stream string, n int64) (logChunk, error) {
	return fetchLog(client, attemptID, stream, url.Values{"tail": {strconv.FormatInt(n, 10)}})
}

// readLog prints a log from offset to its current end and returns the offset
// after what it printed and the log's size.
func readLog(client *apiClient, attemptID, stream string, offset int64, write func([]byte)) (int64, int64, error) {
	for {
		chunk, err := fetchLog(client, attemptID, stream, url.Values{"offset": {strconv.FormatInt(offset, 10)}, "limit": {strconv.Itoa(maxLogChunk)}})
		if err != nil {
			return offset, 0, err
		}
		write(chunk.data)
		offset = chunk.next
		if len(chunk.data) == 0 || offset >= chunk.size {
			return offset, chunk.size, nil
		}
	}
}

// printTailBytes prints the last n bytes of a log (all of it for n < 0) and
// returns the offset just after what it printed.
func printTailBytes(client *apiClient, attemptID, stream string, n int64, out *logWriter) (int64, error) {
	write := func(data []byte) { out.write(stream, data) }
	var offset int64
	if n >= 0 {
		chunk, err := fetchTail(client, attemptID, stream, n)
		if err != nil {
			return 0, err
		}
		if n <= maxLogChunk || chunk.start() == 0 {
			write(chunk.data)
			return chunk.next, nil
		}
		// More than one call holds.
		offset = max(chunk.size-n, 0)
	}
	end, _, err := readLog(client, attemptID, stream, offset, write)
	return end, err
}

// printTailLines prints the last n lines of a log (all of it for n == 0) and
// returns the offset just after what it printed.
func printTailLines(client *apiClient, attemptID, stream string, n int, write func([]byte)) (int64, error) {
	if n == 0 {
		end, _, err := readLog(client, attemptID, stream, 0, write)
		return end, err
	}
	want := int64(max(64<<10, n*256))
	for {
		want = min(want, maxLogChunk)
		chunk, err := fetchTail(client, attemptID, stream, want)
		if err != nil {
			return 0, err
		}
		lines := lastLines(chunk.data, n)
		// The tail holds n whole lines, or it is the whole log, or the cap.
		if len(lines) < len(chunk.data) || chunk.start() == 0 || want >= maxLogChunk {
			write(lines)
			return chunk.next, nil
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

// followLogs polls the target's logs from offsets and prints the new bytes
// until Ctrl+C or, with untilEnded, until the attempt has exited and its logs
// stopped growing.
func followLogs(ctx context.Context, client *apiClient, target logTarget, streams []string, offsets map[string]int64, out *logWriter, untilEnded bool) error {
	quiet := 0
	failing := false
	reportedNoLog := len(offsets) < len(streams)
	for {
		if !sleepFunc(ctx, logPollInterval) {
			return nil
		}
		grew := false
		var pollErr error
		var reason, missingStream string
		for _, stream := range streams {
			offset, ok := offsets[stream]
			if !ok {
				continue
			}
			next, size, err := readLog(client, target.attemptID, stream, offset, func(data []byte) {
				if len(data) > 0 && len(streams) > 1 && out.stream != stream {
					out.header(target, stream, false)
				}
				out.write(stream, data)
			})
			if ctx.Err() != nil {
				return nil
			}
			if why := noLogReason(err); why != "" {
				if !untilEnded {
					return err
				}
				delete(offsets, stream)
				reason, missingStream = why, stream
				continue
			}
			if err == nil && size < offset {
				// The log was replaced (shipped anew): start over.
				next = 0
			}
			grew = grew || next > offset
			offsets[stream] = next
			if err != nil {
				if routeMissing(err) {
					return err
				}
				if untilEnded {
					pollErr = fmt.Errorf("%s of %s: %w", stream, target.attemptID, err)
					if !transient(err) {
						return pollErr
					}
				}
				if !notFound(err) && !failing {
					fmt.Fprintf(os.Stderr, "kairo: %s of %s: %v (retrying)\n", stream, target.attemptID, err)
					failing = true
				}
				continue
			}
			failing = false
		}
		if reason != "" && !reportedNoLog {
			if len(offsets) == 0 {
				missingStream = ""
			}
			out.noLog(target, missingStream, reason)
			reportedNoLog = true
		}
		if len(offsets) == 0 {
			return nil
		}
		if grew || !untilEnded {
			quiet = 0
			continue
		}
		attempt, err := executionAttempt(client, target.executionID)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil || attempt == nil || !attempt.ended() {
			quiet = 0
			continue
		}
		// Exited: one more quiet poll lets late (shipped) output arrive.
		if quiet++; quiet >= 2 {
			return pollErr
		}
	}
}
