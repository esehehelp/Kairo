package executor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"kairo/internal/processidentity"
	"kairo/internal/store"
)

// An attempt runs under a shim: this binary run as shimArg, the attempt's
// launcher (its pid, process group, job object and creation time are the
// attempt's). The shim waits for the go-ahead on its stdin (the agent has
// put it in the attempt's job object, handing it a handle to the job so the
// job keeps its name while it runs, and recorded the attempt), starts the
// attempt's command as its child (confined when asked, as confineCommand
// does), lets what it is sent to stop it go by (the command, in its process
// group, is sent the same), and writes the command's exit code to the
// attempt's exit file before it exits with it. An agent is thus not the
// parent of the work: one restarted while the attempt runs adopts it
// (AdoptableAttempts, Local.Adopt) and learns its exit from the exit file.
//
//	kairo __kairo-shim --exit FILE [--write DIR]... [--scratch DIR] -- PATH ARGV0 ARGV...
const shimArg = "__kairo-shim"

// Exit codes of a shim that ran no command: no go-ahead (the agent went
// away first; nothing ran, and no exit is written), or one it could not
// confine or start (written, with why on its stderr).
const (
	shimNoGoAhead   = 125
	shimNotConfined = 126
	shimNotStarted  = 127
)

// shimExit is an exit file's content.
type shimExit struct {
	ExitCode int `json:"exit_code"`
}

// runShim runs the shim when args name it and does not return then.
func runShim(args []string) {
	if len(args) < 2 || args[1] != shimArg {
		return
	}
	os.Exit(shim(args[2:], os.Stdin))
}

func shim(args []string, goAhead io.Reader) int {
	// A graceful stop reaches the attempt's whole process group (SIGTERM,
	// CTRL_BREAK): the command decides what to do with it; the shim stays
	// to write its exit.
	stop := make(chan os.Signal, 8)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for range stop {
		}
	}()
	var exitFile, scratch string
	var writable []string
	i := 0
	for ; i < len(args); i++ {
		if args[i] == "--" {
			i++
			break
		}
		if i+1 >= len(args) {
			break
		}
		switch args[i] {
		case "--exit":
			exitFile = args[i+1]
		case "--write":
			writable = append(writable, args[i+1])
		case "--scratch":
			scratch = args[i+1]
		default:
			fmt.Fprintf(os.Stderr, "kairo shim: unknown argument %q\n", args[i])
			return shimNotStarted
		}
		i++
	}
	command := args[i:]
	if exitFile == "" || len(command) < 2 {
		fmt.Fprintln(os.Stderr, "kairo shim: usage: --exit FILE [--write DIR]... [--scratch DIR] -- PATH ARGV0 ARGV...")
		return shimNotStarted
	}
	var b [1]byte
	if n, _ := io.ReadFull(goAhead, b[:]); n != 1 {
		return shimNoGoAhead
	}
	cmd := &exec.Cmd{Path: command[0], Args: command[1:], Stdout: os.Stdout, Stderr: os.Stderr}
	var release func()
	if len(writable) > 0 {
		r, err := confineCommand(cmd, writable, scratch)
		if err != nil {
			fmt.Fprintln(os.Stderr, "kairo shim:", err)
			return writeShimExit(exitFile, shimNotConfined)
		}
		release = r
	}
	err := cmd.Start()
	if release != nil {
		release()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kairo shim: start:", err)
		return writeShimExit(exitFile, shimNotStarted)
	}
	_ = cmd.Wait()
	return writeShimExit(exitFile, cmd.ProcessState.ExitCode())
}

// writeShimExit writes code to path (whole or not at all) and returns it.
func writeShimExit(path string, code int) int {
	body, _ := json.Marshal(shimExit{ExitCode: code})
	tmp := path + ".tmp"
	err := os.WriteFile(tmp, body, 0o644)
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kairo shim: exit not written:", err)
	}
	return code
}

// AttemptRecord is what an agent keeps on disk of an attempt it launched
// (in recordsDir), to adopt it once restarted.
type AttemptRecord struct {
	AttemptID  string `json:"attempt_id"`
	ExecutorID string `json:"executor_id"`
	LeaseID    string `json:"lease_id"`
	Epoch      int64  `json:"epoch"`
	RunID      string `json:"run_id,omitempty"`
	// PID and Identity are the shim's (the attempt's launcher).
	PID      int    `json:"pid"`
	Identity string `json:"process_identity"`
	// Job names the attempt's job object (Windows).
	Job string `json:"job,omitempty"`
}

// recordsDir is where the agent writing logs to logDir keeps its attempt
// records and their exit files.
func recordsDir(logDir string) string { return filepath.Join(logDir, "running") }

func recordPath(logDir, attemptID string) string {
	return filepath.Join(recordsDir(logDir), attemptID+".json")
}

func exitPath(logDir, attemptID string) string {
	return filepath.Join(recordsDir(logDir), attemptID+".exit")
}

func writeRecord(logDir string, r AttemptRecord) error {
	if err := os.MkdirAll(recordsDir(logDir), 0o755); err != nil {
		return err
	}
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	path := recordPath(logDir, r.AttemptID)
	if err = os.WriteFile(path+".tmp", body, 0o644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

// forgetRecord removes an attempt's record and exit file.
func forgetRecord(logDir, attemptID string) {
	for _, p := range []string{recordPath(logDir, attemptID), exitPath(logDir, attemptID), exitPath(logDir, attemptID) + ".tmp"} {
		_ = os.Remove(p)
	}
}

// readExit returns the exit the attempt's shim wrote, if it did.
func readExit(logDir, attemptID string) (int, bool) {
	body, err := os.ReadFile(exitPath(logDir, attemptID))
	if err != nil {
		return 0, false
	}
	var e shimExit
	if json.Unmarshal(body, &e) != nil {
		return 0, false
	}
	return e.ExitCode, true
}

// AdoptableAttempts lists the attempts recorded in logDir for executors
// whose shim still runs, or is gone having written its exit; the record of
// any other (its shim killed, the node rebooted) is removed: the daemon
// marks that attempt lost. Records of other executors are left alone.
func AdoptableAttempts(logDir string, executors map[string]bool, logger *slog.Logger) []AttemptRecord {
	paths, _ := filepath.Glob(filepath.Join(recordsDir(logDir), "*.json"))
	var out []AttemptRecord
	for _, path := range paths {
		body, err := os.ReadFile(path)
		var r AttemptRecord
		if err != nil || json.Unmarshal(body, &r) != nil || r.AttemptID == "" || filepath.Base(path) != r.AttemptID+".json" {
			logger.Warn("attempt record unreadable; removed", "path", path, "error", err)
			_ = os.Remove(path)
			continue
		}
		if !executors[r.ExecutorID] {
			continue
		}
		if _, exited := readExit(logDir, r.AttemptID); exited {
			out = append(out, r)
			continue
		}
		liveness, err := processidentity.HostProcessLiveness(r.PID, r.Identity)
		if liveness == processidentity.LivenessAbsent {
			logger.Warn("attempt's shim is gone without its exit: not adopted (lost)", "attempt_id", r.AttemptID, "pid", r.PID)
			forgetRecord(logDir, r.AttemptID)
			continue
		}
		if err != nil {
			// Not known gone: kept, and watched until it is known.
			logger.Warn("attempt's shim liveness unknown; adopting it", "attempt_id", r.AttemptID, "error", err)
		}
		out = append(out, r)
	}
	return out
}

// Adopted turns records into what the daemon is told the agent adopts.
func Adopted(records []AttemptRecord) []store.AdoptedAttempt {
	out := make([]store.AdoptedAttempt, len(records))
	for i, r := range records {
		out[i] = store.AdoptedAttempt{AttemptID: r.AttemptID, ExecutorID: r.ExecutorID, PID: r.PID, ProcessIdentity: r.Identity}
	}
	return out
}

// adoptPoll is how often an adopted attempt's shim is checked for its exit.
var adoptPoll = time.Second

// Adopt takes over the attempts of this executor in records that the daemon
// kept (adopted): it watches each shim until it exits and then records the
// exit as a launch does; the record of every other one is removed.
func (e *Local) Adopt(records []AttemptRecord, adopted []string) {
	kept := map[string]bool{}
	for _, id := range adopted {
		kept[id] = true
	}
	for _, r := range records {
		if r.ExecutorID != e.ID {
			continue
		}
		if !kept[r.AttemptID] {
			e.Logger.Warn("attempt not adopted by the daemon; its record is removed", "attempt_id", r.AttemptID)
			forgetRecord(e.LogDir, r.AttemptID)
			continue
		}
		tree, err := openTree(r.Job)
		if err != nil {
			e.Logger.Warn("adopted attempt's job object not opened; its tree is found by its process group and environment", "attempt_id", r.AttemptID, "error", err)
		}
		e.mu.Lock()
		e.running[r.AttemptID] = r.PID
		if tree != nil {
			if e.trees == nil {
				e.trees = map[string]*treeHandle{}
			}
			e.trees[r.AttemptID] = tree
		}
		if r.RunID != "" {
			if e.outputRuns == nil {
				e.outputRuns = map[string]string{}
			}
			e.outputRuns[r.AttemptID] = r.RunID
		}
		e.mu.Unlock()
		e.trackUsage(r.AttemptID, r.PID, r.Identity)
		e.Logger.Info("adopted attempt", "attempt_id", r.AttemptID, "pid", r.PID)
		go e.watchAdopted(r)
	}
}

// watchAdopted waits for an adopted attempt's shim to exit, then records the
// exit it wrote (-1 when it wrote none: killed).
func (e *Local) watchAdopted(r AttemptRecord) {
	var warned bool
	for {
		liveness, err := processidentity.HostProcessLiveness(r.PID, r.Identity)
		if liveness == processidentity.LivenessAbsent {
			break
		}
		if err != nil && !warned {
			e.Logger.Warn("adopted attempt's shim liveness unknown; still watching", "attempt_id", r.AttemptID, "error", err)
			warned = true
		}
		time.Sleep(adoptPoll)
	}
	exit, ok := readExit(e.LogDir, r.AttemptID)
	if !ok {
		e.Logger.Warn("adopted attempt's shim exited without writing its exit; recorded as killed", "attempt_id", r.AttemptID)
		exit = -1
	}
	e.finishAttempt(r, exit, nil)
}

// shimCommand wraps cmd, an attempt's command as built for it (confined as
// conf says, when not nil), in its shim: the shim gets cmd's environment,
// directory and process attributes (its own process group or console group,
// which the command then shares); goAhead lets the command start.
func shimCommand(cmd *exec.Cmd, exitFile string, conf *confinement) (shim *exec.Cmd, goAhead *os.File, err error) {
	self, err := os.Executable()
	if err != nil {
		return nil, nil, err
	}
	args := []string{self, shimArg, "--exit", exitFile}
	if conf != nil {
		for _, w := range conf.writable {
			args = append(args, "--write", w)
		}
		args = append(args, "--scratch", conf.scratch)
	}
	args = append(append(args, "--", cmd.Path), cmd.Args...)
	r, w, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	shim = &exec.Cmd{Path: self, Args: args, Env: cmd.Env, Dir: cmd.Dir, Stdin: r, SysProcAttr: cmd.SysProcAttr}
	return shim, w, nil
}

// sendGoAhead lets a shim start its command.
func sendGoAhead(w *os.File) error {
	_, err := w.Write([]byte{1})
	return errors.Join(err, w.Close())
}
