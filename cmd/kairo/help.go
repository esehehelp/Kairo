package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// Help, usage errors and exit codes.
//
// Every command prints its usage to stdout and exits 0 when asked for help
// (help, -h, --help); a mistyped command prints the usage to stderr, with a
// suggestion when one is close, and exits 2.

// cliError carries an exit code. An empty message prints nothing (the
// command already said what happened); raw prints the message without the
// "kairo: " prefix (usage texts).
type cliError struct {
	code int
	msg  string
	raw  bool
}

func (e *cliError) Error() string { return e.msg }

// errHelpShown: a command printed its help; the process exits 0.
var errHelpShown = errors.New("help shown")

const exitUsage = 2

func usageError(format string, args ...any) error {
	return &cliError{code: exitUsage, msg: fmt.Sprintf(format, args...)}
}

// exitCode is the process exit code for the error run returned.
func exitCode(err error) int {
	var ce *cliError
	if errors.As(err, &ce) {
		return ce.code
	}
	return 1
}

// reportError prints err as main does; it is separate for tests.
func reportError(w io.Writer, err error) {
	var ce *cliError
	if errors.As(err, &ce) {
		if ce.msg == "" {
			return
		}
		if ce.raw {
			fmt.Fprintln(w, strings.TrimRight(ce.msg, "\n"))
			return
		}
	}
	fmt.Fprintln(w, "kairo:", err)
}

func isHelpArg(arg string) bool {
	switch arg {
	case "help", "-h", "--help", "-help", "-?", "/?":
		return true
	}
	return false
}

// command is one entry of a usage text.
type command struct {
	name    string
	args    string
	summary string
}

// commandGroup is a top-level command with subcommands.
type commandGroup struct {
	name    string
	summary string
	note    string // printed under the usage line
	subs    []command
	// aliases maps names people type for a subcommand to a suggestion.
	aliases map[string]string
}

var topCommands = []command{
	{"project", "", "declare (apply), inspect (status) and watch projects"},
	{"task", "", "list, show, wait for, read logs of, pause, cancel or rerun tasks"},
	{"execution", "", "submit, list, show, withdraw executions; their logs"},
	{"logs", "", "print or follow one attempt's log by ATTEMPT_ID (task logs finds the attempt)"},
	{"queue", "", "pause or resume a queue"},
	{"resource", "", "resource status, quarantine, enable, reconcile"},
	{"node", "", "quarantine or release nodes"},
	{"doctor", "", "check the API URL, token and versions"},
	{"version", "", "print the CLI version and the daemon's"},
	{"serve", "", "run the daemon (--config DAEMON.toml)"},
	{"agent", "", "run this host as a remote node (--config AGENT.toml)"},
	{"token", "", "create, list or revoke API tokens (direct database access)"},
	{"tls", "", "create the CA and the server certificate"},
	{"db", "", "database maintenance (compact)"},
	{"help", "", "help for a command: kairo help task logs"},
}

var topAliases = map[string]string{
	"tasks": "task", "projects": "project", "executions": "execution", "exec": "execution",
	"queues": "queue", "resources": "resource", "nodes": "node", "tokens": "token",
	"log": "logs (or kairo task logs)", "status": "project status", "ls": "task list",
	"list": "task list", "wait": "task wait", "watch": "project watch", "apply": "project apply",
	"show": "task show", "cancel": "task cancel", "rerun": "task rerun", "-v": "version",
	"--version": "version", "health": "doctor", "daemon": "serve", "server": "serve",
}

const remoteNote = "API commands take --api URL (default $KAIRO_API, else " + defaultAPIURL + "); the token comes from\n" +
	"$KAIRO_TOKEN, $KAIRO_TOKEN_FILE or <user config dir>/kairo/token, the CA from $KAIRO_CA_FILE or <user config dir>/kairo/ca.pem."

var groups = []*commandGroup{
	{
		name: "task", summary: "tasks of a project",
		note: "QUEUE may be left out for tasks declared by a ProjectSpec: it is looked up.",
		subs: []command{
			{"list", "PROJECT [--state S,..] [--match PREFIX] [--live] [--since D] [--all] [--json]", "tasks as a table (default: live and updated in the last 24h)"},
			{"show", "PROJECT [QUEUE] TASK [--json]", "one task: spec, dependencies, executions, attempts, log paths"},
			{"logs", "PROJECT [QUEUE] TASK [--tail N] [-f] [--stream S] [--attempt A] [--rank R]", "print or follow attempt logs"},
			{"wait", "PROJECT [QUEUE] TASK [TASK...] | PROJECT --match PREFIX [--until terminal|started] [--timeout D]", "wait for tasks, one line per state change (exit 0 ok, 1 failed, 2 timeout, 3 error)"},
			{"pause", "PROJECT [QUEUE] TASK [--force [--grace D] [--reason R]] [--no-wait]", "checkpoint and pause a task, or end it with --force"},
			{"resume", "PROJECT [QUEUE] TASK", "resume a paused task"},
			{"cancel", "PROJECT [QUEUE] TASK [--dry-run]", "cancel a task that is not running (pause --force a running one)"},
			{"rerun", "PROJECT [QUEUE] TASK", "run a failed, blocked, stopped or cancelled task again"},
		},
		aliases: map[string]string{
			"get": "show", "describe": "show", "info": "show", "inspect": "show", "status": "show", "detail": "show",
			"ls": "list", "ps": "list", "all": "list",
			"log": "logs", "tail": "logs", "output": "logs", "follow": "logs",
			"stop": "cancel (or pause --force for a running task)", "kill": "pause --force", "abort": "cancel",
			"delete": "cancel", "rm": "cancel", "remove": "cancel",
			"retry": "rerun", "restart": "rerun", "run": "rerun",
			"watch": "wait (or kairo project watch)", "await": "wait", "monitor": "wait",
			"suspend": "pause", "continue": "resume", "unpause": "resume",
		},
	},
	{
		name: "project", summary: "projects (ProjectSpec files)",
		subs: []command{
			{"validate", "FILE [--json]", "check a ProjectSpec file locally"},
			{"apply", "FILE [--json]", "declare a ProjectSpec's tasks (additive; declared tasks are immutable)"},
			{"status", "PROJECT [--task T]... [--match PREFIX] [--state S,..] [--live] [--since D]", "project status as JSON"},
			{"watch", "PROJECT [--match PREFIX] [--interval D] [--exit-when-idle]", "stream state and progress changes, one line each"},
			{"prune", "FILE [--write]", "keep only the tasks of FILE that can still run"},
			{"pause", "PROJECT [--force [--grace D] [--reason R]] [--no-wait]", "pause every task of the project"},
			{"resume", "PROJECT", "resume the project"},
		},
		aliases: map[string]string{
			"get": "status", "show": "status", "describe": "status", "info": "status",
			"ls": "status (or kairo task list)", "list": "status (or kairo task list)",
			"check": "validate", "lint": "validate", "submit": "apply", "declare": "apply", "create": "apply", "up": "apply",
			"monitor": "watch", "follow": "watch", "wait": "watch (or kairo task wait)",
		},
	},
	{
		name: "execution", summary: "executions (single process requests)",
		subs: []command{
			{"submit", "FILE", "submit an execution TOML file"},
			{"list", "[--project P] [--queue Q] [--task T] [--state S] [--limit N]", "list executions (JSON)"},
			{"show", "EXECUTION_ID", "one execution (JSON)"},
			{"withdraw", "EXECUTION_ID", "withdraw an execution that has not started"},
			{"logs", "EXECUTION_ID [--tail N] [-f] [--stream S] [--attempt A]", "print or follow the logs of an execution's attempts"},
		},
		aliases: map[string]string{
			"ls": "list", "get": "show", "describe": "show", "status": "show",
			"cancel": "withdraw", "stop": "withdraw", "rm": "withdraw", "delete": "withdraw",
			"log": "logs", "tail": "logs",
		},
	},
	{
		name: "queue", summary: "queues of a project",
		subs: []command{
			{"pause", "PROJECT QUEUE [--force [--grace D] [--reason R]] [--no-wait]", "pause every task of the queue"},
			{"resume", "PROJECT QUEUE", "resume the queue"},
		},
		aliases: map[string]string{"suspend": "pause", "continue": "resume", "unpause": "resume"},
	},
	{
		name: "resource", summary: "GPUs, CPU, RAM and disks",
		subs: []command{
			{"status", "", "resources and their state (JSON)"},
			{"quarantine", "RESOURCE_ID [--reason R]", "stop placing work on a resource"},
			{"enable", "RESOURCE_ID", "release a quarantined resource"},
			{"reconcile", "RESOURCE_ID --confirm-process-absent", "clear a resource held by stale attempts"},
		},
		aliases: map[string]string{"ls": "status", "list": "status", "show": "status", "release": "enable"},
	},
	{
		name: "node", summary: "nodes",
		subs: []command{
			{"quarantine", "NODE_ID|--all [--reason R]", "suspend or end the work on a node"},
			{"release", "NODE_ID|--all", "end a node quarantine"},
			{"quarantine-status", "", "current node quarantines (JSON)"},
		},
		aliases: map[string]string{"status": "quarantine-status", "ls": "quarantine-status", "list": "quarantine-status", "enable": "release"},
	},
	{
		name: "db", summary: "database maintenance (stop the daemon first)",
		subs: []command{
			{"compact", "(--config DAEMON.toml | --db PATH)", "prune resource observations and VACUUM the database"},
		},
		aliases: map[string]string{"vacuum": "compact"},
	},
	{
		name: "token", summary: "API tokens, written directly to the daemon's database",
		subs: []command{
			{"create", "(--config DAEMON.toml | --db PATH) --name NAME --role admin|read|node [--node NODE_ID] [--save | --out FILE] [--force]", "create a token"},
			{"list", "(--config DAEMON.toml | --db PATH)", "list active tokens"},
			{"revoke", "(--config DAEMON.toml | --db PATH) ID_OR_NAME", "revoke a token"},
		},
		aliases: map[string]string{"new": "create", "add": "create", "ls": "list", "rm": "revoke", "delete": "revoke"},
	},
	{
		name: "tls", summary: "TLS material for the daemon",
		subs: []command{
			{"init", "--dir DIR --host HOST [--host HOST ...]", "create a CA and a server certificate"},
			{"issue", "--dir DIR --host HOST [--host HOST ...]", "re-issue the server certificate from the existing CA"},
		},
	},
}

// leafCommands are top-level commands without subcommands.
var leafCommands = map[string]command{
	"serve":   {"serve", "--config DAEMON.toml", "run the daemon"},
	"agent":   {"agent", "--config AGENT.toml", "run this host as a remote node: its providers and executors report to the daemon"},
	"doctor":  {"doctor", "[--api URL]", "check the API URL and token, show both versions and the resources"},
	"version": {"version", "[--api URL]", "print the CLI version and, when reachable, the daemon's"},
	"logs":    {"logs", "ATTEMPT_ID [--stream stdout|stderr] [--tail BYTES] [-f]", "print an attempt's log (default stderr, whole), or follow it; kairo task logs finds the attempt of a task"},
}

func findGroup(name string) *commandGroup {
	for _, g := range groups {
		if g.name == name {
			return g
		}
	}
	return nil
}

func (g *commandGroup) find(name string) *command {
	for i := range g.subs {
		if g.subs[i].name == name {
			return &g.subs[i]
		}
	}
	return nil
}

func topUsage() string {
	var b strings.Builder
	b.WriteString("usage: kairo <command> [args]    (kairo help <command>, kairo <command> <sub> --help)\n\ncommands:\n")
	for _, c := range topCommands {
		fmt.Fprintf(&b, "  %-10s %s\n", c.name, c.summary)
	}
	b.WriteString("\n" + remoteNote + "\n")
	return b.String()
}

func (g *commandGroup) usage() string {
	var b strings.Builder
	names := make([]string, len(g.subs))
	for i, s := range g.subs {
		names[i] = s.name
	}
	fmt.Fprintf(&b, "usage: kairo %s <%s> ...    (kairo %s <command> --help for flags)\n", g.name, strings.Join(names, "|"), g.name)
	if g.note != "" {
		b.WriteString(g.note + "\n")
	}
	b.WriteString("\n")
	for _, s := range g.subs {
		fmt.Fprintf(&b, "  %s\n      %s\n", strings.TrimSpace(s.name+" "+s.args), s.summary)
	}
	return b.String()
}

// unknownCommand is the error for a mistyped command in group g (nil: top level).
func unknownCommand(g *commandGroup, name string) error {
	var known []string
	var aliases map[string]string
	text := topUsage()
	where := "command"
	if g != nil {
		for _, s := range g.subs {
			known = append(known, s.name)
		}
		aliases, text, where = g.aliases, g.usage(), g.name+" command"
	} else {
		for _, c := range topCommands {
			known = append(known, c.name)
		}
		aliases = topAliases
	}
	msg := fmt.Sprintf("kairo: unknown %s %q", where, name)
	if suggestion := suggest(name, known, aliases); suggestion != "" {
		prefix := "kairo "
		if g != nil {
			prefix += g.name + " "
		}
		msg += fmt.Sprintf("; did you mean `%s%s`?", prefix, suggestion)
	}
	return &cliError{code: exitUsage, msg: msg + "\n" + text, raw: true}
}

// missingCommand is the error for a group given without a subcommand.
func missingCommand(g *commandGroup) error {
	return &cliError{code: exitUsage, msg: g.usage(), raw: true}
}

// suggest returns the alias for name, else the closest known name within
// edit distance 2, else "".
func suggest(name string, known []string, aliases map[string]string) string {
	lower := strings.ToLower(name)
	if s, ok := aliases[lower]; ok {
		return s
	}
	best, bestDistance := "", 3
	for _, k := range known {
		if strings.HasPrefix(k, lower) && len(lower) >= 2 {
			return k
		}
		if d := editDistance(lower, k); d < bestDistance && d < len(k) {
			best, bestDistance = k, d
		}
	}
	return best
}

func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

// dispatch runs the subcommand args[0] of g.
func dispatch(g *commandGroup, args []string, handlers map[string]func([]string) error) error {
	if len(args) == 0 {
		return missingCommand(g)
	}
	if isHelpArg(args[0]) {
		if len(args) > 1 {
			if _, ok := handlers[args[1]]; ok {
				return handlers[args[1]]([]string{"--help"})
			}
		}
		fmt.Fprint(os.Stdout, g.usage())
		return errHelpShown
	}
	handler, ok := handlers[args[0]]
	if !ok {
		return unknownCommand(g, args[0])
	}
	return handler(args[1:])
}

// helpCommand: kairo help [COMMAND [SUB]].
func helpCommand(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stdout, topUsage())
		return errHelpShown
	}
	if _, ok := leafCommands[args[0]]; ok {
		return run([]string{args[0], "--help"})
	}
	if findGroup(args[0]) == nil {
		return unknownCommand(nil, args[0])
	}
	return run(append([]string{args[0], "help"}, args[1:]...))
}

// newFlags makes the flag set of command "group sub" (or a leaf command):
// --help prints its usage and flags to stdout; parse errors are returned,
// not printed.
func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	return fs
}

// commandSynopsis is "kairo NAME ARGS" and the summary of a command.
func commandSynopsis(name string) (string, string) {
	if leaf, ok := leafCommands[name]; ok {
		return "kairo " + name + " " + leaf.args, leaf.summary
	}
	group, sub, _ := strings.Cut(name, " ")
	if g := findGroup(group); g != nil {
		if c := g.find(sub); c != nil {
			return strings.TrimSpace("kairo " + name + " " + c.args), c.summary
		}
	}
	return "kairo " + name, ""
}

func printFlagHelp(fs *flag.FlagSet) {
	synopsis, summary := commandSynopsis(fs.Name())
	w := os.Stdout
	fmt.Fprintf(w, "usage: %s\n", synopsis)
	if summary != "" {
		fmt.Fprintf(w, "%s\n", summary)
	}
	if note := commandNotes[fs.Name()]; note != "" {
		fmt.Fprintf(w, "\n%s\n", note)
	}
	hasFlags := false
	fs.VisitAll(func(*flag.Flag) { hasFlags = true })
	if hasFlags {
		fmt.Fprintln(w, "\nflags:")
		fs.SetOutput(w)
		fs.PrintDefaults()
		fs.SetOutput(io.Discard)
	}
}

// commandNotes are details printed by a command's --help.
var commandNotes = map[string]string{
	"task wait": "Prints \"HH:MM:SS TASK STATE\" on first sight and \"HH:MM:SS TASK OLD -> NEW [note]\" on every change.\n" +
		"Exit codes: 0 all reached the target (and, for terminal, succeeded); 1 all ended but some did not\n" +
		"succeed; 2 timeout; 3 usage error or task/project not found. API outages print DOWN/UP and are retried.",
	"project watch": "Prints state changes, progress (at most once a minute per task) and a summary every 10 minutes.\n" +
		"With --exit-when-idle: exit 0 once nothing is pending or running, 1 if a task seen ending did not succeed.",
	"task logs": "Default: the last 50 lines of stdout and stderr of the latest execution's attempt.\n" +
		"--attempt all prints every attempt; for a gang, --rank selects the rank (default 0).",
	"task list":     "Columns: NAME QUEUE STATE EXEC (latest execution) STARTED ELAPSED NOTE.",
	"project apply": "A declared task is immutable: to change one give it a new name; `kairo task rerun` runs it again.",
}

// parseFlags parses args interleaving flags and positionals; on --help it
// prints the command's usage and returns errHelpShown.
func parseFlags(fs *flag.FlagSet, args []string) ([]string, error) {
	positional, err := parseInterleaved(fs, args)
	if errors.Is(err, flag.ErrHelp) {
		printFlagHelp(fs)
		return nil, errHelpShown
	}
	if err != nil {
		synopsis, _ := commandSynopsis(fs.Name())
		return nil, usageError("%v\nusage: %s    (kairo %s --help)", err, synopsis, fs.Name())
	}
	return positional, nil
}
