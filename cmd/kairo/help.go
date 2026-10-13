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

// command is a subcommand of a group: its usage and its handler.
type command struct {
	name    string
	args    string
	summary string
	run     func([]string) error
}

// commandGroup is a top-level command with subcommands.
type commandGroup struct {
	name string
	note string // printed under the usage line
	subs []command
	// aliases maps names people type for a subcommand to a suggestion.
	aliases map[string]string
}

// topCommand is a command of kairo: a group of subcommands or a leaf
// command (args and help are a leaf's synopsis and --help summary).
type topCommand struct {
	name    string
	summary string // its line in kairo's usage
	args    string
	help    string
	run     func([]string) error // a group's dispatches to its subcommands
	group   *commandGroup
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

// topCommands are kairo's commands, in the order of its usage, with their
// subcommands and handlers. It is set in init: the handlers read it back.
var topCommands []*topCommand

// bind is f with its first argument (the subcommand's action) given.
func bind(f func(string, []string) error, action string) func([]string) error {
	return func(args []string) error { return f(action, args) }
}

// scope is the pause or resume of a project, a queue or a task.
func scope(kind, action string) func([]string) error {
	return func(args []string) error { return scopeAction(kind, action, args) }
}

func init() {
	topCommands = []*topCommand{
		{name: "project", summary: "declare (apply), inspect (status) and watch projects", group: &commandGroup{
			subs: []command{
				{"validate", "FILE [--json]", "check a ProjectSpec file locally", projectValidate},
				{"apply", "FILE [--json]", "declare a ProjectSpec's tasks (additive; declared tasks are immutable)", projectApply},
				{"status", "PROJECT [--task T]... [--match PREFIX] [--state S,..] [--live] [--since D]", "project status as JSON", projectStatusCommand},
				{"watch", "PROJECT [--match PREFIX] [--interval D] [--exit-when-idle]", "stream state and progress changes, one line each", projectWatch},
				{"prune", "FILE [--write]", "keep only the tasks of FILE that can still run", projectPrune},
				{"pause", "PROJECT [--force [--grace D] [--reason R]] [--no-wait]", "pause every task of the project", scope("project", "pause")},
				{"resume", "PROJECT", "resume the project", scope("project", "resume")},
			},
			aliases: map[string]string{
				"get": "status", "show": "status", "describe": "status", "info": "status",
				"ls": "status (or kairo task list)", "list": "status (or kairo task list)",
				"check": "validate", "lint": "validate", "submit": "apply", "declare": "apply", "create": "apply", "up": "apply",
				"monitor": "watch", "follow": "watch", "wait": "watch (or kairo task wait)",
			},
		}},
		{name: "task", summary: "list, show, wait for, read logs of, pause, cancel or rerun tasks", group: &commandGroup{
			note: "QUEUE may be left out for tasks declared by a ProjectSpec: it is looked up.",
			subs: []command{
				{"list", "PROJECT [--state S,..] [--match PREFIX] [--live] [--since D] [--all] [--json]", "tasks as a table (default: live and updated in the last 24h)", taskList},
				{"show", "PROJECT [QUEUE] TASK [--json]", "one task: spec, dependencies, executions, attempts, log paths", taskShow},
				{"logs", "PROJECT [QUEUE] TASK [--tail N] [-f] [--stream S] [--attempt A] [--rank R]", "print or follow attempt logs", taskLogs},
				{"wait", "PROJECT [QUEUE] TASK [TASK...] | PROJECT --match PREFIX [--until terminal|started] [--timeout D]", "wait for tasks, one line per state change (exit 0 ok, 1 failed, 2 timeout, 3 error)", taskWait},
				{"pause", "PROJECT [QUEUE] TASK [--force [--grace D] [--reason R]] [--no-wait]", "checkpoint and pause a task, or end it with --force", scope("task", "pause")},
				{"resume", "PROJECT [QUEUE] TASK", "resume a paused task", scope("task", "resume")},
				{"cancel", "PROJECT [QUEUE] TASK [--dry-run]", "cancel a task that is not running (pause --force a running one)", taskCancel},
				{"rerun", "PROJECT [QUEUE] TASK", "run a failed, blocked, stopped or cancelled task again", taskRerun},
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
		}},
		{name: "execution", summary: "submit, list, show, withdraw executions; their logs", group: &commandGroup{
			subs: []command{
				{"submit", "FILE", "submit an execution TOML file", executionSubmit},
				{"list", "[--project P] [--queue Q] [--task T] [--state S] [--limit N]", "list executions (JSON)", executionList},
				{"show", "EXECUTION_ID", "one execution (JSON)", bind(executionByID, "show")},
				{"withdraw", "EXECUTION_ID", "withdraw an execution that has not started", bind(executionByID, "withdraw")},
				{"logs", "EXECUTION_ID [--tail N] [-f] [--stream S] [--attempt A]", "print or follow the logs of an execution's attempts", executionLogs},
			},
			aliases: map[string]string{
				"ls": "list", "get": "show", "describe": "show", "status": "show",
				"cancel": "withdraw", "stop": "withdraw", "rm": "withdraw", "delete": "withdraw",
				"log": "logs", "tail": "logs",
			},
		}},
		{name: "logs", summary: "print or follow one attempt's log by ATTEMPT_ID (task logs finds the attempt)", run: logsCommand,
			args: "ATTEMPT_ID [--stream stdout|stderr] [--tail BYTES] [-f]",
			help: "print an attempt's log (default stderr, whole), or follow it; kairo task logs finds the attempt of a task"},
		{name: "queue", summary: "pause or resume a queue", group: &commandGroup{
			subs: []command{
				{"pause", "PROJECT QUEUE [--force [--grace D] [--reason R]] [--no-wait]", "pause every task of the queue", scope("queue", "pause")},
				{"resume", "PROJECT QUEUE", "resume the queue", scope("queue", "resume")},
			},
			aliases: map[string]string{"suspend": "pause", "continue": "resume", "unpause": "resume"},
		}},
		{name: "resource", summary: "resource status, quarantine, enable, reconcile", group: &commandGroup{
			subs: []command{
				{"status", "", "resources and their state (JSON)", bind(resourceAction, "status")},
				{"quarantine", "RESOURCE_ID [--reason R]", "stop placing work on a resource", bind(resourceAction, "quarantine")},
				{"enable", "RESOURCE_ID", "release a quarantined resource", bind(resourceAction, "enable")},
				{"reconcile", "RESOURCE_ID --confirm-process-absent", "clear a resource held by stale attempts", bind(resourceAction, "reconcile")},
			},
			aliases: map[string]string{"ls": "status", "list": "status", "show": "status", "release": "enable"},
		}},
		{name: "node", summary: "quarantine or release nodes", group: &commandGroup{
			subs: []command{
				{"quarantine", "NODE_ID|--all [--reason R]", "suspend or end the work on a node", bind(nodeAction, "quarantine")},
				{"release", "NODE_ID|--all", "end a node quarantine", bind(nodeAction, "release")},
				{"quarantine-status", "", "current node quarantines (JSON)", bind(nodeAction, "quarantine-status")},
			},
			aliases: map[string]string{"status": "quarantine-status", "ls": "quarantine-status", "list": "quarantine-status", "enable": "release"},
		}},
		{name: "doctor", summary: "check the API URL, token and versions", run: doctorCommand,
			args: "[--api URL]", help: "check the API URL and token, show both versions and the resources"},
		{name: "version", summary: "print the CLI version and the daemon's", run: versionCommand,
			args: "[--api URL]", help: "print the CLI version and, when reachable, the daemon's"},
		{name: "serve", summary: "run the daemon (--config DAEMON.toml)", run: serve,
			args: "--config DAEMON.toml", help: "run the daemon"},
		{name: "agent", summary: "run this host as a remote node (--config AGENT.toml)", run: agentCommand,
			args: "--config AGENT.toml", help: "run this host as a remote node: its providers and executors report to the daemon"},
		{name: "token", summary: "create, list or revoke API tokens (direct database access)", group: &commandGroup{
			subs: []command{
				{"create", "(--config DAEMON.toml | --db PATH) --name NAME --role admin|read|node [--node NODE_ID] [--save | --out FILE] [--force]", "create a token", bind(tokenAction, "create")},
				{"list", "(--config DAEMON.toml | --db PATH)", "list active tokens", bind(tokenAction, "list")},
				{"revoke", "(--config DAEMON.toml | --db PATH) ID_OR_NAME", "revoke a token", bind(tokenAction, "revoke")},
			},
			aliases: map[string]string{"new": "create", "add": "create", "ls": "list", "rm": "revoke", "delete": "revoke"},
		}},
		{name: "tls", summary: "create the CA and the server certificate", group: &commandGroup{
			subs: []command{
				{"init", "--dir DIR --host HOST [--host HOST ...]", "create a CA and a server certificate", bind(tlsAction, "init")},
				{"issue", "--dir DIR --host HOST [--host HOST ...]", "re-issue the server certificate from the existing CA", bind(tlsAction, "issue")},
			},
		}},
		{name: "db", summary: "database maintenance (compact)", group: &commandGroup{
			subs: []command{
				{"compact", "(--config DAEMON.toml | --db PATH)", "prune resource observations and VACUUM the database", dbCompact},
			},
			aliases: map[string]string{"vacuum": "compact"},
		}},
		{name: "help", summary: "help for a command: kairo help task logs", run: helpCommand},
	}
	for _, c := range topCommands {
		if g := c.group; g != nil {
			g.name = c.name
			c.run = func(args []string) error { return dispatch(g, args) }
		}
	}
}

// findCommand is the top-level command name, nil if there is none.
func findCommand(name string) *topCommand {
	for _, c := range topCommands {
		if c.name == name {
			return c
		}
	}
	return nil
}

// findLeaf is the leaf command name (one with its own --help), nil if there is none.
func findLeaf(name string) *topCommand {
	if c := findCommand(name); c != nil && c.group == nil && c.help != "" {
		return c
	}
	return nil
}

func findGroup(name string) *commandGroup {
	if c := findCommand(name); c != nil {
		return c.group
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
func dispatch(g *commandGroup, args []string) error {
	if len(args) == 0 {
		return missingCommand(g)
	}
	if isHelpArg(args[0]) {
		if len(args) > 1 {
			if c := g.find(args[1]); c != nil {
				return c.run([]string{"--help"})
			}
		}
		fmt.Fprint(os.Stdout, g.usage())
		return errHelpShown
	}
	c := g.find(args[0])
	if c == nil {
		return unknownCommand(g, args[0])
	}
	return c.run(args[1:])
}

// helpCommand: kairo help [COMMAND [SUB]].
func helpCommand(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stdout, topUsage())
		return errHelpShown
	}
	if findLeaf(args[0]) != nil {
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
	if leaf := findLeaf(name); leaf != nil {
		return "kairo " + name + " " + leaf.args, leaf.help
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
