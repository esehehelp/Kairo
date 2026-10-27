package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"kairo/internal/bench"
)

func init() {
	var b strings.Builder
	b.WriteString("Writes JSON lines to stdout: a start record (parameters, host and device identity), an interval\n" +
		"record every --interval (work done, latency percentiles), and an end record (totals, reason).\n" +
		"SIGTERM or Ctrl-C ends the run with its end record and exit 0; a refused or failed run exits 1.\n\nworkloads (unit):\n")
	for _, w := range bench.Workloads() {
		fmt.Fprintf(&b, "  %-15s %s (%s)\n", w.Name, w.Summary, w.Unit)
	}
	b.WriteString("\nkairo bench run WORKLOAD --help lists the workload's flags.")
	commandNotes["bench run"] = b.String()
	commandNotes["bench serve"] = "The peer of net-latency and net-bandwidth: echoes, sinks or sources one test per connection,\n" +
		"one JSON line per connection on stdout. It serves anyone who connects: listen on a trusted network."
}

// signalContext ends on SIGTERM or Ctrl-C (CTRL_BREAK on Windows).
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// benchRun: kairo bench run WORKLOAD [flags].
func benchRun(args []string) error {
	if len(args) == 0 || isHelpArg(args[0]) || strings.HasPrefix(args[0], "-") {
		// No workload: --help shows the workloads; anything else is a usage error.
		cfg := bench.Config{Seconds: 60, Interval: 1}
		fs := newFlags("bench run")
		cfg.RegisterFlags(fs)
		if len(args) > 0 && isHelpArg(args[0]) {
			args = []string{"--help"}
		}
		if _, err := parseFlags(fs, args); err != nil {
			return err
		}
		return usageError("bench run needs a WORKLOAD    (kairo bench run --help lists them)")
	}
	cfg, err := bench.Defaults(args[0])
	if err != nil {
		return usageError("%v", err)
	}
	fs := newFlags("bench run " + cfg.Workload)
	cfg.RegisterFlags(fs)
	positional, err := parseInterleaved(fs, leafArgs(args[1:]))
	if errors.Is(err, flag.ErrHelp) {
		printWorkloadHelp(fs, cfg.Workload)
		return errHelpShown
	}
	if err != nil {
		return usageError("%v\nusage: kairo bench run %s [flags]    (kairo bench run %s --help)", err, cfg.Workload, cfg.Workload)
	}
	if len(positional) != 0 {
		return usageError("bench run takes one WORKLOAD, got also %q", positional)
	}
	if err := cfg.Validate(); err != nil {
		return usageError("%s: %v", cfg.Workload, err)
	}
	ctx, stop := signalContext()
	defer stop()
	return bench.Run(ctx, os.Stdout, cfg)
}

// printWorkloadHelp is kairo bench run WORKLOAD --help: the workload and its flags.
func printWorkloadHelp(fs *flag.FlagSet, name string) {
	w := os.Stdout
	fmt.Fprintf(w, "usage: kairo bench run %s [flags]\n", name)
	for _, wl := range bench.Workloads() {
		if wl.Name == name {
			fmt.Fprintf(w, "%s; unit: %s\n", wl.Summary, wl.Unit)
		}
	}
	fmt.Fprintln(w, "\nflags:")
	fs.SetOutput(w)
	fs.PrintDefaults()
	fs.SetOutput(io.Discard)
}

// benchServe: kairo bench serve --listen ADDR.
func benchServe(args []string) error {
	fs := newFlags("bench serve")
	listen := fs.String("listen", "", "HOST:PORT to listen on (required; port 0 picks one, printed in the first line)")
	idle := fs.Duration("idle-timeout", 5*time.Minute, "exit once no connection has been open for this long")
	maxConn := fs.Duration("max-conn-duration", time.Hour, "close a connection open for this long")
	positional, err := parseFlags(fs, leafArgs(args))
	if err != nil {
		return err
	}
	if *listen == "" || len(positional) != 0 {
		return usageError("bench serve requires --listen HOST:PORT and no arguments")
	}
	if *idle <= 0 || *maxConn <= 0 {
		return usageError("--idle-timeout and --max-conn-duration must be positive")
	}
	ctx, stop := signalContext()
	defer stop()
	return bench.Serve(ctx, os.Stdout, bench.ServeConfig{Listen: *listen, IdleTimeout: *idle, MaxConnDuration: *maxConn})
}
