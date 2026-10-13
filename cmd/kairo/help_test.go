package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestTopLevelHelpGoesToStdoutAndSucceeds(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"--help"}, {"-h"}} {
		out, err := captureStdout(t, func() error { return run(args) })
		if err != nil || !strings.HasPrefix(out, "usage: kairo <command>") || !strings.Contains(out, "task       list, show") {
			t.Fatalf("%v: %v %q", args, err, out)
		}
	}
}

func TestEveryGroupAndSubcommandHasHelp(t *testing.T) {
	isolateClientEnv(t)
	for _, g := range groups {
		for _, h := range []string{"help", "--help", "-h"} {
			out, err := captureStdout(t, func() error { return run([]string{g.name, h}) })
			if err != nil || !strings.HasPrefix(out, "usage: kairo "+g.name+" <") {
				t.Fatalf("%s %s: %v %q", g.name, h, err, out)
			}
			for _, s := range g.subs {
				if !strings.Contains(out, "\n  "+s.name) {
					t.Fatalf("%s usage does not list %s: %q", g.name, s.name, out)
				}
			}
		}
		for _, s := range g.subs {
			for _, h := range []string{"--help", "-h"} {
				out, err := captureStdout(t, func() error { return run([]string{g.name, s.name, h}) })
				if err != nil || !strings.HasPrefix(out, "usage: kairo "+g.name+" "+s.name) || !strings.Contains(out, s.summary) {
					t.Fatalf("%s %s %s: %v %q", g.name, s.name, h, err, out)
				}
			}
		}
	}
	for name := range leafCommands {
		for _, args := range [][]string{{name, "--help"}, {name, "-h"}, {name, "help"}, {"help", name}} {
			out, err := captureStdout(t, func() error { return run(args) })
			if err != nil || !strings.HasPrefix(out, "usage: kairo "+name) || !strings.Contains(out, "flags:") {
				t.Fatalf("%v: %v %q", args, err, out)
			}
		}
	}
	out, err := captureStdout(t, func() error { return run([]string{"help", "task", "logs"}) })
	if err != nil || !strings.HasPrefix(out, "usage: kairo task logs PROJECT [QUEUE] TASK") || !strings.Contains(out, "-tail") {
		t.Fatalf("help task logs: %v %q", err, out)
	}
	out, err = captureStdout(t, func() error { return run([]string{"task", "wait", "--help"}) })
	if err != nil || !strings.Contains(out, "2 timeout") {
		t.Fatalf("task wait --help documents its exit codes: %v %q", err, out)
	}
}

func TestMistypedCommandsSuggestTheRightOne(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"task", "get", "p", "t"}, "did you mean `kairo task show`"},
		{[]string{"task", "ls"}, "did you mean `kairo task list`"},
		{[]string{"task", "lsit"}, "did you mean `kairo task list`"},
		{[]string{"task", "log"}, "did you mean `kairo task logs`"},
		{[]string{"task", "stop"}, "did you mean `kairo task cancel (or pause --force for a running task)`"},
		{[]string{"project", "stauts"}, "did you mean `kairo project status`"},
		{[]string{"execution", "ls"}, "did you mean `kairo execution list`"},
		{[]string{"tasks"}, "did you mean `kairo task`"},
		{[]string{"log"}, "did you mean `kairo logs (or kairo task logs)`"},
	} {
		out, err := captureStdout(t, func() error { return run(c.args) })
		if err == nil || exitCode(err) != exitUsage || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%v: %v", c.args, err)
		}
		if out != "" {
			t.Fatalf("%v: usage errors go to stderr, stdout got %q", c.args, out)
		}
		// The group usage follows the suggestion.
		if !strings.Contains(err.Error(), "usage: kairo") {
			t.Fatalf("%v: no usage in %q", c.args, err)
		}
	}
	if err := run([]string{"task", "frobnicate"}); err == nil || strings.Contains(err.Error(), "did you mean") {
		t.Fatalf("no suggestion expected: %v", err)
	}
}

func TestBadFlagIsAUsageError(t *testing.T) {
	err := run([]string{"task", "logs", "--bogus", "p", "t"})
	if err == nil || exitCode(err) != exitUsage || !strings.Contains(err.Error(), "-bogus") || !strings.Contains(err.Error(), "kairo task logs --help") {
		t.Fatalf("bad flag: %v", err)
	}
	var stderr bytes.Buffer
	reportError(&stderr, err)
	if !strings.HasPrefix(stderr.String(), "kairo: flag provided but not defined") {
		t.Fatalf("reported: %q", stderr.String())
	}
}

func TestQueueIsOptionalInUsage(t *testing.T) {
	for _, sub := range []string{"pause", "resume", "cancel", "rerun", "show", "logs"} {
		err := run([]string{"task", sub, "--api", "http://127.0.0.1:1", "p"})
		if err == nil || !strings.Contains(err.Error(), "PROJECT [QUEUE] TASK") || exitCode(err) != exitUsage {
			t.Fatalf("task %s with one argument: %v", sub, err)
		}
	}
}
