package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"kairo/internal/tlsutil"
)

// stringList is a repeatable string flag.
type stringList []string

func (l *stringList) String() string { return strings.Join(*l, ",") }

func (l *stringList) Set(value string) error {
	*l = append(*l, value)
	return nil
}

// tlsCommand: kairo tls init --dir DIR --host H [--host H ...] creates a CA and
// a server certificate; kairo tls issue --dir DIR --host H [...] re-issues the
// server certificate from the existing CA.
func tlsCommand(args []string) error {
	return dispatch(findGroup("tls"), args, map[string]func([]string) error{
		"init":  func(a []string) error { return tlsAction("init", a) },
		"issue": func(a []string) error { return tlsAction("issue", a) },
	})
}

func tlsAction(action string, args []string) error {
	fs := newFlags("tls " + action)
	dir := fs.String("dir", "", "directory holding the TLS material")
	var hosts stringList
	fs.Var(&hosts, "host", "server IP address or DNS name (repeatable)")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return usageError("tls %s takes no positional arguments", action)
	}
	if *dir == "" {
		return usageError("tls %s requires --dir", action)
	}
	if len(hosts) == 0 {
		return usageError("tls %s requires at least one --host", action)
	}
	var written []string
	if action == "init" {
		if err := tlsutil.InitCA(*dir); err != nil {
			return err
		}
		written = append(written, tlsutil.CACertFile, tlsutil.CAKeyFile)
	}
	if err := tlsutil.IssueServer(*dir, hosts); err != nil {
		return err
	}
	written = append(written, tlsutil.ServerCertFile, tlsutil.ServerKeyFile)
	for _, name := range written {
		fmt.Println(filepath.Join(*dir, name))
	}
	return nil
}
