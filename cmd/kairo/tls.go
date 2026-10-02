package main

import (
	"errors"
	"flag"
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
	const usageText = "usage: kairo tls <init|issue> --dir DIR --host HOST [--host HOST ...]"
	if len(args) == 0 {
		return errors.New(usageText)
	}
	action := args[0]
	if action != "init" && action != "issue" {
		return fmt.Errorf("unknown tls command %q; %s", action, usageText)
	}
	fs := flag.NewFlagSet("tls "+action, flag.ContinueOnError)
	dir := fs.String("dir", "", "directory holding the TLS material")
	var hosts stringList
	fs.Var(&hosts, "host", "server IP address or DNS name (repeatable)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("tls %s takes no positional arguments; %s", action, usageText)
	}
	if *dir == "" {
		return fmt.Errorf("tls %s requires --dir", action)
	}
	if len(hosts) == 0 {
		return fmt.Errorf("tls %s requires at least one --host", action)
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
