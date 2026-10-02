package main

import (
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
)

// nodeCommand: kairo node <quarantine|release> <NODE_ID|--all>, kairo node quarantine-status.
func nodeCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: kairo node <quarantine|release> <NODE_ID|--all> | kairo node quarantine-status")
	}
	action := args[0]
	fs := flag.NewFlagSet("node "+action, flag.ContinueOnError)
	api := apiFlag(fs)
	all := fs.Bool("all", false, "every node")
	reason := fs.String("reason", "operator request", "quarantine reason")
	actor := fs.String("actor", defaultActor(), "who acts")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if action != "quarantine" && action != "release" && action != "quarantine-status" {
		return fmt.Errorf("unknown node command %q", action)
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	if action == "quarantine-status" {
		return client.print(http.MethodGet, "/api/nodes/quarantines", nil)
	}
	node := "all"
	switch {
	case *all && fs.NArg() == 0:
	case !*all && fs.NArg() == 1:
		node = fs.Arg(0)
	default:
		return fmt.Errorf("node %s takes exactly one of NODE_ID or --all", action)
	}
	path := "/api/nodes/" + url.PathEscape(node) + "/quarantine"
	if action == "quarantine" {
		return client.print(http.MethodPost, path, map[string]string{"actor": *actor, "reason": *reason})
	}
	return client.print(http.MethodDelete, path, map[string]string{"actor": *actor})
}

func defaultActor() string {
	if u := os.Getenv("USERNAME"); u != "" {
		return u
	}
	if u := os.Getenv("USER"); u != "" {
		return u
	}
	return "operator"
}
