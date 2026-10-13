package main

import (
	"net/http"
	"net/url"
	"os"
)

// nodeCommand: kairo node <quarantine|release> <NODE_ID|--all>, kairo node quarantine-status.
func nodeCommand(args []string) error {
	handler := func(action string) func([]string) error {
		return func(a []string) error { return nodeAction(action, a) }
	}
	return dispatch(findGroup("node"), args, map[string]func([]string) error{
		"quarantine": handler("quarantine"), "release": handler("release"), "quarantine-status": handler("quarantine-status"),
	})
}

func nodeAction(action string, args []string) error {
	fs := newFlags("node " + action)
	api := apiFlag(fs)
	var all *bool
	var reason, actor *string
	if action != "quarantine-status" {
		all = fs.Bool("all", false, "every node")
		actor = fs.String("actor", defaultActor(), "who acts")
	}
	if action == "quarantine" {
		reason = fs.String("reason", "operator request", "quarantine reason")
	}
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if action == "quarantine-status" {
		if len(positional) != 0 {
			return usageError("node quarantine-status accepts no arguments")
		}
		client, err := newAPIClient(*api)
		if err != nil {
			return err
		}
		return client.print(http.MethodGet, "/api/nodes/quarantines", nil)
	}
	node := "all"
	switch {
	case *all && len(positional) == 0:
	case !*all && len(positional) == 1:
		node = positional[0]
	default:
		return usageError("node %s takes exactly one of NODE_ID or --all", action)
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
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
