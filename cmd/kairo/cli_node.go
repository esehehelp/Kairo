package main

import (
	"net/http"
	"net/url"
	"os"
)

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
