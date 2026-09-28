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
	positional, err := parseInterspersed(fs, args[1:])
	if err != nil {
		return err
	}
	if action == "quarantine-status" {
		return request(*api, http.MethodGet, "/v2/node-quarantines", nil)
	}
	if action != "quarantine" && action != "release" {
		return fmt.Errorf("unknown node command %q", action)
	}
	var path string
	switch {
	case *all && len(positional) == 0:
		path = "/v2/node-quarantines/all"
	case !*all && len(positional) == 1:
		path = "/v2/nodes/" + url.PathEscape(positional[0]) + "/quarantine"
	default:
		return fmt.Errorf("node %s takes exactly one of NODE_ID or --all", action)
	}
	if action == "quarantine" {
		return request(*api, http.MethodPost, path, map[string]string{"actor": *actor, "reason": *reason})
	}
	return request(*api, http.MethodDelete, path, map[string]string{"actor": *actor})
}

// parseInterspersed parses flags placed before or after the positional
// arguments (the flag package alone stops at the first positional one) and
// returns the positional arguments. Everything after "--" is positional.
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			return positional, nil
		}
		if consumed := len(args) - len(rest); consumed > 0 && args[consumed-1] == "--" {
			return append(positional, rest...), nil
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
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
