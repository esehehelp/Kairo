package main

import (
	"fmt"
	"net/http"
	"os"

	"kairo/internal/buildinfo"
)

// versionCommand: kairo version prints the CLI's build and, when the daemon
// answers /health (no token needed), the daemon's.
func versionCommand(args []string) error {
	fs := newFlags("version")
	api := apiFlag(fs)
	positional, err := parseFlags(fs, leafArgs(args))
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return usageError("version accepts no arguments")
	}
	var health struct {
		Version string `json:"version"`
	}
	client, err := newAPIClient(*api)
	if err == nil {
		err = client.callJSON(http.MethodGet, "/health", nil, &health)
	}
	printVersions(health.Version, err)
	return nil
}

// printVersions prints the CLI's and the daemon's versions and warns on
// stderr when they differ.
func printVersions(daemon string, daemonErr error) {
	cli := buildinfo.Version()
	fmt.Printf("kairo cli %s\n", cli)
	switch {
	case daemonErr != nil:
		fmt.Printf("daemon unreachable: %v\n", daemonErr)
	case daemon == "":
		fmt.Println("daemon version unknown (it predates version reporting)")
		fmt.Fprintf(os.Stderr, "warning: %s\n", olderDaemonHint())
	default:
		fmt.Printf("daemon %s\n", daemon)
		if daemon != cli {
			fmt.Fprintf(os.Stderr, "warning: the CLI (%s) and the daemon (%s) come from different builds; restart the daemon from the same build if commands fail\n", cli, daemon)
		}
	}
}
