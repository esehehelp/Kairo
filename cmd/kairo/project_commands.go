package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"kairo/internal/orchestration"
)

// projectSpecCommand: kairo project validate|apply FILE, kairo project status PROJECT.
func projectSpecCommand(args []string) error {
	action := args[0]
	fs := flag.NewFlagSet("project "+action, flag.ContinueOnError)
	api := apiFlag(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if action == "status" {
		if fs.NArg() != 1 {
			return errors.New("project status requires PROJECT")
		}
		client, err := newAPIClient(*api)
		if err != nil {
			return err
		}
		return client.print(http.MethodGet, "/api/projects/"+url.PathEscape(fs.Arg(0)), nil)
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("project %s requires a project TOML file", action)
	}
	source, err := os.ReadFile(fs.Arg(0))
	if err != nil {
		return err
	}
	validated, err := orchestration.Parse(source)
	if err != nil {
		return err
	}
	if action == "validate" {
		body, err := json.MarshalIndent(map[string]any{
			"ok":          true,
			"project":     validated.Manifest.Project.Name,
			"spec_digest": validated.Digest,
			"tasks":       validated.Manifest.Tasks,
		}, "", "  ")
		if err != nil {
			return err
		}
		body = append(body, '\n')
		_, err = os.Stdout.Write(body)
		return err
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	return client.print(http.MethodPost, "/api/projects", validated.Manifest)
}
