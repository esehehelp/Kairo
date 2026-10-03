package main

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kairo/internal/conformance"
	"kairo/internal/tlsutil"
)

// The operator client follows the cross-language contract in sdk/conformance.

func TestLoopbackConformance(t *testing.T) {
	var fixture struct {
		Cases []struct {
			Host     string `json:"host"`
			Loopback bool   `json:"loopback"`
		} `json:"cases"`
	}
	conformance.Load(t, "loopback.json", &fixture)
	for _, c := range fixture.Cases {
		if got := isLoopback(c.Host); got != c.Loopback {
			t.Errorf("isLoopback(%q) = %v, want %v", c.Host, got, c.Loopback)
		}
	}
}

func TestTokenTransportConformance(t *testing.T) {
	var fixture struct {
		Cases []struct {
			URL     string `json:"url"`
			Allowed bool   `json:"allowed"`
		} `json:"cases"`
	}
	conformance.Load(t, "token_transport.json", &fixture)
	isolateClientEnv(t)
	t.Setenv("KAIRO_TOKEN", testToken)
	for _, c := range fixture.Cases {
		parsed, err := url.Parse(c.URL)
		got := err == nil && tokenTransportAllowed(parsed)
		if got != c.Allowed {
			t.Errorf("token to %q allowed = %v, want %v", c.URL, got, c.Allowed)
		}
		// newAPIClient with a token refuses exactly the URLs a token may not
		// travel to.
		if _, err = newAPIClient(c.URL); (err == nil) != c.Allowed {
			t.Errorf("newAPIClient(%q) with a token: %v, want allowed=%v", c.URL, err, c.Allowed)
		}
	}
}

// operatorEnv is every variable operator_resolution.json may set; a case
// starts with all of them unset.
var operatorEnv = []string{"KAIRO_API", "KAIRO_TOKEN", "KAIRO_TOKEN_FILE", "KAIRO_CA_FILE", "KAIRO_API_URL", "KAIRO_ATTEMPT_TOKEN"}

func TestOperatorResolutionConformance(t *testing.T) {
	var fixture struct {
		Cases []struct {
			Name   string            `json:"name"`
			Arg    map[string]string `json:"arg"`
			Env    map[string]string `json:"env"`
			Files  map[string]string `json:"files"`
			Expect struct {
				URL   *string `json:"url"`
				Token *string `json:"token"`
				CA    *string `json:"ca"`
				Error *string `json:"error"`
			} `json:"expect"`
		} `json:"cases"`
	}
	conformance.Load(t, "operator_resolution.json", &fixture)
	caDir := t.TempDir()
	if err := tlsutil.InitCA(caDir); err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(filepath.Join(caDir, tlsutil.CACertFile))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range fixture.Cases {
		t.Run(c.Name, func(t *testing.T) {
			configDir := isolateClientEnv(t)
			for _, key := range operatorEnv {
				t.Setenv(key, "")
				os.Unsetenv(key)
			}
			tmp := t.TempDir()
			materialize := func(value string) string {
				if !strings.Contains(value, "{config}") && !strings.Contains(value, "{tmp}") {
					return value
				}
				value = strings.NewReplacer("{config}", configDir, "{tmp}", tmp).Replace(value)
				return filepath.Clean(filepath.FromSlash(value))
			}
			for path, content := range c.Files {
				if content == "<CA>" {
					content = string(caPEM)
				}
				writeFile(t, materialize(path), content)
			}
			for key, value := range c.Env {
				t.Setenv(key, materialize(value))
			}

			settings, err := resolveSettings(c.Arg["url"])
			client, clientErr := newAPIClient(c.Arg["url"])
			if c.Expect.Error != nil {
				if err == nil || !strings.Contains(err.Error(), *c.Expect.Error) {
					t.Fatalf("resolveSettings: %v, want an error containing %q", err, *c.Expect.Error)
				}
				if clientErr == nil || !strings.Contains(clientErr.Error(), *c.Expect.Error) {
					t.Fatalf("newAPIClient: %v, want an error containing %q", clientErr, *c.Expect.Error)
				}
				return
			}
			if err != nil || clientErr != nil {
				t.Fatalf("resolveSettings: %v; newAPIClient: %v", err, clientErr)
			}
			if c.Expect.URL == nil || settings.url != *c.Expect.URL || client.baseURL != *c.Expect.URL {
				t.Errorf("url = %q (client %q), want %v", settings.url, client.baseURL, deref(c.Expect.URL))
			}
			wantToken := ""
			if c.Expect.Token != nil {
				wantToken = *c.Expect.Token
			}
			if settings.token != wantToken || client.token != wantToken {
				t.Errorf("token = %q (client %q), want %q", settings.token, client.token, wantToken)
			}
			wantCA := ""
			if c.Expect.CA != nil {
				wantCA = materialize(*c.Expect.CA)
			}
			if gotCA := settings.caPath; (gotCA == "") != (wantCA == "") || (gotCA != "" && filepath.Clean(gotCA) != wantCA) {
				t.Errorf("ca = %q, want %q", gotCA, wantCA)
			}
		})
	}
}

func deref(value *string) string {
	if value == nil {
		return "<null>"
	}
	return *value
}
