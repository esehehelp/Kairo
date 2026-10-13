package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kairo/internal/buildinfo"
	"kairo/internal/tlsutil"
)

const defaultAPIURL = "https://127.0.0.1:7474"

// errNoToken is returned when a request needs a token and none was found.
var errNoToken = errors.New("no Kairo token: set KAIRO_TOKEN, KAIRO_TOKEN_FILE or run `kairo token create ... --save`")

func apiFlag(fs *flag.FlagSet) *string {
	// The token and CA resolution is in `kairo help` (remoteNote).
	return fs.String("api", "", "Kairo API URL (default: $KAIRO_API, else "+defaultAPIURL+"; token and CA: see kairo help)")
}

// apiClient is the one HTTP client of every CLI command: it talks to one
// Kairo API with one bearer token and one TLS trust.
type apiClient struct {
	baseURL string
	token   string
	http    *http.Client
	// ctx, when set, cancels requests (Ctrl+C while following or waiting).
	ctx context.Context
}

// kairoConfigDir is <user config dir>/kairo, where the token and CA live.
func kairoConfigDir() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "kairo"), nil
}

// apiSettings is where an operator client finds the daemon: its URL, token
// and the CA it trusts (sdk/conformance/operator_resolution.json).
type apiSettings struct {
	url   string
	token string // "" when none was found: a request needing one fails
	// caPath is the CA file trusted ("" for the system roots); caPEM its content.
	caPath string
	caPEM  []byte
}

// resolveSettings resolves the API URL, token and CA (see apiFlag) without
// any network traffic. A missing token is only an error once a request needs
// one.
func resolveSettings(apiFlag string) (apiSettings, error) {
	base := apiFlag
	if base == "" {
		base = os.Getenv("KAIRO_API")
	}
	if base == "" {
		base = defaultAPIURL
	}
	base = strings.TrimRight(base, "/")
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return apiSettings{}, fmt.Errorf("invalid Kairo API URL %q: want https://HOST:PORT", base)
	}
	if parsed.User != nil {
		return apiSettings{}, fmt.Errorf("invalid Kairo API URL: credentials do not belong in the URL (use KAIRO_TOKEN)")
	}
	token, err := resolveToken()
	if err != nil {
		return apiSettings{}, err
	}
	if token != "" && !tokenTransportAllowed(parsed) {
		return apiSettings{}, fmt.Errorf("refusing to send a token over plain http to %s: use https", parsed.Host)
	}
	caPath, caPEM, err := resolveCA()
	if err != nil {
		return apiSettings{}, err
	}
	return apiSettings{url: base, token: token, caPath: caPath, caPEM: caPEM}, nil
}

// newAPIClient resolves the settings (resolveSettings) and builds the client.
func newAPIClient(apiFlag string) (*apiClient, error) {
	settings, err := resolveSettings(apiFlag)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := tlsutil.ClientConfig(settings.caPEM)
	if err != nil {
		return nil, fmt.Errorf("Kairo CA: %w", err)
	}
	return &apiClient{
		baseURL: settings.url,
		token:   settings.token,
		http: &http.Client{
			Timeout: 30 * time.Second,
			// Never proxied, never redirected: the token goes to this URL only.
			Transport: &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func resolveToken() (string, error) {
	if token := strings.TrimSpace(os.Getenv("KAIRO_TOKEN")); token != "" {
		return token, nil
	}
	if path := os.Getenv("KAIRO_TOKEN_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("KAIRO_TOKEN_FILE: %w", err)
		}
		token := strings.TrimSpace(string(data))
		if token == "" {
			return "", fmt.Errorf("KAIRO_TOKEN_FILE %s is empty", path)
		}
		return token, nil
	}
	dir, err := kairoConfigDir()
	if err != nil {
		return "", nil
	}
	data, err := os.ReadFile(filepath.Join(dir, "token"))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read token: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// resolveCA returns the CA file to trust and its content, or "" and nil for
// the system roots.
func resolveCA() (string, []byte, error) {
	if path := os.Getenv("KAIRO_CA_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", nil, fmt.Errorf("KAIRO_CA_FILE: %w", err)
		}
		return path, data, nil
	}
	dir, err := kairoConfigDir()
	if err != nil {
		return "", nil, nil
	}
	path := filepath.Join(dir, tlsutil.CACertFile)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, fmt.Errorf("read CA: %w", err)
	}
	return path, data, nil
}

// tokenTransportAllowed reports whether a bearer token may be sent to u:
// https always, http only to a loopback host; never another scheme, a URL
// without a host or one carrying user:password
// (sdk/conformance/token_transport.json).
func tokenTransportAllowed(u *url.URL) bool {
	if u.Host == "" || u.User != nil {
		return false
	}
	switch u.Scheme {
	case "https":
		return true
	case "http":
		return isLoopback(u.Hostname())
	default:
		return false
	}
}

// isLoopback reports whether host (brackets removed) is "localhost" in any
// case or a loopback IP literal, IPv4-mapped ones included. Nothing else
// matches: no DNS lookups, no zone suffixes (sdk/conformance/loopback.json).
func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if strings.Contains(host, "%") {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// do sends one request and returns the status and body whatever the status.
func (c *apiClient) do(method, path string, body any) (int, []byte, error) {
	status, _, payload, err := c.send(method, path, body)
	return status, payload, err
}

// send is do with the response headers.
func (c *apiClient) send(method, path string, body any) (int, http.Header, []byte, error) {
	if c.token == "" && path != "/health" {
		return 0, nil, nil, errNoToken
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, nil, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, payload, nil
}

// getJSON decodes the body of a 2xx GET response into target.
func (c *apiClient) getJSON(path string, target any) error {
	payload, err := c.call(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

// call returns the body of a 2xx response, and an error naming the status and
// the server's message otherwise.
func (c *apiClient) call(method, path string, body any) ([]byte, error) {
	status, payload, err := c.do(method, path, body)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, httpError(status, payload)
	}
	return payload, nil
}

// print writes the body of a 2xx response to stdout.
func (c *apiClient) print(method, path string, body any) error {
	payload, err := c.call(method, path, body)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(payload)
	return err
}

// apiError is a non-2xx response.
type apiError struct {
	Status int
	Code   string // the server's error code ("" when the body was not a Kairo error)
	// Message is the server's error message (or the start of the body).
	Message string
	// RouteMissing: the daemon's mux, not a handler, answered: the daemon does
	// not know the route, so it is likely older than this CLI.
	RouteMissing bool
	text         string
}

func (e *apiError) Error() string { return e.text }

// notFound reports whether err is a 404 for a missing entity (not a missing route).
func notFound(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.Status == http.StatusNotFound && !ae.RouteMissing
}

// routeMissing reports whether err says the daemon does not know the route.
func routeMissing(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.RouteMissing
}

// olderDaemonHint is added when the daemon does not know a route.
func olderDaemonHint() string {
	return "the daemon may be older than this CLI (cli " + buildinfo.Version() + "); restart it from the same build"
}

// httpError turns a non-2xx response into "HTTP 403 forbidden: <message>".
// Handlers answer {"error", "code"}; the daemon's mux answers an unknown route
// with a plain-text 404 (a 405 for a known path with another method).
func httpError(status int, payload []byte) error {
	var wire struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	if json.Unmarshal(payload, &wire) == nil && wire.Error != "" {
		code := wire.Code
		if code == "" {
			code = strings.ToLower(http.StatusText(status))
		}
		return &apiError{Status: status, Code: wire.Code, Message: wire.Error, text: fmt.Sprintf("HTTP %d %s: %s", status, code, wire.Error)}
	}
	text := strings.TrimSpace(string(payload))
	if len(text) > 200 {
		text = text[:200] + "..."
	}
	message := fmt.Sprintf("HTTP %d %s", status, http.StatusText(status))
	if status >= 300 && status < 400 {
		message += " (redirects are not followed)"
	}
	if text != "" {
		message += ": " + text
	}
	missing := status == http.StatusNotFound || status == http.StatusMethodNotAllowed
	if missing {
		message += "; " + olderDaemonHint()
	}
	return &apiError{Status: status, Message: text, RouteMissing: missing, text: message}
}
