package main

import (
	"bytes"
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

	"kairo/internal/tlsutil"
)

const defaultAPIURL = "https://127.0.0.1:7474"

// errNoToken is returned when a request needs a token and none was found.
var errNoToken = errors.New("no Kairo token: set KAIRO_TOKEN, KAIRO_TOKEN_FILE or run `kairo token create ... --save`")

func apiFlag(fs *flag.FlagSet) *string {
	return fs.String("api", "", "Kairo API URL (default: $KAIRO_API, else "+defaultAPIURL+"); "+
		"token from $KAIRO_TOKEN, else the file $KAIRO_TOKEN_FILE, else <user config dir>/kairo/token; "+
		"CA from $KAIRO_CA_FILE, else <user config dir>/kairo/ca.pem if present, else the system roots")
}

// apiClient is the one HTTP client of every CLI command: it talks to one
// Kairo API with one bearer token and one TLS trust.
type apiClient struct {
	baseURL string
	token   string
	http    *http.Client
}

// kairoConfigDir is <user config dir>/kairo, where the token and CA live.
func kairoConfigDir() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "kairo"), nil
}

// newAPIClient resolves the API URL, token and CA (see apiFlag) without any
// network traffic. A missing token is only an error once a request needs one.
func newAPIClient(apiFlag string) (*apiClient, error) {
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
		return nil, fmt.Errorf("invalid Kairo API URL %q: want https://HOST:PORT", base)
	}
	token, err := resolveToken()
	if err != nil {
		return nil, err
	}
	if parsed.Scheme == "http" && token != "" && !isLoopback(parsed.Hostname()) {
		return nil, fmt.Errorf("refusing to send a token over plain http to %s: use https", parsed.Host)
	}
	caPEM, err := resolveCA()
	if err != nil {
		return nil, err
	}
	tlsConfig, err := tlsutil.ClientConfig(caPEM)
	if err != nil {
		return nil, fmt.Errorf("Kairo CA: %w", err)
	}
	return &apiClient{
		baseURL: base,
		token:   token,
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
		return strings.TrimSpace(string(data)), nil
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

func resolveCA() ([]byte, error) {
	if path := os.Getenv("KAIRO_CA_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("KAIRO_CA_FILE: %w", err)
		}
		return data, nil
	}
	dir, err := kairoConfigDir()
	if err != nil {
		return nil, nil
	}
	data, err := os.ReadFile(filepath.Join(dir, tlsutil.CACertFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	return data, nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// do sends one request and returns the status and body whatever the status.
func (c *apiClient) do(method, path string, body any) (int, []byte, error) {
	if c.token == "" && path != "/health" {
		return 0, nil, errNoToken
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, c.baseURL+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, payload, nil
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

// httpError turns a non-2xx response into "HTTP 403 forbidden: <message>".
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
		return fmt.Errorf("HTTP %d %s: %s", status, code, wire.Error)
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
	return errors.New(message)
}
