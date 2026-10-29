package hwbench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"kairo/internal/store"
	"kairo/internal/tlsutil"
)

// Client is the Daemon of a bench driver: the worker API's bench routes,
// with the attempt token, for one run.
type Client struct {
	BaseURL string
	Token   string
	RunID   string
	HTTP    *http.Client
}

// NewClient is a client of the daemon at baseURL for run runID. caEnv is
// KAIRO_API_CA (the daemon's CA, base64 DER; empty: system roots). Control
// traffic never goes through an HTTP(S)_PROXY and never follows a redirect.
func NewClient(baseURL, caEnv, token, runID string) (*Client, error) {
	var caPEM []byte
	if strings.TrimSpace(caEnv) != "" {
		var err error
		if caPEM, err = tlsutil.CAFromEnv(caEnv); err != nil {
			return nil, fmt.Errorf("KAIRO_API_CA: %w", err)
		}
	}
	tlsConfig, err := tlsutil.ClientConfig(caPEM)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true, IdleConnTimeout: 90 * time.Second}
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, RunID: runID, HTTP: &http.Client{
		Timeout: 60 * time.Second, Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// APIError is a request the daemon answered with an error.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("worker API %d %s: %s", e.Status, e.Code, e.Message)
}

// retryable: a failure that may pass (the daemon unreachable, busy, or
// failing on its side); a refused request (4xx) is not retried.
func retryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Status >= 500
	}
	return true
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	if query == nil {
		query = url.Values{}
	}
	query.Set("run", c.RunID)
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path+"?"+query.Encode(), payload)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		_ = json.Unmarshal(raw, &e)
		return &APIError{Status: resp.StatusCode, Code: e.Code, Message: e.Error}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func (c *Client) Status(ctx context.Context) (store.BenchStatus, error) {
	var status store.BenchStatus
	err := c.do(ctx, http.MethodGet, "/api/worker/bench", nil, nil, &status)
	return status, err
}

func (c *Client) SetSetting(ctx context.Context, s store.DeviceSettings) (store.DeviceSettingResult, error) {
	var result store.DeviceSettingResult
	err := c.do(ctx, http.MethodPost, "/api/worker/bench/settings", nil, map[string]any{"settings": s}, &result)
	return result, err
}

func (c *Client) CPUEnergy(ctx context.Context, from, to time.Time) (store.CPUEnergy, error) {
	query := url.Values{"from": {from.UTC().Format(time.RFC3339Nano)}, "to": {to.UTC().Format(time.RFC3339Nano)}}
	var energy store.CPUEnergy
	err := c.do(ctx, http.MethodGet, "/api/worker/bench/cpu-energy", query, nil, &energy)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Code == "not_found" {
		// no samples cover the window: the energy is not known
		return store.CPUEnergy{Seconds: to.Sub(from).Seconds()}, nil
	}
	return energy, err
}

func (c *Client) Start(ctx context.Context, start store.BenchStart) error {
	return c.do(ctx, http.MethodPost, "/api/worker/bench/start", nil, start, nil)
}

func (c *Client) AddPoints(ctx context.Context, points []store.BenchPoint) error {
	return c.do(ctx, http.MethodPost, "/api/worker/bench/points", nil, map[string]any{"points": points}, nil)
}

func (c *Client) RecordKind(ctx context.Context, kind string, ref store.BenchReference) error {
	return c.do(ctx, http.MethodPost, "/api/worker/bench/kinds", nil, map[string]any{"kind": kind, "reference": ref}, nil)
}

func (c *Client) Finish(ctx context.Context, finish store.BenchFinish) error {
	return c.do(ctx, http.MethodPost, "/api/worker/bench/finish", nil, finish, nil)
}

func (c *Client) Commands(ctx context.Context) ([]store.Command, error) {
	var body struct {
		Commands []store.Command `json:"commands"`
	}
	err := c.do(ctx, http.MethodGet, "/api/worker/commands", nil, nil, &body)
	return body.Commands, err
}

func (c *Client) Ack(ctx context.Context, commandID, phase string, payload map[string]any) error {
	if payload == nil {
		payload = map[string]any{}
	}
	return c.do(ctx, http.MethodPost, "/api/worker/commands/"+url.PathEscape(commandID)+"/acks", nil, map[string]any{"phase": phase, "payload": payload}, nil)
}

// Main is `kairo hardware bench-run --run ID` inside a bench's attempt: the
// daemon from KAIRO_API_URL, KAIRO_API_CA and KAIRO_ATTEMPT_TOKEN, the run
// it resumes from KAIRO_CONTINUATION_REF, the programs from what the node
// reports, progress on stderr. The first SIGTERM or CTRL_BREAK / Ctrl-C
// (the graceful phase of a force stop) leaves the run at once, the workload
// stopped; a second one cancels whatever the driver is doing.
func Main(ctx context.Context, runID string) error {
	apiURL, token := os.Getenv("KAIRO_API_URL"), os.Getenv("KAIRO_ATTEMPT_TOKEN")
	if apiURL == "" || token == "" {
		return errors.New("KAIRO_API_URL and KAIRO_ATTEMPT_TOKEN are not set: kairo hardware bench-run runs inside a bench's attempt (kairo hardware bench)")
	}
	client, err := NewClient(apiURL, os.Getenv("KAIRO_API_CA"), token, runID)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var stopping atomic.Bool
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	go func() {
		for s := range signals {
			if stopping.Swap(true) {
				cancel()
				return
			}
			logger.Info("stop signal: leaving the bench run", "signal", s.String())
		}
	}()
	d := &Driver{Daemon: client, Runner: ExecRunner{Stderr: os.Stderr}, Logger: logger, Env: os.Getenv, Timing: DefaultTiming(),
		Continuation: os.Getenv("KAIRO_CONTINUATION_REF"), Stopping: stopping.Load}
	return d.Run(ctx, runID)
}
