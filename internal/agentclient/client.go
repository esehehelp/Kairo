// Package agentclient is the node-agent side of the daemon's /v3/agent API:
// an executor.Coordinator over HTTP, node registration, and attempt log
// shipping into the daemon's log directory.
package agentclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"kairo/internal/api"
	"kairo/internal/store"
)

type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

func New(baseURL, token string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, HTTP: &http.Client{Timeout: 60 * time.Second}}
}

// Error is a non-sentinel failure reported by the daemon.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("agent API %d: %s", e.Status, e.Message) }

func (c *Client) call(ctx context.Context, op string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v3/agent/"+op, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		_ = json.Unmarshal(raw, &e)
		if sentinel := api.AgentErrorForCode(e.Code); sentinel != nil {
			return sentinel
		}
		return &Error{Status: resp.StatusCode, Message: e.Error}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

type Provider struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

// Register upserts this node, its providers and executors, and marks the
// executors' previous leases stale until quiescence is proven again.
func (c *Client) Register(ctx context.Context, node store.Node, executors []store.Executor, providers []Provider) (map[string]int, error) {
	var out struct {
		StaleLeases map[string]int `json:"stale_leases"`
	}
	err := c.call(ctx, "register", map[string]any{"node": node, "executors": executors, "providers": providers}, &out)
	return out.StaleLeases, err
}

func (c *Client) ApplyObservationBatch(ctx context.Context, providerID string, resources []store.ResourceInstance, observations []store.Observation, claims []store.ExternalClaim) error {
	return c.call(ctx, "observations", map[string]any{"provider_id": providerID, "resources": resources, "observations": observations, "claims": claims}, nil)
}

func (c *Client) ReserveNext(ctx context.Context, executorID string) (*store.Reservation, error) {
	var out struct {
		Reservation *store.Reservation `json:"reservation"`
	}
	err := c.call(ctx, "reserve-next", map[string]string{"executor_id": executorID}, &out)
	return out.Reservation, err
}

func (c *Client) EnsurePriorityPreemption(ctx context.Context, executorID string) ([]string, error) {
	var out struct {
		Commands []string `json:"commands"`
	}
	err := c.call(ctx, "ensure-preemption", map[string]string{"executor_id": executorID}, &out)
	return out.Commands, err
}

func (c *Client) ValidateReservation(ctx context.Context, leaseID string, epoch int64) error {
	return c.call(ctx, "validate-reservation", map[string]any{"lease_id": leaseID, "epoch": epoch}, nil)
}

func (c *Client) MarkLeasePrepared(ctx context.Context, leaseID string, epoch int64) error {
	return c.call(ctx, "mark-lease-prepared", map[string]any{"lease_id": leaseID, "epoch": epoch}, nil)
}

func (c *Client) ReleaseReservation(ctx context.Context, leaseID string, epoch int64, reason string) error {
	return c.call(ctx, "release-reservation", map[string]any{"lease_id": leaseID, "epoch": epoch, "reason": reason}, nil)
}

func (c *Client) AuthorizeLaunch(ctx context.Context, reservation *store.Reservation) (*store.Launch, error) {
	var out struct {
		Launch *store.Launch `json:"launch"`
	}
	if err := c.call(ctx, "authorize-launch", map[string]any{"reservation": reservation}, &out); err != nil {
		return nil, err
	}
	if out.Launch == nil {
		return nil, errors.New("authorize-launch returned no launch")
	}
	return out.Launch, nil
}

func (c *Client) ActivateLaunch(ctx context.Context, attemptID, leaseID string, epoch int64, token string, pid int, processIdentity string) error {
	return c.call(ctx, "activate-launch", map[string]any{"attempt_id": attemptID, "lease_id": leaseID, "epoch": epoch, "token": token, "pid": pid, "process_identity": processIdentity}, nil)
}

func (c *Client) SetAttemptLogPaths(ctx context.Context, attemptID string, epoch int64, stdoutPath, stderrPath string) error {
	return c.call(ctx, "set-log-paths", map[string]any{"attempt_id": attemptID, "epoch": epoch, "stdout_path": stdoutPath, "stderr_path": stderrPath}, nil)
}

func (c *Client) MarkAttemptProcessExited(ctx context.Context, attemptID, role string, rank int, identity string) error {
	return c.call(ctx, "mark-process-exited", map[string]any{"attempt_id": attemptID, "role": role, "rank": rank, "identity": identity}, nil)
}

func (c *Client) RecordTerminal(ctx context.Context, attemptID, leaseID string, epoch int64, exitCode int, exitSignal string) error {
	return c.call(ctx, "record-terminal", map[string]any{"attempt_id": attemptID, "lease_id": leaseID, "epoch": epoch, "exit_code": exitCode, "exit_signal": exitSignal}, nil)
}

func (c *Client) ListQuiescenceCandidates(ctx context.Context, executorID string) ([]store.QuiescenceCandidate, error) {
	var out struct {
		Candidates []store.QuiescenceCandidate `json:"candidates"`
	}
	err := c.call(ctx, "quiescence-candidates", map[string]string{"executor_id": executorID}, &out)
	return out.Candidates, err
}

func (c *Client) FinalizeQuiescence(ctx context.Context, attemptID, leaseID string, epoch int64) error {
	return c.call(ctx, "finalize-quiescence", map[string]any{"attempt_id": attemptID, "lease_id": leaseID, "epoch": epoch}, nil)
}

// appendLog sends data found at offset of the node-side file and returns the
// daemon-side size afterwards. A gap (the daemon has less than offset) is
// reported as *GapError carrying the daemon's size to resume from.
func (c *Client) appendLog(ctx context.Context, name string, offset int64, data []byte) (int64, error) {
	payload, err := json.Marshal(api.AgentLogAppend{Name: name, Offset: offset, Data: data})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v3/agent/logs", bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Size  int64  `json:"size"`
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return 0, err
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		return out.Size, nil
	case out.Code == "log_gap":
		return 0, &GapError{Size: out.Size}
	default:
		return 0, &Error{Status: resp.StatusCode, Message: out.Error}
	}
}

type GapError struct{ Size int64 }

func (e *GapError) Error() string { return fmt.Sprintf("log gap; daemon has %d bytes", e.Size) }

// LogShipper copies every attempt log in Dir to the daemon, appending new
// bytes as the files grow. Offsets live in memory: after an agent restart the
// first append starts at 0 and the daemon skips what it already has.
type LogShipper struct {
	Client   *Client
	Dir      string
	Interval time.Duration
	Chunk    int

	mu      sync.Mutex
	shipped map[string]int64
}

func (s *LogShipper) Run(ctx context.Context) error {
	if s.Interval == 0 {
		s.Interval = 2 * time.Second
	}
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		s.Sweep(ctx)
		select {
		case <-ctx.Done():
			s.Sweep(context.Background())
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Sweep ships whatever each log file has gained since the last sweep.
func (s *LogShipper) Sweep(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shipped == nil {
		s.shipped = map[string]int64{}
	}
	if s.Chunk <= 0 {
		s.Chunk = 512 << 10
	}
	matches, _ := filepath.Glob(filepath.Join(s.Dir, "*.log"))
	for _, path := range matches {
		name := filepath.Base(path)
		if !strings.HasSuffix(name, ".stdout.log") && !strings.HasSuffix(name, ".stderr.log") {
			continue
		}
		if err := s.shipFile(ctx, path, name); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "kairo agent: ship %s: %v\n", name, err)
		}
	}
}

func (s *LogShipper) shipFile(ctx context.Context, path, name string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	offset := s.shipped[name]
	buf := make([]byte, s.Chunk)
	for offset < info.Size() {
		n, err := f.ReadAt(buf, offset)
		if n == 0 {
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			break
		}
		size, err := s.Client.appendLog(ctx, name, offset, buf[:n])
		var gap *GapError
		if errors.As(err, &gap) {
			offset = gap.Size
			s.shipped[name] = offset
			continue
		}
		if err != nil {
			return err
		}
		offset = size
		s.shipped[name] = offset
	}
	return nil
}
