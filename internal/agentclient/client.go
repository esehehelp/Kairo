// Package agentclient is the node-agent side of the daemon's /api/agent
// operations: an executor.Coordinator over HTTP, node registration, and
// attempt log shipping into the daemon's log directory. It authenticates with
// the node's token and trusts the daemon's CA.
package agentclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"kairo/internal/agentops"
	"kairo/internal/api"
	"kairo/internal/loader"
	"kairo/internal/machine"
	"kairo/internal/store"
)

type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// New returns a client for the daemon at baseURL. tlsConfig carries the CA the
// daemon's certificate is checked against (nil: system roots). Control traffic
// never goes through an HTTP(S)_PROXY.
func New(baseURL, token string, tlsConfig *tls.Config) *Client {
	transport := &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true, IdleConnTimeout: 90 * time.Second}
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, HTTP: &http.Client{
		Timeout: 60 * time.Second, Transport: transport,
		// A redirect would carry the node token elsewhere: never follow one.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// Error is a non-sentinel failure reported by the daemon.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("agent API %d: %s", e.Status, e.Message) }

// HTTPStatus is the status of the daemon's answer: an executor asks again
// after a 5xx (no answer from the store), never after a 4xx.
func (e *Error) HTTPStatus() int { return e.Status }

func (c *Client) call(ctx context.Context, op string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/agent/"+op, bytes.NewReader(payload))
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
		if sentinel := api.ErrorForCode(e.Code); sentinel != nil {
			return fmt.Errorf("%w: %s", sentinel, e.Error)
		}
		return &Error{Status: resp.StatusCode, Message: e.Error}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// do calls the agent operation op (internal/agentops) with req and returns
// its answer.
func do[Req, Resp any](ctx context.Context, c *Client, op agentops.Op[Req, Resp], req Req) (Resp, error) {
	var out Resp
	err := c.call(ctx, op.Name, req, &out)
	return out, err
}

// done is do for an operation that only records.
func done[Req any](ctx context.Context, c *Client, op agentops.Op[Req, agentops.OK], req Req) error {
	_, err := do(ctx, c, op, req)
	return err
}

// Provider is a provider this node registers.
type Provider = agentops.Provider

// Register upserts this node, its providers and executors, and marks the
// executors' previous leases stale until quiescence is proven again.
func (c *Client) Register(ctx context.Context, node store.Node, executors []store.Executor, providers []Provider) (map[string]int, error) {
	stale, _, err := c.RegisterAdopting(ctx, node, executors, providers, nil)
	return stale, err
}

// RegisterAdopting is Register keeping the attempts in adopt the daemon
// finds as they were launched (store.ReregisterExecutor); adopted are those
// it kept. An empty adopt is not sent (a daemon before adoption refuses the
// field).
func (c *Client) RegisterAdopting(ctx context.Context, node store.Node, executors []store.Executor, providers []Provider, adopt []store.AdoptedAttempt) (stale map[string]int, adopted []string, err error) {
	req := agentops.RegisterRequest{Node: node, Executors: executors, Providers: providers, Adopt: adopt}
	if seconds, ok := uptimeSeconds(); ok {
		req.UptimeSeconds = &seconds
	}
	out, err := do(ctx, c, agentops.Register, req)
	return out.StaleLeases, out.Adopted, err
}

func (c *Client) ApplyObservationBatch(ctx context.Context, providerID string, resources []store.ResourceInstance, observations []store.Observation, claims []store.ExternalClaim) error {
	return done(ctx, c, agentops.Observations, agentops.ObservationRequest{ProviderID: providerID, Resources: resources, Observations: observations, Claims: claims})
}

func (c *Client) ReserveNext(ctx context.Context, executorID string) (*store.Reservation, error) {
	out, err := do(ctx, c, agentops.ReserveNext, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Reservation, err
}

func (c *Client) EnsurePriorityPreemption(ctx context.Context, executorID string) ([]string, error) {
	out, err := do(ctx, c, agentops.EnsurePreemption, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Commands, err
}

// NodeStores asks for the stores declared on this node. A daemon older than
// stores answers 404 (an unknown route).
func (c *Client) NodeStores(ctx context.Context, nodeID string) ([]machine.Store, error) {
	out, err := do(ctx, c, agentops.Stores, agentops.NodeRequest{NodeID: nodeID})
	return out.Stores, err
}

// RecordStoreObservation reports what this node sees of one of its stores.
func (c *Client) RecordStoreObservation(ctx context.Context, obs store.StoreObservation) error {
	return done(ctx, c, agentops.StoreObservation, agentops.StoreObservationRequest{Observation: obs})
}

// RecordAttemptScratch records the scratch made for an attempt about to
// launch.
func (c *Client) RecordAttemptScratch(ctx context.Context, scratch store.AttemptScratch) error {
	return done(ctx, c, agentops.AttemptScratch, agentops.AttemptScratchRequest{Scratch: scratch})
}

// RecordRunOutput records the output directory made (or found) for an
// attempt about to launch.
func (c *Client) RecordRunOutput(ctx context.Context, out store.RunOutput) error {
	return done(ctx, c, agentops.RunOutput, agentops.RunOutputRequest{Output: out})
}

// PacksToMake asks for what this executor is to pack now.
func (c *Client) PacksToMake(ctx context.Context, executorID string) ([]store.PackOrder, error) {
	out, err := do(ctx, c, agentops.PacksToMake, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Packs, err
}

// RecordPack records a pack this executor made.
func (c *Client) RecordPack(ctx context.Context, made store.PackMade) error {
	return done(ctx, c, agentops.RecordPack, agentops.RecordPackRequest{Made: made})
}

// CopiesToMake asks for what this executor is to copy to a durable store
// now.
func (c *Client) CopiesToMake(ctx context.Context, executorID string) ([]store.CopyOrder, error) {
	out, err := do(ctx, c, agentops.CopiesToMake, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Copies, err
}

// RecordDescribed records that the description of a verified copy is written
// beside it.
func (c *Client) RecordDescribed(ctx context.Context, executorID, objectID, storeID string) error {
	return done(ctx, c, agentops.RecordDescribed, agentops.ObjectStoreRequest{ExecutorID: executorID, ObjectID: objectID, StoreID: storeID})
}

// AbandonPack records that the source of an import this executor was to
// pack is gone.
func (c *Client) AbandonPack(ctx context.Context, executorID, objectID, reason string) error {
	return done(ctx, c, agentops.AbandonPack, agentops.AbandonPackRequest{ExecutorID: executorID, ObjectID: objectID, Reason: reason})
}

// InventoryDurable reports what a durable store's directory holds.
func (c *Client) InventoryDurable(ctx context.Context, l store.DurableListing) (store.DurableInventory, error) {
	return do(ctx, c, agentops.InventoryDurable, agentops.InventoryRequest{Listing: l})
}

// AdoptCopies records copies found on a durable store that the daemon lacks.
func (c *Client) AdoptCopies(ctx context.Context, executorID, storeID, root string, copies []store.AdoptedCopy) (int, error) {
	out, err := do(ctx, c, agentops.AdoptCopies, agentops.AdoptCopiesRequest{ExecutorID: executorID, StoreID: storeID, Root: root, Copies: copies})
	return out.Adopted, err
}

// ClaimShed claims the removal of a copy of a durable store below its
// reserve; the files are removed only once it is claimed.
func (c *Client) ClaimShed(ctx context.Context, executorID, objectID, storeID string) error {
	return done(ctx, c, agentops.ClaimShed, agentops.ObjectStoreRequest{ExecutorID: executorID, ObjectID: objectID, StoreID: storeID})
}

// RawPlaced records a raw file the executor fetched, checked and keeps on a
// volatile store, and its identity.
func (c *Client) RawPlaced(ctx context.Context, executorID, storeID, root string, f loader.File) error {
	return done(ctx, c, agentops.RawPlaced, agentops.RawPlacedRequest{ExecutorID: executorID, StoreID: storeID, Root: root, File: f})
}

// RawIdentity is what a raw file was found to be the first time it was
// read (found false: never read yet).
func (c *Client) RawIdentity(ctx context.Context, executorID, url string) (loader.File, bool, error) {
	out, err := do(ctx, c, agentops.RawIdentity, agentops.RawIdentityRequest{ExecutorID: executorID, URL: url})
	return out.File, out.Found, err
}

// RawRecords records how many records a kept raw file holds.
func (c *Client) RawRecords(ctx context.Context, executorID string, count store.RecordsCount) error {
	return done(ctx, c, agentops.RawRecords, agentops.RawRecordsRequest{ExecutorID: executorID, Count: count})
}

// KeepIdentity sends the file list beside a copy of a dropped object, for
// the daemon to keep as its identity before the copy is removed.
func (c *Client) KeepIdentity(ctx context.Context, executorID, objectID, storeID, list string) error {
	return done(ctx, c, agentops.KeepIdentity, agentops.KeepIdentityRequest{ExecutorID: executorID, ObjectID: objectID, StoreID: storeID, List: list})
}

// InputGone reports an input whose path is not there at launch.
func (c *Client) InputGone(ctx context.Context, executorID, objectID, path string) error {
	return done(ctx, c, agentops.InputGone, agentops.InputPathRequest{ExecutorID: executorID, ObjectID: objectID, Path: path})
}

// InputIsFile reports an import read in place whose source is a file.
func (c *Client) InputIsFile(ctx context.Context, executorID, objectID, path string) error {
	return done(ctx, c, agentops.InputIsFile, agentops.InputPathRequest{ExecutorID: executorID, ObjectID: objectID, Path: path})
}

// InPlaceSources are the paths of this node inputs are read in place at.
func (c *Client) InPlaceSources(ctx context.Context, executorID string) ([]store.InPlaceSource, error) {
	out, err := do(ctx, c, agentops.InPlaceSources, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Sources, err
}

// RecordCopy records a copy this executor wrote and read back.
func (c *Client) RecordCopy(ctx context.Context, made store.CopyMade) error {
	return done(ctx, c, agentops.RecordCopy, agentops.RecordCopyRequest{Made: made})
}

// FetchesToMake asks for what this executor is to bring to its node now.
func (c *Client) FetchesToMake(ctx context.Context, executorID string) ([]store.FetchOrder, error) {
	out, err := do(ctx, c, agentops.FetchesToMake, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Fetches, err
}

// RecordFetch records an object this executor brought and unpacked.
func (c *Client) RecordFetch(ctx context.Context, made store.FetchMade) error {
	return done(ctx, c, agentops.RecordFetch, agentops.RecordFetchRequest{Made: made})
}

// AuthorizePackRead asks whether token lets this node hand over an
// object's pack, and where it is.
func (c *Client) AuthorizePackRead(ctx context.Context, nodeID, objectID, token string) (store.PackRead, error) {
	out, err := do(ctx, c, agentops.PackRead, agentops.PackReadRequest{NodeID: nodeID, ObjectID: objectID, Token: token})
	return out.Read, err
}

// NetBenches are the network benches this node's agent measures.
func (c *Client) NetBenches(ctx context.Context, nodeID string) ([]store.NetBench, error) {
	out, err := do(ctx, c, agentops.NetBenches, agentops.NetBenchesRequest{NodeID: nodeID})
	return out.Benches, err
}

// NetBenchStatus is a network bench's status, for the agent measuring it.
func (c *Client) NetBenchStatus(ctx context.Context, nodeID, runID string) (store.BenchStatus, error) {
	out, err := do(ctx, c, agentops.NetBenchStatus, agentops.NetBenchRequest{NodeID: nodeID, RunID: runID})
	return out.Status, err
}

// NetBenchStart, NetBenchPoints, NetBenchKind and NetBenchFinish report a
// network bench the agent measures.
func (c *Client) NetBenchStart(ctx context.Context, nodeID, runID string, start store.BenchStart) error {
	return done(ctx, c, agentops.NetBenchStart, agentops.NetBenchStartRequest{NodeID: nodeID, RunID: runID, Start: start})
}

func (c *Client) NetBenchPoints(ctx context.Context, nodeID, runID string, points []store.BenchPoint) error {
	return done(ctx, c, agentops.NetBenchPoints, agentops.NetBenchPointsRequest{NodeID: nodeID, RunID: runID, Points: points})
}

func (c *Client) NetBenchKind(ctx context.Context, nodeID, runID, kind string, ref store.BenchReference) error {
	return done(ctx, c, agentops.NetBenchKind, agentops.NetBenchKindRequest{NodeID: nodeID, RunID: runID, Kind: kind, Reference: ref})
}

func (c *Client) NetBenchFinish(ctx context.Context, nodeID, runID string, finish store.BenchFinish) error {
	return done(ctx, c, agentops.NetBenchFinish, agentops.NetBenchFinishRequest{NodeID: nodeID, RunID: runID, Finish: finish})
}

// AuthorizeBenchNet asks whether token lets a network bench measure this
// node's transfer endpoint.
func (c *Client) AuthorizeBenchNet(ctx context.Context, nodeID, runID, token string) error {
	return done(ctx, c, agentops.NetBenchAuthorize, agentops.NetBenchAuthorizeRequest{NodeID: nodeID, RunID: runID, Token: token})
}

// DeletionsToMake asks for the sources this executor is to delete now.
func (c *Client) DeletionsToMake(ctx context.Context, executorID string) ([]store.DeletionOrder, error) {
	out, err := do(ctx, c, agentops.DeletionsToMake, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Deletions, err
}

// SettleDeletion records what this executor did with a source.
func (c *Client) SettleDeletion(ctx context.Context, settled store.ObjectDeletion) error {
	return done(ctx, c, agentops.SettleDeletion, agentops.SettleDeletionRequest{Settled: settled})
}

// EvictionsToMake asks for what this executor is to evict now.
func (c *Client) EvictionsToMake(ctx context.Context, executorID string) ([]store.EvictionOrder, error) {
	out, err := do(ctx, c, agentops.EvictionsToMake, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Evictions, err
}

// SettleEviction records what this executor did with an eviction.
func (c *Client) SettleEviction(ctx context.Context, settled store.EvictionSettled) error {
	return done(ctx, c, agentops.SettleEviction, agentops.SettleEvictionRequest{Settled: settled})
}

// ScrubsToMake asks for the copies this executor is to read back now.
func (c *Client) ScrubsToMake(ctx context.Context, executorID string) ([]store.ScrubOrder, error) {
	out, err := do(ctx, c, agentops.ScrubsToMake, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Scrubs, err
}

// RecordScrub records what reading a copy back found.
func (c *Client) RecordScrub(ctx context.Context, result store.ScrubResult) error {
	return done(ctx, c, agentops.RecordScrub, agentops.RecordScrubRequest{Result: result})
}

// TrimsToMake asks for the containers this controller is to trim now.
func (c *Client) TrimsToMake(ctx context.Context, nodeID string) ([]store.TrimOrder, error) {
	out, err := do(ctx, c, agentops.TrimsToMake, agentops.NodeRequest{NodeID: nodeID})
	return out.Trims, err
}

// RecordTrim records how a trim this controller made went.
func (c *Client) RecordTrim(ctx context.Context, nodeID string, trimmed store.TrimDone) error {
	return done(ctx, c, agentops.RecordTrim, agentops.RecordTrimRequest{NodeID: nodeID, Done: trimmed})
}

// ScratchToSettle asks for this executor's scratch to delete or keep now.
func (c *Client) ScratchToSettle(ctx context.Context, executorID string) ([]store.ScratchSettlement, error) {
	out, err := do(ctx, c, agentops.ScratchToSettle, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Scratch, err
}

// SettleScratch records what this executor did with a scratch.
func (c *Client) SettleScratch(ctx context.Context, settled store.ScratchSettled) error {
	return done(ctx, c, agentops.SettleScratch, agentops.SettleScratchRequest{Settled: settled})
}

// SourcesInUse are the code snapshots of executions not yet terminal
// (executor.SourceKeeper), for this executor to keep unpacked.
func (c *Client) SourcesInUse(ctx context.Context, executorID string) ([]string, error) {
	out, err := do(ctx, c, agentops.SourcesInUse, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Sources, err
}

// ListUnlaunchedLeases asks for this executor's leases that never reached a
// running attempt (store.ListUnlaunchedLeases). A daemon older than this
// operation answers 404, as an unknown route.
func (c *Client) ListUnlaunchedLeases(ctx context.Context, executorID string, olderThan time.Duration) ([]store.UnlaunchedLease, error) {
	out, err := do(ctx, c, agentops.UnlaunchedLeases, agentops.UnlaunchedRequest{ExecutorID: executorID, OlderThanSeconds: olderThan.Seconds()})
	return out.Leases, err
}

func (c *Client) ValidateReservation(ctx context.Context, leaseID string, epoch int64) error {
	return done(ctx, c, agentops.ValidateReservation, agentops.LeaseRequest{LeaseID: leaseID, Epoch: epoch})
}

func (c *Client) MarkLeasePrepared(ctx context.Context, leaseID string, epoch int64) error {
	return done(ctx, c, agentops.MarkLeasePrepared, agentops.LeaseRequest{LeaseID: leaseID, Epoch: epoch})
}

func (c *Client) ReleaseReservation(ctx context.Context, leaseID string, epoch int64, reason string) error {
	return done(ctx, c, agentops.ReleaseReservation, agentops.LeaseRequest{LeaseID: leaseID, Epoch: epoch, Reason: reason})
}

func (c *Client) AuthorizeLaunch(ctx context.Context, reservation *store.Reservation) (*store.Launch, error) {
	out, err := do(ctx, c, agentops.AuthorizeLaunch, agentops.AuthorizeRequest{Reservation: *reservation})
	if err != nil {
		return nil, err
	}
	if out.Launch == nil {
		return nil, errors.New("authorize-launch returned no launch")
	}
	return out.Launch, nil
}

func (c *Client) ActivateLaunch(ctx context.Context, attemptID, leaseID string, epoch int64, token string, pid int, processIdentity string) error {
	return done(ctx, c, agentops.ActivateLaunch, agentops.ActivateRequest{AttemptID: attemptID, LeaseID: leaseID, Epoch: epoch, Token: token, PID: pid, ProcessIdentity: processIdentity})
}

func (c *Client) SetAttemptLogPaths(ctx context.Context, attemptID string, epoch int64, stdoutPath, stderrPath string) error {
	return done(ctx, c, agentops.SetLogPaths, agentops.LogPathsRequest{AttemptID: attemptID, Epoch: epoch, StdoutPath: stdoutPath, StderrPath: stderrPath})
}

func (c *Client) RecordAttemptLogProblem(ctx context.Context, attemptID string, problem store.LogProblem) error {
	return done(ctx, c, agentops.LogProblem, agentops.LogProblemRequest{AttemptID: attemptID, Problem: problem})
}

func (c *Client) MarkAttemptProcessExited(ctx context.Context, attemptID, role string, rank int, identity string) error {
	return done(ctx, c, agentops.MarkProcessExited, agentops.ProcessExitedRequest{AttemptID: attemptID, Role: role, Rank: rank, Identity: identity})
}

func (c *Client) RecordTerminal(ctx context.Context, attemptID, leaseID string, epoch int64, exitCode int, exitSignal string) error {
	return done(ctx, c, agentops.RecordTerminal, agentops.TerminalRequest{AttemptID: attemptID, LeaseID: leaseID, Epoch: epoch, ExitCode: exitCode, ExitSignal: exitSignal})
}

func (c *Client) ListQuiescenceCandidates(ctx context.Context, executorID string) ([]store.QuiescenceCandidate, error) {
	out, err := do(ctx, c, agentops.QuiescenceCandidates, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Candidates, err
}

func (c *Client) FinalizeQuiescence(ctx context.Context, attemptID, leaseID string, epoch int64) error {
	return done(ctx, c, agentops.FinalizeQuiescence, agentops.AttemptLeaseRequest{AttemptID: attemptID, LeaseID: leaseID, Epoch: epoch})
}

func (c *Client) ListQuarantineTerminations(ctx context.Context, executorID string) ([]store.QuarantineTermination, error) {
	out, err := do(ctx, c, agentops.QuarantineTerms, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Terminations, err
}

func (c *Client) MarkQuarantineTerminated(ctx context.Context, attemptID string) error {
	return done(ctx, c, agentops.QuarantineTerminated, agentops.AttemptRequest{AttemptID: attemptID})
}

func (c *Client) ForceStopOrders(ctx context.Context, executorID string) ([]store.ForceStopOrder, error) {
	out, err := do(ctx, c, agentops.ForceStopOrders, agentops.ExecutorRequest{ExecutorID: executorID})
	return out.Orders, err
}

func (c *Client) AckForceStop(ctx context.Context, attemptID, phase string, detail json.RawMessage) error {
	return done(ctx, c, agentops.ForceStopAcked, agentops.ForceStopAck{AttemptID: attemptID, Phase: phase, Detail: detail})
}

func (c *Client) RecordAttemptUsage(ctx context.Context, attemptID string, ramBytes, diskWrittenBytes *int64) error {
	return done(ctx, c, agentops.AttemptUsed, agentops.AttemptUsage{AttemptID: attemptID, RAMBytes: ramBytes, DiskWrittenBytes: diskWrittenBytes})
}

// RegisterController records that node's device controller started (kairo
// controller); a node not known yet is created, a known one left as it is.
func (c *Client) RegisterController(ctx context.Context, node store.Node, version string) error {
	_, err := do(ctx, c, agentops.ControllerRegister, agentops.ControllerRegisterRequest{Node: node, Version: version})
	return err
}

// DeviceSettings is a controller's poll: the devices its node controls and
// the settings each is to have.
func (c *Client) DeviceSettings(ctx context.Context, nodeID string) ([]store.ControlledDevice, error) {
	out, err := do(ctx, c, agentops.DeviceSettings, agentops.NodeRequest{NodeID: nodeID})
	return out.Devices, err
}

// ReportDevices sends what is in place on the node's devices; it returns the
// reports of devices the node does not control, which the daemon skipped.
func (c *Client) ReportDevices(ctx context.Context, nodeID string, reports []store.DeviceReport) ([]string, error) {
	out, err := do(ctx, c, agentops.DeviceReport, agentops.DeviceReportRequest{NodeID: nodeID, Reports: reports})
	return out.Skipped, err
}

// ReportSamples sends what the controller of node read of its host (the CPU
// package power); it returns the samples the daemon skipped (no host names
// node as its controller).
func (c *Client) ReportSamples(ctx context.Context, nodeID string, samples []store.ControllerSample) ([]string, error) {
	out, err := do(ctx, c, agentops.DeviceSamples, agentops.DeviceSamplesRequest{NodeID: nodeID, Samples: samples})
	return out.Skipped, err
}

// appendLog sends data found at offset of the node-side file and returns the
// daemon-side size afterwards. A gap (the daemon has less than offset) is
// reported as *GapError carrying the daemon's size to resume from.
func (c *Client) appendLog(ctx context.Context, name string, offset int64, data []byte) (int64, error) {
	payload, err := json.Marshal(api.AgentLogAppend{Name: name, Offset: offset, Data: data})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/agent/logs", bytes.NewReader(payload))
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
	case api.ErrorForCode(out.Code) != nil:
		return 0, api.ErrorForCode(out.Code)
	default:
		return 0, &Error{Status: resp.StatusCode, Message: out.Error}
	}
}

type GapError struct{ Size int64 }

func (e *GapError) Error() string { return fmt.Sprintf("log gap; daemon has %d bytes", e.Size) }

// LogShipper copies every attempt log in Dir to the daemon, appending new
// bytes as the files grow; an empty log is shipped too, so the daemon holds
// every log an attempt has, even one it could not write to. Offsets live in
// memory: after an agent restart the first append starts at 0 and the daemon
// skips what it already has. A log that cannot be shipped (the daemon is
// unreachable, or its disk is full) is retried at every sweep from where the
// daemon's copy ends, so nothing is lost while the node-side file exists; one
// that has not reached the daemon for ReportAfter is recorded on its attempt
// (log_forward_failed), and so is its catching up.
type LogShipper struct {
	Client   *Client
	Dir      string
	Interval time.Duration
	Chunk    int
	// ReportAfter is how long shipping a log may fail before the failure is
	// recorded on its attempt (zero: 30 s); a shorter outage leaves no trace.
	ReportAfter time.Duration
	// Clock is the time source (nil: time.Now).
	Clock func() time.Time

	mu      sync.Mutex
	shipped map[string]int64
	// refused holds logs the daemon will never take (no such attempt, or
	// another node's), so they are not read and re-sent every sweep.
	refused map[string]bool
	// failing holds the logs whose last shipping failed, by name.
	failing map[string]*shipFailure
}

// shipFailure is a log that has not reached the daemon since a time.
type shipFailure struct {
	since    time.Time
	err      string
	reported bool // recorded on the attempt
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

func (s *LogShipper) init() {
	if s.shipped == nil {
		s.shipped = map[string]int64{}
		s.refused = map[string]bool{}
		s.failing = map[string]*shipFailure{}
	}
	if s.Chunk <= 0 {
		s.Chunk = 512 << 10
	}
}

// Sweep ships whatever each log file has gained since the last sweep.
func (s *LogShipper) Sweep(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	matches, _ := filepath.Glob(filepath.Join(s.Dir, "*.log"))
	for _, path := range matches {
		name := filepath.Base(path)
		if !strings.HasSuffix(name, ".stdout.log") && !strings.HasSuffix(name, ".stderr.log") || s.refused[name] {
			continue
		}
		_ = s.ship(ctx, path, name)
	}
}

// ShipAttempt ships both logs of one attempt now (an executor calls it as the
// attempt exits, through executor.Local.FlushLogs).
func (s *LogShipper) ShipAttempt(ctx context.Context, attemptID string) error {
	// A sweep in progress holds the lock; wait for it no longer than ctx.
	for !s.mu.TryLock() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	defer s.mu.Unlock()
	s.init()
	var first error
	for _, stream := range []string{"stdout", "stderr"} {
		name := attemptID + "." + stream + ".log"
		path := filepath.Join(s.Dir, name)
		if _, err := os.Stat(path); err != nil || s.refused[name] {
			continue
		}
		if err := s.ship(ctx, path, name); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// ship ships one log and keeps track of its failures.
func (s *LogShipper) ship(ctx context.Context, path, name string) error {
	err := s.shipFile(ctx, path, name)
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, api.ErrForbidden) {
		s.refused[name] = true
		delete(s.failing, name)
		fmt.Fprintf(os.Stderr, "kairo agent: not shipping %s: %v\n", name, err)
		return err
	}
	if ctx.Err() != nil {
		// Shutting down (or a flush that ran out of time): not the log's fault.
		return err
	}
	s.track(ctx, name, err)
	return err
}

// track follows a log's shipping failures: the first is logged, one that
// lasts ReportAfter is recorded on the attempt, and so is the catching up
// that ends it.
func (s *LogShipper) track(ctx context.Context, name string, err error) {
	now := time.Now()
	if s.Clock != nil {
		now = s.Clock()
	}
	reportAfter := s.ReportAfter
	if reportAfter == 0 {
		reportAfter = 30 * time.Second
	}
	attemptID, _ := strings.CutSuffix(name, ".log")
	attemptID, stream, _ := strings.Cut(attemptID, ".")
	f := s.failing[name]
	if err != nil {
		if f == nil {
			f = &shipFailure{since: now}
			s.failing[name] = f
			fmt.Fprintf(os.Stderr, "kairo agent: ship %s: %v (retrying)\n", name, err)
		}
		f.err = err.Error()
		if !f.reported && now.Sub(f.since) >= reportAfter {
			problem := store.LogProblem{Problem: store.LogForwardFailed, Stream: stream, Detail: f.err + " (retrying)"}
			f.reported = s.Client.RecordAttemptLogProblem(ctx, attemptID, problem) == nil
		}
		return
	}
	if f == nil {
		return
	}
	if f.reported || now.Sub(f.since) >= reportAfter {
		problem := store.LogProblem{Problem: store.LogForwardFailed, Stream: stream, Resolved: true,
			Detail: fmt.Sprintf("%s; caught up after %s", f.err, now.Sub(f.since).Round(time.Second))}
		if s.Client.RecordAttemptLogProblem(ctx, attemptID, problem) != nil {
			return // recorded at the next sweep
		}
	}
	delete(s.failing, name)
	fmt.Fprintf(os.Stderr, "kairo agent: ship %s: caught up\n", name)
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
	offset, known := s.shipped[name]
	if !known {
		// The first append of a log (also an empty one) creates the daemon's
		// copy and tells where it ends.
		size, err := s.Client.appendLog(ctx, name, 0, nil)
		if err != nil {
			return err
		}
		offset = size
		s.shipped[name] = offset
	}
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

// FetchSource writes file ("pack" or "list") of code snapshot id, as the
// daemon keeps it, to w (executor.SourceFetcher).
func (c *Client) FetchSource(ctx context.Context, id, file string, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/api/agent/sources/"+url.PathEscape(id)+"/"+url.PathEscape(file), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var e struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		_ = json.Unmarshal(raw, &e)
		if sentinel := api.ErrorForCode(e.Code); sentinel != nil {
			return fmt.Errorf("%w: %s", sentinel, e.Error)
		}
		return &Error{Status: resp.StatusCode, Message: e.Error}
	}
	_, err = io.Copy(w, resp.Body)
	return err
}
