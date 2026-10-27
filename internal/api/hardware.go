package api

import (
	"context"
	"net/http"

	"kairo/internal/buildinfo"
	"kairo/internal/store"
)

// Device settings: operators (kairo hardware) and bench runs set the desired
// settings of a host's devices; the host's controller polls them through the
// agent API with its node token and reports what is in place.

func (s *Server) listHardware(w http.ResponseWriter, r *http.Request) {
	hosts, err := s.Store.ListHardware(r.Context(), r.URL.Query().Get("host"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"hosts": hosts})
}

// deviceSettingBody sets or clears a setting. Owner defaults to manual; a
// bench or policy owner names its lease in owner.ref.
type deviceSettingBody struct {
	Actor    string               `json:"actor"`
	Owner    *store.DeviceOwner   `json:"owner"`
	Settings store.DeviceSettings `json:"settings"`
}

func (b deviceSettingBody) owner() store.DeviceOwner {
	if b.Owner == nil {
		return store.DeviceOwner{Kind: store.OwnerManual}
	}
	return *b.Owner
}

func (s *Server) setDeviceSetting(w http.ResponseWriter, r *http.Request) {
	var body deviceSettingBody
	if err := decode(r, &body); err != nil {
		writeError(w, err)
		return
	}
	result, err := s.Store.SetDeviceSetting(r.Context(), r.PathValue("host"), r.PathValue("device"), body.owner(), body.Settings, actorOr(r, body.Actor))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) clearDeviceSetting(w http.ResponseWriter, r *http.Request) {
	var body deviceSettingBody
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	result, err := s.Store.ClearDeviceSetting(r.Context(), r.PathValue("host"), r.PathValue("device"), body.owner(), actorOr(r, body.Actor))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// Controller operations on the agent API, for the controller's own node.

type controllerRegisterRequest struct {
	Node    store.Node `json:"node"`
	Version string     `json:"version"`
}

type controllerPollRequest struct {
	NodeID string `json:"node_id"`
}

type controllerReportRequest struct {
	NodeID  string               `json:"node_id"`
	Reports []store.DeviceReport `json:"reports"`
}

type controllerSamplesRequest struct {
	NodeID  string                   `json:"node_id"`
	Samples []store.ControllerSample `json:"samples"`
}

func (s *Server) controllerOps() []agentOp {
	return []agentOp{
		newAgentOp(s, "controller-register", observeAllowed, nil,
			func(_ context.Context, b controllerRegisterRequest) (string, error) { return b.Node.ID, nil },
			func(ctx context.Context, b controllerRegisterRequest) (any, error) {
				return map[string]any{"ok": true, "version": buildinfo.Version()}, s.Store.RegisterDeviceController(ctx, b.Node, b.Version)
			}),
		// An observe-only daemon changes nothing: the controller is told it
		// controls no device, so it only returns those it may have changed to
		// their defaults.
		newAgentOp(s, "device-settings", observeAllowed, nil,
			func(_ context.Context, b controllerPollRequest) (string, error) { return b.NodeID, nil },
			func(ctx context.Context, b controllerPollRequest) (any, error) {
				devices, err := s.Store.PollDeviceSettings(ctx, b.NodeID)
				if s.ObserveOnly {
					devices = []store.ControlledDevice{}
				}
				return map[string]any{"devices": devices}, err
			}),
		newAgentOp(s, "device-report", observeAllowed, nil,
			func(_ context.Context, b controllerReportRequest) (string, error) { return b.NodeID, nil },
			func(ctx context.Context, b controllerReportRequest) (any, error) {
				skipped, err := s.Store.ReportDevices(ctx, b.NodeID, b.Reports)
				return map[string]any{"skipped": skipped}, err
			}),
		// What the controller reads of its host (the CPU package's power) is
		// observation, recorded by an observe-only daemon too.
		newAgentOp(s, "device-samples", observeAllowed, nil,
			func(_ context.Context, b controllerSamplesRequest) (string, error) { return b.NodeID, nil },
			func(ctx context.Context, b controllerSamplesRequest) (any, error) {
				skipped, err := s.Store.RecordControllerSamples(ctx, b.NodeID, b.Samples)
				return map[string]any{"skipped": skipped}, err
			}),
	}
}
