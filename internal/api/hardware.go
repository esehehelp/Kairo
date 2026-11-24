package api

import (
	"context"
	"net/http"

	"kairo/internal/agentops"
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

// Controller operations on the agent API, for the controller's own node
// (declared in internal/agentops).

func (s *Server) controllerOps() []agentOp {
	byNode := func(_ context.Context, b agentops.NodeRequest) (string, error) { return b.NodeID, nil }
	return []agentOp{
		handle(s, agentops.ControllerRegister,
			func(_ context.Context, b agentops.ControllerRegisterRequest) (string, error) { return b.Node.ID, nil },
			func(ctx context.Context, b agentops.ControllerRegisterRequest) (agentops.ControllerRegisterAnswer, error) {
				return agentops.ControllerRegisterAnswer{OK: true, Version: buildinfo.Version()}, s.Store.RegisterDeviceController(ctx, b.Node, b.Version)
			}),
		// An observe-only daemon changes nothing: the controller is told it
		// controls no device, so it only returns those it may have changed to
		// their defaults.
		handle(s, agentops.DeviceSettings, byNode,
			func(ctx context.Context, b agentops.NodeRequest) (agentops.DevicesAnswer, error) {
				devices, err := s.Store.PollDeviceSettings(ctx, b.NodeID)
				if s.ObserveOnly {
					devices = []store.ControlledDevice{}
				}
				return agentops.DevicesAnswer{Devices: devices}, err
			}),
		handle(s, agentops.DeviceReport,
			func(_ context.Context, b agentops.DeviceReportRequest) (string, error) { return b.NodeID, nil },
			func(ctx context.Context, b agentops.DeviceReportRequest) (agentops.SkippedAnswer, error) {
				skipped, err := s.Store.ReportDevices(ctx, b.NodeID, b.Reports)
				return agentops.SkippedAnswer{Skipped: skipped}, err
			}),
		handle(s, agentops.TrimsToMake, byNode,
			func(ctx context.Context, b agentops.NodeRequest) (agentops.TrimsAnswer, error) {
				trims, err := s.Store.TrimsToMake(ctx, b.NodeID)
				return agentops.TrimsAnswer{Trims: trims}, err
			}),
		handle(s, agentops.RecordTrim,
			func(_ context.Context, b agentops.RecordTrimRequest) (string, error) { return b.NodeID, nil },
			func(ctx context.Context, b agentops.RecordTrimRequest) (agentops.OK, error) {
				return recorded(s.Store.RecordTrim(ctx, b.NodeID, b.Done))
			}),
		handle(s, agentops.DeviceSamples,
			func(_ context.Context, b agentops.DeviceSamplesRequest) (string, error) { return b.NodeID, nil },
			func(ctx context.Context, b agentops.DeviceSamplesRequest) (agentops.SkippedAnswer, error) {
				skipped, err := s.Store.RecordControllerSamples(ctx, b.NodeID, b.Samples)
				return agentops.SkippedAnswer{Skipped: skipped}, err
			}),
	}
}
