package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"kairo/internal/store"
)

// The worker API is called by the processes of an attempt with the attempt
// token handed to them at launch (KAIRO_ATTEMPT_TOKEN). The token stands for
// the attempt and for the lease and epoch it was issued under, so the store's
// epoch fencing applies exactly as before.

func workerIdentity(r *http.Request) store.WorkerIdentity {
	return *principalOf(r).Worker
}

func (s *Server) workerPoll(w http.ResponseWriter, r *http.Request) {
	worker := workerIdentity(r)
	commands, err := s.Store.PollCommands(r.Context(), worker.AttemptID, worker.LeaseID, worker.Epoch)
	if err != nil {
		writeError(w, err)
		return
	}
	if commands == nil {
		commands = []store.Command{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"commands": commands})
}

func (s *Server) workerAck(w http.ResponseWriter, r *http.Request) {
	worker := workerIdentity(r)
	var body struct {
		Phase   string          `json:"phase"`
		Payload json.RawMessage `json:"payload"`
	}
	err := decode(r, &body)
	if err == nil {
		err = s.Store.AckCommand(r.Context(), worker.AttemptID, worker.LeaseID, worker.Epoch, r.PathValue("command"), body.Phase, body.Payload)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	if body.Phase == "checkpointed" {
		s.protectCheckpoint(r.Context(), worker.AttemptID, body.Payload)
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

// workerCheckpoint is a worker asking for a checkpoint of its own attempt
// now (store.RequestCheckpoint): the checkpoint command, to be handled at
// once and acknowledged as any other.
func (s *Server) workerCheckpoint(w http.ResponseWriter, r *http.Request) {
	worker := workerIdentity(r)
	var body struct {
		Reason string `json:"reason"`
	}
	err := decode(r, &body)
	var command store.Command
	if err == nil {
		command, err = s.Store.RequestCheckpoint(r.Context(), worker.AttemptID, worker.LeaseID, worker.Epoch, body.Reason)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"command": command})
}

// protectCheckpoint keeps the checkpoint an attempt just published
// (store.ProtectCheckpoint); the acknowledgement stands whatever becomes of
// it.
func (s *Server) protectCheckpoint(ctx context.Context, attemptID string, payload json.RawMessage) {
	var body struct {
		ContinuationRef string `json:"continuation_ref"`
	}
	if json.Unmarshal(payload, &body) != nil || body.ContinuationRef == "" {
		return
	}
	logger := s.Logger
	if logger == nil {
		logger = slog.Default()
	}
	objectID, err := s.Store.ProtectCheckpoint(ctx, attemptID, body.ContinuationRef)
	switch {
	case err != nil:
		logger.Warn("published checkpoint not kept", "attempt_id", attemptID, "ref", body.ContinuationRef, "error", err)
	case objectID != "":
		logger.Info("published checkpoint kept", "attempt_id", attemptID, "ref", body.ContinuationRef, "object_id", objectID)
	}
}

func (s *Server) workerHeartbeat(w http.ResponseWriter, r *http.Request) {
	worker := workerIdentity(r)
	var body struct {
		Progress json.RawMessage `json:"progress"`
	}
	err := decode(r, &body)
	if err == nil {
		err = s.Store.Heartbeat(r.Context(), worker.AttemptID, worker.LeaseID, worker.Epoch, body.Progress)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

func (s *Server) workerProcess(w http.ResponseWriter, r *http.Request) {
	worker := workerIdentity(r)
	var body struct {
		Rank            int    `json:"rank"`
		PID             int    `json:"pid"`
		ProcessIdentity string `json:"process_identity"`
	}
	err := decode(r, &body)
	if err == nil {
		err = s.Store.RegisterProcess(r.Context(), worker.AttemptID, worker.LeaseID, worker.Epoch, body.Rank, body.PID, body.ProcessIdentity)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}
