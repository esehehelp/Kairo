package api

import (
	"encoding/json"
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
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
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
