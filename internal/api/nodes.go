package api

import (
	"errors"
	"net/http"

	"kairo/internal/store"
)

// nodeParam maps the path value "all" to the all-node quarantine.
func nodeParam(r *http.Request) string {
	if id := r.PathValue("id"); id != "all" {
		return id
	}
	return store.AllNodes
}

func (s *Server) listNodeQuarantines(w http.ResponseWriter, r *http.Request) {
	value, err := s.Store.ListNodeQuarantines(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if value == nil {
		value = []store.NodeQuarantine{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"quarantines": value})
}

func (s *Server) quarantineNode(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Actor  string `json:"actor"`
		Reason string `json:"reason"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	value, err := s.Store.QuarantineNode(r.Context(), nodeParam(r), actorOr(r, body.Actor), body.Reason)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"quarantine": value})
}

func (s *Server) releaseNodeQuarantine(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Actor string `json:"actor"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if err := s.Store.ReleaseNodeQuarantine(r.Context(), nodeParam(r), actorOr(r, body.Actor)); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"released": true})
}

// fenceNode settles the leases of a node whose agent is gone for good
// (store.FenceNode); the caller confirms it is.
func (s *Server) fenceNode(w http.ResponseWriter, r *http.Request) {
	if s.ObserveOnly {
		writeError(w, errObserveOnly)
		return
	}
	var body struct {
		Actor            string `json:"actor"`
		Reason           string `json:"reason"`
		ConfirmAgentGone bool   `json:"confirm_agent_gone"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	id := r.PathValue("id")
	switch {
	case id == "all":
		writeError(w, errors.New("a node is fenced by its id, one at a time"))
		return
	case !body.ConfirmAgentGone:
		writeError(w, errors.New("fencing a node needs confirm_agent_gone: its agent is gone and will not come back to adopt its attempts"))
		return
	}
	stale, err := s.Store.FenceNode(r.Context(), id, actorOr(r, body.Actor), body.Reason)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": id, "stale_leases": stale})
}
