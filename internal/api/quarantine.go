package api

import (
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
	value, err := s.Store.QuarantineNode(r.Context(), nodeParam(r), body.Actor, body.Reason)
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
	if err := s.Store.ReleaseNodeQuarantine(r.Context(), nodeParam(r), body.Actor); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"released": true})
}
