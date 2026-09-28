package api

import (
	"errors"
	"net/http"

	"kairo/internal/store"
)

// quarantineTarget is the node a quarantine route acts on: the {id} path value
// taken literally, or every node on /v2/node-quarantines/all. The all-node
// quarantine is only reachable through its own route, so no node id aliases it.
func quarantineTarget(r *http.Request) (string, error) {
	id := r.PathValue("id")
	if id == "" {
		return store.AllNodes, nil
	}
	if id == store.AllNodes {
		return "", errors.New("the all-node quarantine is /v2/node-quarantines/all")
	}
	return id, nil
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
	node, err := quarantineTarget(r)
	if err != nil {
		writeError(w, err)
		return
	}
	value, err := s.Store.QuarantineNode(r.Context(), node, body.Actor, body.Reason)
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
	node, err := quarantineTarget(r)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := s.Store.ReleaseNodeQuarantine(r.Context(), node, body.Actor); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"released": true})
}
