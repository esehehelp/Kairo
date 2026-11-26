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

// retireNode: a node gone for good (store.RetireNode).
func (s *Server) retireNode(w http.ResponseWriter, r *http.Request) {
	if s.ObserveOnly {
		writeError(w, errObserveOnly)
		return
	}
	var body struct {
		Actor           string `json:"actor"`
		Reason          string `json:"reason"`
		ConfirmNodeGone bool   `json:"confirm_node_gone"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	id := r.PathValue("id")
	switch {
	case id == "all":
		writeError(w, errors.New("a node is retired by its id, one at a time"))
		return
	case !body.ConfirmNodeGone:
		writeError(w, errors.New("retiring a node needs confirm_node_gone: the node (its container or host) is gone for good"))
		return
	}
	released, err := s.Store.RetireNode(r.Context(), id, actorOr(r, body.Actor), body.Reason)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": id, "released_leases": released})
}

// useNodeAsOwner records that the owner of a node uses it (store.UseNodeAsOwner).
func (s *Server) useNodeAsOwner(w http.ResponseWriter, r *http.Request) {
	var body store.OwnerUseRequest
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	body.Actor = actorOr(r, body.Actor)
	uses, err := s.Store.UseNodeAsOwner(r.Context(), r.PathValue("id"), body)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"owner_use": uses})
}

// ownerDone ends what owner-use set on a node (store.OwnerDone).
func (s *Server) ownerDone(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Actor string `json:"actor"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	n, err := s.Store.OwnerDone(r.Context(), r.PathValue("id"), actorOr(r, body.Actor))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"ended": n})
}
