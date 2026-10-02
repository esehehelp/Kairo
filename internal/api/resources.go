package api

import (
	"net/http"

	"kairo/internal/store"
)

func (s *Server) resourceStatus(w http.ResponseWriter, r *http.Request) {
	resources, err := s.Store.ListResourceStatus(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	claims, err := s.Store.ListClaims(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if resources == nil {
		resources = []store.ResourceInstance{}
	}
	if claims == nil {
		claims = []store.ExternalClaim{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"resources": resources, "external_claims": claims})
}

func (s *Server) enableResource(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.SetResourceAdminState(r.Context(), r.PathValue("id"), "enabled", ""); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

func (s *Server) quarantineResource(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Reason string `json:"reason"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if err := s.Store.SetResourceAdminState(r.Context(), r.PathValue("id"), "quarantined", body.Reason); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

func (s *Server) reconcileResource(w http.ResponseWriter, r *http.Request) {
	if s.ObserveOnly {
		writeError(w, errObserveOnly)
		return
	}
	var body struct {
		ConfirmProcessAbsent bool `json:"confirm_process_absent"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if err := s.Store.ReconcileResource(r.Context(), r.PathValue("id"), body.ConfirmProcessAbsent); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}
