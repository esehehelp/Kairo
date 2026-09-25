package api

import (
	"net/http"

	"kairo/internal/orchestration"
)

func (s *Server) applyProjectDeclaration(w http.ResponseWriter, r *http.Request) {
	var manifest orchestration.Manifest
	if err := decode(r, &manifest); err != nil {
		writeError(w, err)
		return
	}
	validated, err := orchestration.Normalize(manifest)
	if err != nil {
		writeError(w, err)
		return
	}
	idempotent, err := s.Store.ApplyProject(r.Context(), validated)
	if err != nil {
		writeError(w, err)
		return
	}
	status, err := s.Store.GetProjectStatus(r.Context(), validated.Manifest.Project.Name)
	if err != nil {
		writeError(w, err)
		return
	}
	code := http.StatusCreated
	if idempotent {
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]any{
		"project":     status,
		"spec_digest": validated.Digest,
		"idempotent":  idempotent,
	})
}

func (s *Server) getProjectOrchestrationStatus(w http.ResponseWriter, r *http.Request) {
	status, err := s.Store.GetProjectStatus(r.Context(), r.PathValue("project"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"project": status})
}
