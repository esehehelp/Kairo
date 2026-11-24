package api

import (
	"errors"
	"net/http"
	"strconv"

	"kairo/internal/store"
)

// importData imports directories of a node (kairo data import).
func (s *Server) importData(w http.ResponseWriter, r *http.Request) {
	var body store.ImportRequest
	if err := decode(r, &body); err != nil {
		writeError(w, err)
		return
	}
	objects, err := s.Store.ImportData(r.Context(), body)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"objects": objects})
}

// dropData drops data Kairo is not to keep the bytes of (kairo data drop),
// or says what it would drop.
func (s *Server) dropData(w http.ResponseWriter, r *http.Request) {
	var body store.DropRequest
	if err := decode(r, &body); err != nil {
		writeError(w, err)
		return
	}
	objects, err := s.Store.DropData(r.Context(), body, actorOr(r, ""))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"objects": objects})
}

// cancelImport abandons an import not packed yet (kairo data cancel).
func (s *Server) cancelImport(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.CancelImport(r.Context(), r.PathValue("id"), actorOr(r, "")); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// listDataObjects lists data objects, newest first (kairo data list):
// ?project=, ?kind=, ?task=, ?execution= and ?limit= narrow it.
func (s *Server) listDataObjects(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := store.DataObjectFilter{Project: q.Get("project"), Kind: q.Get("kind"), Task: q.Get("task"), Execution: q.Get("execution")}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 10000 {
			writeError(w, errors.New("limit is a number from 1 to 10000"))
			return
		}
		filter.Limit = n
	}
	objects, err := s.Store.DataObjects(r.Context(), filter)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"objects": objects})
}

// getDataPlacement reports where an object is being unpacked on a node.
func (s *Server) getDataPlacement(w http.ResponseWriter, r *http.Request) {
	node := r.URL.Query().Get("node")
	if node == "" {
		writeError(w, errors.New("node is required"))
		return
	}
	placement, err := s.Store.DataPlacement(r.Context(), r.PathValue("id"), node)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, placement)
}

// fetchData orders data objects brought to a node (kairo data fetch).
func (s *Server) fetchData(w http.ResponseWriter, r *http.Request) {
	var body store.FetchRequest
	if err := decode(r, &body); err != nil {
		writeError(w, err)
		return
	}
	objects, err := s.Store.FetchData(r.Context(), body)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"objects": objects})
}
