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

// listDataObjects lists data objects, newest first (kairo data list):
// ?project=, ?kind= and ?limit= narrow it.
func (s *Server) listDataObjects(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := store.DataObjectFilter{Project: q.Get("project"), Kind: q.Get("kind")}
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
