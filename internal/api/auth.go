package api

import (
	"context"
	"net/http"
	"strings"

	"kairo/internal/store"
)

// Route roles. roleNone is only for /health; roleAny accepts any valid token.
// An admin token may do everything a read token may.
const (
	roleNone   = ""
	roleAny    = "any"
	roleRead   = store.RoleRead
	roleAdmin  = store.RoleAdmin
	roleNode   = store.RoleNode
	roleWorker = store.RoleWorker
)

// Principal is who a request authenticated as.
type Principal struct {
	Role   string `json:"role"`
	Name   string `json:"name,omitempty"`
	NodeID string `json:"node_id,omitempty"`
	// Worker is set for an attempt token.
	Worker *store.WorkerIdentity `json:"-"`
}

type principalKey struct{}

func principalOf(r *http.Request) Principal {
	p, _ := r.Context().Value(principalKey{}).(Principal)
	return p
}

// attemptTokenPrefix tells an attempt token from an operator or node token
// without a second lookup.
const attemptTokenPrefix = "kairo_worker_"

func (s *Server) authenticate(r *http.Request) (Principal, error) {
	header := r.Header.Get("Authorization")
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok || token == "" {
		return Principal{}, store.ErrUnauthorized
	}
	if strings.HasPrefix(token, attemptTokenPrefix) {
		identity, err := s.Store.AuthenticateWorkerToken(r.Context(), token)
		if err != nil {
			return Principal{}, err
		}
		return Principal{Role: roleWorker, Name: identity.AttemptID, Worker: &identity}, nil
	}
	value, err := s.Store.AuthenticateAPIToken(r.Context(), token)
	if err != nil {
		return Principal{}, err
	}
	p := Principal{Role: value.Role, Name: value.Name}
	if value.NodeID != nil {
		p.NodeID = *value.NodeID
	}
	return p, nil
}

func allows(role string, p Principal) bool {
	switch role {
	case roleAny:
		return true
	case roleRead:
		return p.Role == roleRead || p.Role == roleAdmin
	default:
		return p.Role == role
	}
}

// authorize wraps a handler with authentication (401) and the route's role
// (403).
func (s *Server) authorize(role string, next http.HandlerFunc) http.HandlerFunc {
	if role == roleNone {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := s.authenticate(r)
		if err != nil {
			writeError(w, err)
			return
		}
		if !allows(role, p) {
			writeError(w, ErrForbidden)
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	}
}

// actorOr returns the actor named in a request, or the token's name.
func actorOr(r *http.Request, actor string) string {
	if strings.TrimSpace(actor) != "" {
		return actor
	}
	return principalOf(r).Name
}

func (s *Server) whoami(w http.ResponseWriter, r *http.Request) {
	p := principalOf(r)
	body := map[string]any{"role": p.Role, "name": p.Name}
	if p.NodeID != "" {
		body["node_id"] = p.NodeID
	}
	if p.Worker != nil {
		body["attempt_id"] = p.Worker.AttemptID
	}
	writeJSON(w, http.StatusOK, body)
}
