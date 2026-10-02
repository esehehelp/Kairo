package store

// API credentials. Operators and dashboards authenticate with 'admin' or
// 'read' tokens, node agents with a 'node' token bound to their node, and the
// processes of an attempt with the attempt token issued at launch
// authorization. Only SHA-256 hashes are stored: a token is 32 random bytes,
// so a slow hash would add nothing.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"kairo/internal/id"
)

var (
	// ErrUnauthorized: the token is missing, unknown, revoked, or (for an
	// attempt token) its attempt is over or was fenced.
	ErrUnauthorized = errors.New("valid token required")
	// ErrOwnershipConflict: a node tried to claim an executor, provider or
	// resource registered by another node.
	ErrOwnershipConflict = errors.New("object belongs to another node")
)

// Token roles. RoleWorker is never stored in api_tokens: it is the role of an
// attempt token.
const (
	RoleAdmin  = "admin"
	RoleRead   = "read"
	RoleNode   = "node"
	RoleWorker = "worker"
)

type APIToken struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	Role       string  `json:"role"`
	NodeID     *string `json:"node_id,omitempty"`
	CreatedAt  string  `json:"created_at"`
	LastUsedAt *string `json:"last_used_at,omitempty"`
	RevokedAt  *string `json:"revoked_at,omitempty"`
}

// WorkerIdentity is what an attempt token stands for: the attempt and the
// lease and epoch it was issued under.
type WorkerIdentity struct {
	AttemptID string
	LeaseID   string
	Epoch     int64
}

// CreateAPIToken stores a new token and returns it with its plaintext, which
// is not kept anywhere.
func (s *Store) CreateAPIToken(ctx context.Context, name, role, nodeID string) (APIToken, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return APIToken{}, "", errors.New("token name is required")
	}
	switch role {
	case RoleAdmin, RoleRead:
		if nodeID != "" {
			return APIToken{}, "", fmt.Errorf("a %s token is not bound to a node", role)
		}
	case RoleNode:
		if nodeID == "" {
			return APIToken{}, "", errors.New("a node token needs its node id")
		}
	default:
		return APIToken{}, "", fmt.Errorf("unknown token role %q (admin, read or node)", role)
	}
	token, hash, err := newSecret("kairo_" + role + "_")
	if err != nil {
		return APIToken{}, "", err
	}
	value := APIToken{ID: id.New("tok"), Name: name, Role: role, CreatedAt: now()}
	if nodeID != "" {
		value.NodeID = &nodeID
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO api_tokens(id,name,role,node_id,token_hash,created_at) VALUES(?,?,?,?,?,?)`, value.ID, value.Name, value.Role, value.NodeID, hash, value.CreatedAt)
	if err != nil && strings.Contains(err.Error(), "UNIQUE") {
		return APIToken{}, "", fmt.Errorf("an active token named %q already exists", name)
	}
	if err != nil {
		return APIToken{}, "", err
	}
	return value, token, nil
}

func (s *Store) ListAPITokens(ctx context.Context) ([]APIToken, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,name,role,node_id,created_at,last_used_at,revoked_at FROM api_tokens ORDER BY created_at,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []APIToken{}
	for rows.Next() {
		var value APIToken
		if err := rows.Scan(&value.ID, &value.Name, &value.Role, &value.NodeID, &value.CreatedAt, &value.LastUsedAt, &value.RevokedAt); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

// RevokeAPIToken revokes the active token with this id or name.
func (s *Store) RevokeAPIToken(ctx context.Context, idOrName string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE api_tokens SET revoked_at=? WHERE (id=? OR name=?) AND revoked_at IS NULL`, now(), idOrName, idOrName)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// AuthenticateAPIToken resolves an operator or node token. Revocation takes
// effect immediately: nothing is cached.
func (s *Store) AuthenticateAPIToken(ctx context.Context, token string) (APIToken, error) {
	if token == "" {
		return APIToken{}, ErrUnauthorized
	}
	var value APIToken
	err := s.db.QueryRowContext(ctx, `SELECT id,name,role,node_id,created_at,last_used_at FROM api_tokens WHERE token_hash=? AND revoked_at IS NULL`, hashToken(token)).Scan(&value.ID, &value.Name, &value.Role, &value.NodeID, &value.CreatedAt, &value.LastUsedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return APIToken{}, ErrUnauthorized
	}
	if err != nil {
		return APIToken{}, err
	}
	// last_used_at is informational; write it at most once a minute.
	t := time.Now().UTC()
	if _, err = s.db.ExecContext(ctx, `UPDATE api_tokens SET last_used_at=? WHERE id=? AND (last_used_at IS NULL OR julianday(last_used_at)<julianday(?))`, t.Format(time.RFC3339Nano), value.ID, t.Add(-time.Minute).Format(time.RFC3339Nano)); err != nil {
		return APIToken{}, err
	}
	return value, nil
}

// AuthenticateWorkerToken resolves an attempt token. It is valid while its
// attempt has not quiesced or been lost and still carries the epoch the token
// was issued under.
func (s *Store) AuthenticateWorkerToken(ctx context.Context, token string) (WorkerIdentity, error) {
	if token == "" {
		return WorkerIdentity{}, ErrUnauthorized
	}
	var value WorkerIdentity
	var state string
	var attemptEpoch int64
	err := s.db.QueryRowContext(ctx, `SELECT t.attempt_id,t.lease_id,t.coordination_epoch,a.state,a.coordination_epoch FROM attempt_tokens t JOIN attempts a ON a.id=t.attempt_id WHERE t.token_hash=?`, hashToken(token)).Scan(&value.AttemptID, &value.LeaseID, &value.Epoch, &state, &attemptEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkerIdentity{}, ErrUnauthorized
	}
	if err != nil {
		return WorkerIdentity{}, err
	}
	if state == "quiesced" || state == "lost" || attemptEpoch != value.Epoch {
		return WorkerIdentity{}, ErrUnauthorized
	}
	return value, nil
}

// Node ownership of coordination objects, for checking node agent requests.
// Each returns ErrNotFound for an unknown object.

func (s *Store) ExecutorNode(ctx context.Context, executorID string) (string, error) {
	return s.nodeOf(ctx, `SELECT node_id FROM executors WHERE id=?`, executorID)
}

func (s *Store) ProviderNode(ctx context.Context, providerID string) (string, error) {
	return s.nodeOf(ctx, `SELECT node_id FROM resource_providers WHERE id=?`, providerID)
}

func (s *Store) LeaseNode(ctx context.Context, leaseID string) (string, error) {
	return s.nodeOf(ctx, `SELECT x.node_id FROM leases l JOIN executors x ON x.id=l.executor_id WHERE l.id=?`, leaseID)
}

func (s *Store) AttemptNode(ctx context.Context, attemptID string) (string, error) {
	return s.nodeOf(ctx, `SELECT x.node_id FROM attempts a JOIN executors x ON x.id=a.executor_id WHERE a.id=?`, attemptID)
}

func (s *Store) nodeOf(ctx context.Context, query, key string) (string, error) {
	var nodeID string
	err := s.db.QueryRowContext(ctx, query, key).Scan(&nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return nodeID, err
}
