package store

import (
	"context"
	"database/sql"
)

// ResourceUsage is what resource status shows of a resource besides its
// state: its latest observation, how much of it live leases reserve, and the
// executions holding it. Read in a fixed number of queries.
type ResourceUsage struct {
	// TotalBytes and FreeBytes are the latest observation's (for a cpu,
	// millicores; for a GPU, its memory).
	TotalBytes  *int64   `json:"total_bytes,omitempty"`
	FreeBytes   *int64   `json:"free_bytes,omitempty"`
	Utilization *float64 `json:"utilization,omitempty"`
	// Reserved is the capacity live leases hold of a cpu, ram or disk
	// (millicores or bytes); Holders are those leases' executions.
	Reserved int64            `json:"reserved,omitempty"`
	Holders  []ResourceHolder `json:"holders,omitempty"`
}

// ResourceHolder is a live lease on a resource: its execution and that
// execution's coordination scopes.
type ResourceHolder struct {
	ExecutionID string `json:"execution_id"`
	LeaseState  string `json:"lease_state"`
	Quantity    int64  `json:"quantity"`
	Project     string `json:"project"`
	Queue       string `json:"queue,omitempty"`
	Task        string `json:"task,omitempty"`
}

// ListResourceUsage is the usage of every resource, by resource ID.
func (s *Store) ListResourceUsage(ctx context.Context) (map[string]ResourceUsage, error) {
	out := map[string]ResourceUsage{}
	err := eachRow(ctx, s.db, `SELECT r.id,o.total_bytes,o.free_bytes,o.utilization FROM resource_instances r
		JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=r.id ORDER BY id DESC LIMIT 1)`, nil, func(rows *sql.Rows) error {
		var id string
		var u ResourceUsage
		if err := rows.Scan(&id, &u.TotalBytes, &u.FreeBytes, &u.Utilization); err != nil {
			return err
		}
		out[id] = u
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = eachRow(ctx, s.db, `SELECT li.resource_id,li.kind,li.quantity,l.state,e.id,p.external_key,q.external_key,t.external_key
		FROM lease_items li JOIN leases l ON l.id=li.lease_id JOIN execution_requests e ON e.id=l.execution_id
		JOIN coordination_scopes p ON p.id=e.project_scope_id LEFT JOIN coordination_scopes q ON q.id=e.queue_scope_id LEFT JOIN coordination_scopes t ON t.id=e.task_scope_id
		WHERE li.resource_id IS NOT NULL AND l.state IN`+liveLeaseStates+` ORDER BY li.resource_id,l.created_at,l.id`, nil, func(rows *sql.Rows) error {
		var resourceID, kind string
		var h ResourceHolder
		var queue, task sql.NullString
		if err := rows.Scan(&resourceID, &kind, &h.Quantity, &h.LeaseState, &h.ExecutionID, &h.Project, &queue, &task); err != nil {
			return err
		}
		h.Queue, h.Task = queue.String, task.String
		u := out[resourceID]
		if kind != "gpu" {
			u.Reserved += h.Quantity
		}
		u.Holders = append(u.Holders, h)
		out[resourceID] = u
		return nil
	})
	return out, err
}
