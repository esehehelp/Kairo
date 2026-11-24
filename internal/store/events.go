package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"kairo/internal/store/dbtx"
)

func appendCoordinationEventTx(ctx context.Context, tx *sql.Tx, eventType string, projectScopeID *string, aggregateType, aggregateID string, epoch *int64, payload any) error {
	return dbtx.AppendEvent(ctx, tx, eventType, projectScopeID, aggregateType, aggregateID, epoch, payload)
}

func (s *Store) ListEvents(ctx context.Context, filter EventFilter) ([]CoordinationEvent, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT sequence,event_type,project_scope_id,aggregate_type,aggregate_id,coordination_epoch,payload_json,created_at FROM coordination_events WHERE sequence>$1 AND ($2='' OR project_scope_id=$3) ORDER BY sequence LIMIT $4`, filter.AfterSequence, filter.ProjectScopeID, filter.ProjectScopeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]CoordinationEvent, 0)
	for rows.Next() {
		var event CoordinationEvent
		var payload string
		if err := rows.Scan(&event.Sequence, &event.EventType, &event.ProjectScopeID, &event.AggregateType, &event.AggregateID, &event.CoordinationEpoch, &payload, &event.CreatedAt); err != nil {
			return nil, err
		}
		event.Payload = json.RawMessage(payload)
		out = append(out, event)
	}
	return out, rows.Err()
}
