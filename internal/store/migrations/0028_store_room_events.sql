-- Find space freed on a store without scanning unrelated coordination events.
CREATE INDEX events_store_room_idx ON coordination_events(event_type,(payload_json::jsonb->>'store_id'),aggregate_id)
 WHERE event_type IN('copy_shed','evicted');
