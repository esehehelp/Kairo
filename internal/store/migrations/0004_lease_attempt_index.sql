-- Every worker request (heartbeat, command poll, acknowledgement, process
-- registration) finds its attempt's lease by attempt_id; without an index
-- each one read the whole leases table.
CREATE INDEX leases_attempt_idx ON leases(attempt_id);
