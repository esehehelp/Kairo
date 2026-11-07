-- store_id: the volatile store an execution's attempts put their scratch,
-- run output and cache directories on (NULL: the first their executor
-- serves); the execution is planned onto its node.
ALTER TABLE execution_requests ADD COLUMN store_id TEXT;
