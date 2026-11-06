-- not_before, start_by, finish_by ([tasks.execution]): RFC 3339 times with
-- the offset they were declared with. not_before: the execution is not
-- admitted, and holds nothing, before it; start_by and finish_by order it
-- within its priority (planner.go). NULL for every execution before them.
ALTER TABLE execution_requests ADD COLUMN not_before TEXT;
ALTER TABLE execution_requests ADD COLUMN start_by TEXT;
ALTER TABLE execution_requests ADD COLUMN finish_by TEXT;
