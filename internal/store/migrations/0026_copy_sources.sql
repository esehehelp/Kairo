-- A replica read from a durable copy on another node (replicas.go): the copy
-- its token reads, which that node's pack server serves.
ALTER TABLE object_copies ADD COLUMN source_store_id TEXT;
ALTER TABLE object_copies ADD COLUMN source_root TEXT;
ALTER TABLE object_copies ADD COLUMN source_month TEXT;
ALTER TABLE object_copies ADD COLUMN source_node_id TEXT;
