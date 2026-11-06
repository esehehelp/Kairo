-- What became of a data object's source once its copy on a durable store was
-- verified: present until its executor claims it for deletion (deleting),
-- then deleted (the directory and the pack it made), or kept when the
-- deletion was refused for safety (source_reason says why).
ALTER TABLE data_objects ADD COLUMN source_state TEXT NOT NULL DEFAULT 'present' CHECK(source_state IN('present','deleting','deleted','kept'));
ALTER TABLE data_objects ADD COLUMN source_reason TEXT;
ALTER TABLE data_objects ADD COLUMN source_deleted_at TEXT;
-- A run's output directory deleted once its object was copied.
ALTER TABLE run_outputs DROP CONSTRAINT run_outputs_state_check;
ALTER TABLE run_outputs ADD CONSTRAINT run_outputs_state_check CHECK(state IN('active','missing','deleted'));
-- When a verified copy was last read back in full, and what was wrong if it
-- did not match (NULL: it matched).
ALTER TABLE object_copies ADD COLUMN checked_at TEXT;
ALTER TABLE object_copies ADD COLUMN check_problem TEXT;
-- A node whose filesystems had something deleted, to be trimmed by its
-- host's controller (pct fstrim, for a Proxmox container): requested since
-- the latest trim, with the bytes deleted since; when it was last trimmed and
-- how that went.
CREATE TABLE node_trims(
 node_id TEXT PRIMARY KEY,
 requested_at TEXT,
 requested_bytes BIGINT NOT NULL DEFAULT 0,
 trimmed_at TEXT,
 problem TEXT,
 output TEXT,
 updated_at TEXT NOT NULL
);
