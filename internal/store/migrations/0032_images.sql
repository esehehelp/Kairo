-- The container image a task runs in (an OCI reference pinned by digest, registry/repository@sha256:...),
-- bound when the task is declared, and the image each execution runs in.
ALTER TABLE orchestration_tasks ADD COLUMN image_ref TEXT;
ALTER TABLE execution_requests ADD COLUMN image_ref TEXT;
