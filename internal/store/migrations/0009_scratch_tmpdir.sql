-- scratch_tmpdir (a task's [tasks.execution] scratch_tmpdir = true): the
-- attempt's TMPDIR, TMP and TEMP point at its scratch directory. Off for
-- every execution before it.
ALTER TABLE execution_requests ADD COLUMN scratch_tmpdir BOOLEAN NOT NULL DEFAULT FALSE;
