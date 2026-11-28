-- What each control snapshot of the code carried (a sources-<slot> task of project kairo, README: Kairo keeps
-- itself): a code snapshot is kept once, in the first such run that succeeds with it; the hourly snapshots hold
-- the database's dump and the configuration only, and are thinned.
CREATE TABLE control_source_runs(
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 source_id TEXT NOT NULL REFERENCES source_snapshots(id),
 PRIMARY KEY(execution_id, source_id)
);
CREATE INDEX control_source_runs_source_idx ON control_source_runs(source_id);
