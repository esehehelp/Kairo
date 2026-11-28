-- The database's dump in each hourly control snapshot (README: Kairo keeps itself): whole, a base (its chunk table
-- kept for the deltas after it: dumpdelta), or the difference from a base, which a snapshot holds as a recipe and
-- the chunks its base does not have.
CREATE TABLE control_dumps(
 execution_id TEXT PRIMARY KEY REFERENCES execution_requests(id),
 kind TEXT NOT NULL CHECK(kind IN('base','delta')),
 base_execution_id TEXT REFERENCES execution_requests(id),
 sha256 TEXT NOT NULL,
 bytes BIGINT NOT NULL,
 new_bytes BIGINT NOT NULL DEFAULT 0,
 chunks BYTEA,
 created_at TEXT NOT NULL,
 CHECK((kind='base') = (chunks IS NOT NULL AND base_execution_id IS NULL))
);
CREATE INDEX control_dumps_base_idx ON control_dumps(base_execution_id);
