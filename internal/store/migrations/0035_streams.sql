-- Streams (issue #43): the streams an execution reads, as its task declared them with their raws' files
-- (orchestration.StreamDecl, JSON); what each raw file was found to be the first time a loader read it, by its URL
-- (its size, sha256 and the sha256 of each chunk: every later read is checked against it); and the records a file
-- (by sha256) holds under a framing ("<framing>|<field>"), which lets a loader start mid-way without reading it.
ALTER TABLE execution_requests ADD COLUMN streams_json TEXT;
-- the checkpoint (its command) a continuation goes on from: where its streams start is that command's accepted
-- payload, nothing else
ALTER TABLE execution_requests ADD COLUMN input_checkpoint_command_id TEXT;
CREATE TABLE raw_identities(
 url TEXT PRIMARY KEY,
 size BIGINT NOT NULL,
 sha256 TEXT NOT NULL,
 chunk_bytes BIGINT NOT NULL,
 chunks_json TEXT NOT NULL,
 identified_at TEXT NOT NULL,
 attempt_id TEXT
);
-- raw files kept on a volatile store (<root>/raw/<sha256>) by the agent serving it, fetched and checked there before a
-- loader is handed them; evicted least recently used first when the store runs short (evictions.go)
CREATE TABLE raw_placements(
 store_id TEXT NOT NULL,
 root TEXT NOT NULL,
 sha256 TEXT NOT NULL,
 executor_id TEXT NOT NULL,
 bytes BIGINT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('ready','evicting')),
 created_at TEXT NOT NULL,
 last_used_at TEXT NOT NULL,
 PRIMARY KEY(store_id,sha256)
);
CREATE TABLE raw_records(
 sha256 TEXT NOT NULL,
 framing TEXT NOT NULL,
 records BIGINT NOT NULL,
 learned_at TEXT NOT NULL,
 PRIMARY KEY(sha256, framing)
);
