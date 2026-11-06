-- A data object's copy on a durable store: its pack and file list at
-- <root>/objects/<YYYYMM>/<object id>.tar.zst (the month the object was
-- made). copying from when the store's executor is told to copy it (taken by
-- one executor) until it reports the copy read back from disk and matching
-- the pack's size and sha256; verified then. A copy fetched from another
-- node's agent is read with a token: only its hash is kept, with when it
-- expires.
CREATE TABLE object_copies(
 object_id TEXT NOT NULL REFERENCES data_objects(id),
 store_id TEXT NOT NULL,
 executor_id TEXT NOT NULL,
 root TEXT NOT NULL,
 month TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('copying','verified')),
 path TEXT,
 manifest_path TEXT,
 token_hash TEXT,
 token_expires_at TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 verified_at TEXT,
 PRIMARY KEY(object_id,store_id)
);
CREATE INDEX object_copies_executor_idx ON object_copies(executor_id,state);
