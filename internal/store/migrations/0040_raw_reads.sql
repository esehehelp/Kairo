-- raw_reads: a read of a kept raw file from another node's agent (issue #43, rawpeers.go): the daemon issues a token for
-- one file (sha256) kept on one store of one node, to the raw cache of a node whose leased executions' streams read
-- it; the keeping node's agent asks the daemon before serving it (GET /raw/<sha256> at its transfer endpoint).
CREATE TABLE raw_reads(
 token_hash TEXT PRIMARY KEY,
 sha256 TEXT NOT NULL,
 node_id TEXT NOT NULL,
 store_id TEXT NOT NULL,
 root TEXT NOT NULL,
 reader_executor_id TEXT NOT NULL,
 created_at TEXT NOT NULL,
 expires_at TEXT NOT NULL
);
CREATE INDEX raw_reads_expires ON raw_reads(expires_at);
