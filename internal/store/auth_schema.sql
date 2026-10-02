-- API authentication. Only token hashes are stored; the plaintext is shown
-- once when a token is created. Applied idempotently on every open.
--
-- api_tokens: operator ('admin', 'read') and node agent ('node', bound to one
-- node) credentials. A node token may be created before its node registers,
-- so node_id is not a foreign key.
CREATE TABLE IF NOT EXISTS api_tokens(
 id TEXT PRIMARY KEY,
 name TEXT NOT NULL,
 role TEXT NOT NULL CHECK(role IN('admin','read','node')),
 node_id TEXT,
 token_hash TEXT NOT NULL UNIQUE,
 created_at TEXT NOT NULL,
 last_used_at TEXT,
 revoked_at TEXT,
 CHECK((role='node')=(node_id IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS api_tokens_active_name ON api_tokens(name) WHERE revoked_at IS NULL;

-- attempt_tokens: the credential of one attempt's processes, issued with its
-- launch authorization. The lease and epoch it was issued for are what the
-- worker API acts under, so an attempt fenced since (epoch bumped) is refused.
CREATE TABLE IF NOT EXISTS attempt_tokens(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
 lease_id TEXT NOT NULL REFERENCES leases(id),
 coordination_epoch INTEGER NOT NULL,
 token_hash TEXT NOT NULL UNIQUE,
 issued_at TEXT NOT NULL
);
