-- attempt_streams: what the loader of each stream an attempt reads said last (issue #43): where each rank stands,
-- what is made ahead for it, rates and how long its taker waited for data (loader.Status, JSON), reported by the
-- attempt's agent with its usage samples; task show prints it.
CREATE TABLE attempt_streams(
 attempt_id TEXT NOT NULL REFERENCES attempts(id),
 stream TEXT NOT NULL,
 status_json TEXT NOT NULL,
 observed_at TEXT NOT NULL,
 PRIMARY KEY(attempt_id, stream)
);
