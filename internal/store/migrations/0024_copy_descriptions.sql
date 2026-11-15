-- 2026-11-16: a durable copy is described beside itself (<object id>.tar.zst.object.json: whose it is, what it
-- holds), so what a durable store holds can be known without the database; described_at is when its
-- description was written there (copies.go).
ALTER TABLE object_copies ADD COLUMN described_at TEXT;
