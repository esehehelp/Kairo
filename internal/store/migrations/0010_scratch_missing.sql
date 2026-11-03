-- A scratch directory its executor's report no longer finds under the
-- store's root (gone without Kairo deleting it) is missing: its bytes 0,
-- since when in missing_at. Found again, it is active (or kept, once
-- settled) again.
ALTER TABLE attempt_scratch DROP CONSTRAINT attempt_scratch_state_check;
ALTER TABLE attempt_scratch ADD CONSTRAINT attempt_scratch_state_check CHECK(state IN('active','kept','deleted','missing'));
ALTER TABLE attempt_scratch ADD COLUMN missing_at TEXT;

-- The bytes the last report found under the root in Kairo scratch
-- directories without a record (one whose recording failed), counted with
-- the kept ones.
ALTER TABLE store_observations ADD COLUMN scratch_unrecorded_bytes BIGINT NOT NULL DEFAULT 0;
