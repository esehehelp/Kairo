-- Dropped data (kairo data drop): an object Kairo no longer keeps the bytes of, only its identity (the file list with
-- each file's sha256): a corpus or mix made from raw data, or raw data fetched again from where it came from. Its
-- copies on durable stores and the places it was brought to are removed; it is temporal data from then on.
ALTER TABLE data_objects ADD COLUMN dropped_at TEXT;
