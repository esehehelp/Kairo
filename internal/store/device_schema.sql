-- Device settings: the desired settings of a host's device (a GPU by UUID, or
-- "cpu"), set by an owner, and what the host's controller reports of it.
-- Applied idempotently on every open.
--
-- One setting per owner kind; the effective one is manual, else bench, else
-- policy. A bench setting, and a GPU's policy setting, belongs to a lease and
-- is removed when the lease is released, so no setting outlives its lease. A
-- host's CPU is shared by whatever runs on it: its policy setting belongs to
-- no lease and is removed when the host's policy no longer allows it. No
-- setting: the hardware's defaults.
CREATE TABLE IF NOT EXISTS device_settings(
 host_id TEXT NOT NULL,
 device_id TEXT NOT NULL,
 owner_kind TEXT NOT NULL CHECK(owner_kind IN('manual','bench','policy')),
 owner_ref TEXT NOT NULL,
 lease_id TEXT REFERENCES leases(id),
 settings_json TEXT NOT NULL,
 requested_by TEXT NOT NULL,
 requested_at TEXT NOT NULL,
 PRIMARY KEY(host_id,device_id,owner_kind),
 CHECK(owner_kind='policy' OR (owner_kind='manual')=(lease_id IS NULL))
);
CREATE INDEX IF NOT EXISTS device_settings_lease_idx ON device_settings(lease_id);
-- A device's generation counts changes of its effective setting; the
-- controller reports the generation it applied, what is in place (applied_json,
-- '{}' for the defaults), its latest failure, and what the device supports.
CREATE TABLE IF NOT EXISTS device_state(
 host_id TEXT NOT NULL,
 device_id TEXT NOT NULL,
 generation INTEGER NOT NULL DEFAULT 0,
 applied_generation INTEGER,
 applied_json TEXT,
 applied_note TEXT,
 applied_at TEXT,
 last_error TEXT,
 last_error_at TEXT,
 capabilities_json TEXT,
 capabilities_at TEXT,
 reported_at TEXT,
 PRIMARY KEY(host_id,device_id)
);
-- Controllers (`kairo controller`) by node: when each last polled.
CREATE TABLE IF NOT EXISTS device_controllers(
 node_id TEXT PRIMARY KEY REFERENCES nodes(id),
 version TEXT NOT NULL DEFAULT '',
 started_at TEXT NOT NULL,
 last_seen_at TEXT NOT NULL
);
