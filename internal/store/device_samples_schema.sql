-- Device power and operating point (device_samples.go). Applied idempotently on every
-- open.
--
-- device_samples: what a provider read of a device at one time. A device is
-- its physical identity (a GPU's UUID; a node's CPU package is that node's
-- cpu resource), so the providers of a shared GPU add to one series;
-- resource_id is the row whose provider reported the sample. Times are
-- fixed-width UTC (sampleStamp), so they order as text. A column is NULL when
-- the device or its driver does not report it.
CREATE TABLE IF NOT EXISTS device_samples(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 device TEXT NOT NULL,
 resource_id TEXT NOT NULL REFERENCES resource_instances(id),
 observed_at TEXT NOT NULL,
 valid_until TEXT NOT NULL,
 power_w REAL,
 power_limit_w REAL,
 sm_clock_mhz INTEGER,
 mem_clock_mhz INTEGER,
 pstate TEXT,
 event_reasons INTEGER,
 mem_util REAL,
 fan_pct REAL,
 pcie_gen INTEGER,
 pcie_width INTEGER
);
CREATE INDEX IF NOT EXISTS device_samples_device_idx ON device_samples(device,observed_at);
-- device_properties: what hardly changes of a device (a GPU's default, least
-- and greatest power limit, its greatest clocks and PCIe link), as last
-- reported.
CREATE TABLE IF NOT EXISTS device_properties(
 device TEXT PRIMARY KEY,
 default_power_limit_w REAL,
 min_power_limit_w REAL,
 max_power_limit_w REAL,
 max_sm_clock_mhz INTEGER,
 max_mem_clock_mhz INTEGER,
 pcie_gen_max INTEGER,
 pcie_width_max INTEGER,
 observed_at TEXT NOT NULL
);
-- attempt_energy: the energy the GPUs of an attempt's lease used while it
-- ran, added up as their samples arrive. gpu_seconds_covered is the
-- device-seconds integrated; partial is 1 once part of the run could not be
-- (a gap between samples, a sample without power, a run Kairo lost track of).
CREATE TABLE IF NOT EXISTS attempt_energy(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
 gpu_joules REAL NOT NULL DEFAULT 0,
 gpu_seconds_covered REAL NOT NULL DEFAULT 0,
 partial INTEGER NOT NULL DEFAULT 0 CHECK(partial IN(0,1)),
 updated_at TEXT NOT NULL
);
-- Finding the leases of a device (every sample, and every sameDevice check)
-- starts from its resource rows.
CREATE INDEX IF NOT EXISTS lease_items_resource_idx ON lease_items(resource_id);
