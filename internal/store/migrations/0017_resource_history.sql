-- resource_history: each resource's observations, at most one row per
-- resource every 30 s (hardware_history.go), with what its live leases
-- reserved of it then. Kept for HistoryRetention (72 h), unlike
-- resource_observations (1 h); read by GET /api/hardware/samples. No foreign
-- key: resource rows are never deleted, and this table must never stop that.
CREATE TABLE resource_history(
 resource_id TEXT NOT NULL,
 sampled_at TIMESTAMPTZ NOT NULL,
 total_bytes BIGINT,
 free_bytes BIGINT,
 utilization DOUBLE PRECISION,
 reserved BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY(resource_id,sampled_at)
);
CREATE INDEX resource_history_sampled_idx ON resource_history(sampled_at);
-- device_history: the device samples (power, clocks) of each device, at most
-- one every 30 s, kept as long: what GET /api/hardware/samples reads instead
-- of device_samples, which hold one row per provider poll.
CREATE TABLE device_history(
 device TEXT NOT NULL,
 sampled_at TIMESTAMPTZ NOT NULL,
 power_w DOUBLE PRECISION,
 power_limit_w DOUBLE PRECISION,
 sm_clock_mhz DOUBLE PRECISION,
 mem_util DOUBLE PRECISION,
 PRIMARY KEY(device,sampled_at)
);
CREATE INDEX device_history_sampled_idx ON device_history(sampled_at);
