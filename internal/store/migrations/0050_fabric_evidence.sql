-- Placement from the fabric's evidence (fabric_evidence.go, issue #47 phase 5). A node may say its uplink is metered;
-- a gang may say what its members' links must be (a round trip at most, a bandwidth at least, metered links or not),
-- and is placed only where measurements say they are. A link without the measurement is asked for one: its
-- destination fetches a sample from its source (fabric_measure_requests).
ALTER TABLE fabric_nodes ADD COLUMN metered BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE execution_gangs ADD COLUMN max_rtt_ms DOUBLE PRECISION;
ALTER TABLE execution_gangs ADD COLUMN min_bytes_per_second DOUBLE PRECISION;
ALTER TABLE execution_gangs ADD COLUMN metered BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE fabric_measure_requests(
 src_node TEXT NOT NULL REFERENCES nodes(id),
 dst_node TEXT NOT NULL REFERENCES nodes(id),
 requested_at TEXT NOT NULL,
 PRIMARY KEY(src_node, dst_node)
);
