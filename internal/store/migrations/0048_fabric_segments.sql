-- Routing and segments of the fabric (fabric_segments.go, issue #47 phase 2). A node that runs FRR says so (routing);
-- the route reflectors are chosen among those. A segment is an L2 network over VXLAN (its VNI) on some of them, its
-- IPv4 subnet unique among segments (a wslc VM refuses two networks that overlap) and its MTU under the fabric's.
ALTER TABLE fabric_nodes ADD COLUMN routing BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE fabric_segments(
 vni INTEGER PRIMARY KEY CHECK(vni BETWEEN 1 AND 16777215),
 subnet TEXT NOT NULL UNIQUE,
 mtu INTEGER NOT NULL CHECK(mtu BETWEEN 1280 AND 9000),
 purpose TEXT NOT NULL CHECK(purpose IN ('manual')),
 created_at TEXT NOT NULL
);
CREATE TABLE fabric_segment_members(
 vni INTEGER NOT NULL REFERENCES fabric_segments(vni) ON DELETE CASCADE,
 node_id TEXT NOT NULL REFERENCES nodes(id),
 PRIMARY KEY(vni, node_id)
);
