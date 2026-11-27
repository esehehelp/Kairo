-- Gangs on the fabric (gang.go, issue #47 phase 3). Each placement of a gang (a generation) has a segment of its
-- own: its VNI, never used by an earlier generation (from a sequence, above the manual segments' range), and an
-- address on it for each member. The segment goes when the generation has ended.
ALTER TABLE fabric_segments DROP CONSTRAINT fabric_segments_purpose_check;
ALTER TABLE fabric_segments ADD CONSTRAINT fabric_segments_purpose_check CHECK(purpose IN ('manual','gang'));
CREATE SEQUENCE fabric_gang_vni START 100000 MAXVALUE 16777215;
ALTER TABLE gang_placements ADD COLUMN vni INTEGER;
ALTER TABLE gang_placements ADD COLUMN subnet TEXT NOT NULL DEFAULT '';
ALTER TABLE gang_placements ADD COLUMN mtu INTEGER NOT NULL DEFAULT 0;
ALTER TABLE gang_placement_leases ADD COLUMN address TEXT NOT NULL DEFAULT '';
