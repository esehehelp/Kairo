-- A resource its owner uses (owneruse.go, issue #21): a claim of kind owner_use on a GPU, or on a node's cpu or ram.
-- A node's providers report it (held for a while after it was last seen); kairo node owner-use sets it by hand (id
-- owner-manual-<resource>, until owner-done or its until). Kairo places nothing on such a resource and suspends the
-- checkpointable work holding it (commands of origin owner_use).
ALTER TABLE external_claims DROP CONSTRAINT external_claims_claim_kind_check;
ALTER TABLE external_claims ADD CONSTRAINT external_claims_claim_kind_check CHECK(claim_kind IN('external_process','unattributed_activity','owner_use'));
ALTER TABLE commands DROP CONSTRAINT commands_origin_check;
ALTER TABLE commands ADD CONSTRAINT commands_origin_check
 CHECK(origin IN('scope_pause','priority_preemption','node_quarantine','operator_checkpoint','data_protection','owner_use'));
