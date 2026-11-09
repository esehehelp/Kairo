-- Temporal data brought to another node (transfer.go): an execution waiting
-- on a node for temporal data present on another has it packed there for the
-- move (transfer_state packing, then packed: pack_path, pack_bytes and
-- pack_sha256 say where and what), brought as a durable object is (its
-- placements), and the pack removed once no execution waits for it
-- elsewhere (dropping, then none). Nothing is copied to a durable store.
ALTER TABLE data_objects ADD COLUMN transfer_state TEXT CHECK(transfer_state IN('packing','packed','dropping'));
