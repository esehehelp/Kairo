-- An import whose source is gone, or that an operator cancelled before it was
-- packed, is abandoned: nothing packs it any more (data_abandon.go).
ALTER TABLE data_objects DROP CONSTRAINT data_objects_state_check;
ALTER TABLE data_objects ADD CONSTRAINT data_objects_state_check CHECK(state IN('packing','packed','abandoned'));
-- A checkpoint Kairo asks a running attempt for, to keep its work (checkpoints.go).
ALTER TABLE commands DROP CONSTRAINT commands_origin_check;
ALTER TABLE commands ADD CONSTRAINT commands_origin_check
 CHECK(origin IN('scope_pause','priority_preemption','node_quarantine','operator_checkpoint','data_protection'));
ALTER TABLE commands DROP CONSTRAINT commands_kind_origin_check;
ALTER TABLE commands ADD CONSTRAINT commands_kind_origin_check
 CHECK((kind='checkpoint') = (origin IN('operator_checkpoint','data_protection')));
