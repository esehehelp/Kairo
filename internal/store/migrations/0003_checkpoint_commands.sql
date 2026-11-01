-- A checkpoint command (`kairo task checkpoint`) asks a running attempt to
-- publish a checkpoint at its next safe point and carry on: unlike a suspend it
-- moves neither the attempt to quiescing nor its lease to releasing, and the
-- checkpoint it publishes is not the attempt's continuation. An attempt has at
-- most one live suspend (a checkpointed suspend stays live: the attempt is
-- ending) and at most one checkpoint in flight (a checkpointed one is done).
ALTER TABLE commands DROP CONSTRAINT commands_kind_check;
ALTER TABLE commands ADD CONSTRAINT commands_kind_check CHECK(kind IN('suspend','checkpoint'));
ALTER TABLE commands DROP CONSTRAINT commands_origin_check;
ALTER TABLE commands ADD CONSTRAINT commands_origin_check
 CHECK(origin IN('scope_pause','priority_preemption','node_quarantine','operator_checkpoint'));
ALTER TABLE commands ADD CONSTRAINT commands_kind_origin_check
 CHECK((kind='checkpoint') = (origin='operator_checkpoint'));
DROP INDEX one_live_suspend_per_attempt;
CREATE UNIQUE INDEX one_live_suspend_per_attempt ON commands(attempt_id)
 WHERE kind='suspend' AND state IN('pending','accepted','checkpointing','checkpointed');
CREATE UNIQUE INDEX one_live_checkpoint_per_attempt ON commands(attempt_id)
 WHERE kind='checkpoint' AND state IN('pending','accepted','checkpointing');
