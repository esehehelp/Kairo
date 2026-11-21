-- A rerun from a checkpoint (kairo task rerun --from-checkpoint): the checkpoint command it goes on from and its
-- reference, handed to the rerun's execution as its continuation (node_id: the node that published it).
ALTER TABLE orchestration_task_reruns ADD COLUMN checkpoint_command_id TEXT;
ALTER TABLE orchestration_task_reruns ADD COLUMN continuation_ref TEXT;
-- A task declared continue_after_loss goes on after a lost attempt by a decision of its own reason.
ALTER TABLE orchestration_decisions DROP CONSTRAINT orchestration_decisions_reason_check;
ALTER TABLE orchestration_decisions ADD CONSTRAINT orchestration_decisions_reason_check
 CHECK(reason IN('initial','continuation','quarantine_restart','gang_restart','rerun','loss_continuation'));
