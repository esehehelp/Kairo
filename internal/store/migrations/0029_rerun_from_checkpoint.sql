-- A rerun from a checkpoint (kairo task rerun --from-checkpoint): the checkpoint command it goes on from and its
-- reference, handed to the rerun's execution as its continuation (node_id: the node that published it).
ALTER TABLE orchestration_task_reruns ADD COLUMN checkpoint_command_id TEXT;
ALTER TABLE orchestration_task_reruns ADD COLUMN continuation_ref TEXT;
