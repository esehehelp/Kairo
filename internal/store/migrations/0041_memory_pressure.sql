-- Memory pressure (memorypressure.go, issue #45): while less RAM is available on a node than the floor kept free for
-- its owner, the work there that can checkpoint is suspended, one attempt at a time (commands of origin
-- memory_pressure). It is no claim of the owner's: who took the RAM is not known.
ALTER TABLE commands DROP CONSTRAINT commands_origin_check;
ALTER TABLE commands ADD CONSTRAINT commands_origin_check
 CHECK(origin IN('scope_pause','priority_preemption','node_quarantine','operator_checkpoint','data_protection','owner_use','memory_pressure'));
