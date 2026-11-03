-- A scratch its executor has claimed for deletion (and is deleting) is
-- deleting until it is settled deleted, or kept when the deletion is refused;
-- meanwhile a machine declaration under which it would not be deleted is
-- refused.
ALTER TABLE attempt_scratch DROP CONSTRAINT attempt_scratch_state_check;
ALTER TABLE attempt_scratch ADD CONSTRAINT attempt_scratch_state_check CHECK(state IN('active','kept','deleted','missing','deleting'));
