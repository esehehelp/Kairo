-- A GPU is in at most one live lease, by device: a GPU two nodes share (two
-- containers of one host) has a resource row on each node, and a lease of
-- either row holds the device. The guard compared resource ids only, so two
-- ranks of one gang placement could each lease the same device through its
-- two rows.
CREATE OR REPLACE FUNCTION exclusive_resource_lease_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.resource_id IS NOT NULL AND NEW.kind='gpu' AND EXISTS(
  SELECT 1 FROM lease_items li JOIN leases l ON l.id=li.lease_id
  WHERE li.kind='gpu' AND li.resource_id IN(
   SELECT sib.id FROM resource_instances sib JOIN resource_instances self ON self.id=NEW.resource_id AND self.kind='gpu'
   WHERE sib.kind='gpu' AND sib.stable_identity=self.stable_identity
   UNION SELECT NEW.resource_id)
  AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested')) THEN
  RAISE EXCEPTION 'resource already leased';
 END IF;
 RETURN NEW;
END $$;
