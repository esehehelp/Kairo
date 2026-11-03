-- The lease guard of 0005 refuses a second live lease of one physical GPU
-- (by UUID, through any node's row) from now on; one that already exists
-- was let in before it. Such a pair means two holders of one device:
-- migrating stops here and names them, so that they are resolved (one
-- released, or reconciled) before the daemon runs with the guard.
DO $$
DECLARE
 duplicates TEXT;
BEGIN
 SELECT string_agg(format('%s (leases %s)', identity, leases), '; ' ORDER BY identity) INTO duplicates
 FROM (
  SELECT r.stable_identity AS identity, string_agg(DISTINCT l.id, ', ') AS leases
  FROM lease_items li JOIN leases l ON l.id=li.lease_id JOIN resource_instances r ON r.id=li.resource_id
  WHERE li.kind='gpu' AND r.kind='gpu'
  AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested')
  GROUP BY r.stable_identity HAVING COUNT(DISTINCT l.id)>1
 ) d;
 IF duplicates IS NOT NULL THEN
  RAISE EXCEPTION 'GPUs held by more than one live lease: %', duplicates;
 END IF;
END $$;
