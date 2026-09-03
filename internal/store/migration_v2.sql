BEGIN IMMEDIATE;

ALTER TABLE queue_revisions RENAME TO queue_revisions_v1;
CREATE TABLE queue_revisions(
 queue_id TEXT NOT NULL REFERENCES queues(id), revision INTEGER NOT NULL, digest TEXT NOT NULL,
 normalized_json TEXT NOT NULL, source_text TEXT NOT NULL, actor TEXT NOT NULL, request_id TEXT NOT NULL,
 created_at TEXT NOT NULL, PRIMARY KEY(queue_id,revision), UNIQUE(queue_id,request_id));
INSERT INTO queue_revisions(queue_id,revision,digest,normalized_json,source_text,actor,request_id,created_at)
 SELECT queue_id,revision,digest,normalized_json,source_text,actor,request_id,created_at FROM queue_revisions_v1;
DROP TABLE queue_revisions_v1;

DROP TRIGGER exclusive_resource_lease_guard;
CREATE TRIGGER exclusive_resource_lease_guard BEFORE INSERT ON lease_items
WHEN NEW.resource_id IS NOT NULL AND NEW.kind='gpu' AND EXISTS(
 SELECT 1 FROM lease_items li JOIN leases l ON l.id=li.lease_id
 WHERE li.resource_id=NEW.resource_id AND li.kind='gpu'
 AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested'))
BEGIN SELECT RAISE(ABORT,'resource already leased'); END;

UPDATE lease_items
SET resource_id=(
 SELECT ri.id
 FROM leases l
 JOIN executors e ON e.id=l.executor_id
 JOIN resource_instances ri ON ri.node_id=e.node_id AND ri.kind=lease_items.kind
 WHERE l.id=lease_items.lease_id
 AND (lease_items.kind!='disk' OR ri.stable_identity=lease_items.filesystem)
 ORDER BY ri.id
 LIMIT 1)
WHERE resource_id IS NULL;

INSERT INTO schema_migrations VALUES(2, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
COMMIT;
