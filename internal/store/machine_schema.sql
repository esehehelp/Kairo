-- Machine declaration (`kairo machine apply`): which node holds which GPU.
-- A GPU belongs to the nodes it is declared for; providers only observe. A
-- provider that reports a GPU declared for another node, or for none, does not
-- register it: the report is kept in undeclared_gpus for operators to see. A
-- GPU declared for several nodes is shared (one host's containers): each node
-- registers a row of its own and a lease on any of them excludes the others.
-- Applied idempotently on every open; Open seeds machine_gpus from the GPUs
-- already registered when it creates the table, and widens the key of a table
-- from before shared GPUs (uuid alone).
CREATE TABLE IF NOT EXISTS machine_gpus(
 uuid TEXT NOT NULL,
 node_id TEXT NOT NULL REFERENCES nodes(id),
 declared_at TEXT NOT NULL,
 PRIMARY KEY(uuid,node_id)
);
-- The GPUs a provider's latest poll reported but may not register.
CREATE TABLE IF NOT EXISTS undeclared_gpus(
 provider_id TEXT NOT NULL REFERENCES resource_providers(id),
 uuid TEXT NOT NULL,
 node_id TEXT NOT NULL REFERENCES nodes(id),
 first_observed_at TEXT NOT NULL,
 last_observed_at TEXT NOT NULL,
 PRIMARY KEY(provider_id,uuid)
);
