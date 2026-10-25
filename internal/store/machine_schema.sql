-- Machine declaration (`kairo machine apply`): which node holds which GPU.
-- A GPU belongs to the node it is declared for; providers only observe. A
-- provider that reports a GPU declared for another node, or for none, does not
-- register it: the report is kept in undeclared_gpus for operators to see.
-- Applied idempotently on every open; Open seeds machine_gpus from the GPUs
-- already registered when it creates the table.
CREATE TABLE IF NOT EXISTS machine_gpus(
 uuid TEXT PRIMARY KEY,
 node_id TEXT NOT NULL REFERENCES nodes(id),
 declared_at TEXT NOT NULL
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
