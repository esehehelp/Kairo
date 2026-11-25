-- A network bench runs between two nodes' agents, with no task and no execution: the agent of the measuring node
-- (peer_node_id) measures the transfer endpoint of the other's (node_id) with net_token, which the daemon gives it and
-- the other agent checks with the daemon.
ALTER TABLE bench_runs ALTER COLUMN execution_id DROP NOT NULL;
ALTER TABLE bench_runs ADD COLUMN net_token TEXT;
