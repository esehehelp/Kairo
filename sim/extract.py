"""Extract a scheduling trace from a Kairo database snapshot.

Reads the restored snapshot (default: database kairo_sim in the local test
container kairo-pg-test) through `docker exec ... psql` and writes one JSON
file with the nodes, the GPUs, the executors and every job:

- a job is an orchestrated Task (all its executions, reruns included) or a
  direct execution that belongs to no Task;
- arrival is when the Task was declared (or the execution submitted);
- work is the wall time its attempts ran, summed over every execution;
- deps are the Tasks it waited for.

    python sim/extract.py --out local/sim/trace.json
"""

import argparse
import json
import os
import subprocess
from datetime import datetime


def query(container, database, sql):
    env = dict(os.environ, MSYS_NO_PATHCONV="1")
    out = subprocess.run(
        ["docker", "exec", "-i", container, "psql", "-U", "kairo", "-d", database, "-At", "-v", "ON_ERROR_STOP=1"],
        input=sql, capture_output=True, text=True, encoding="utf-8", env=env, check=True,
    ).stdout.strip()
    return json.loads(out) if out else []


def ts(value):
    if not value:
        return None
    # RFC 3339 with up to 9 fractional digits; Python takes 6.
    head, _, tail = value.rstrip("Z").partition(".")
    frac = (tail + "000000")[:6] if tail else "000000"
    return datetime.fromisoformat(f"{head}.{frac}+00:00").timestamp()


EXECUTIONS = """
select json_agg(x) from (
  select e.id, p.external_key as project, e.task_scope_id as task, e.priority, e.checkpointable, e.preemptible,
         e.executor_selector_json as selector, e.submitted_at, e.started_at, e.terminal_at, e.terminal_cause,
         e.input_continuation_ref is not null as continuation, e.argv_json as argv, q.external_key as queue,
         (select json_object_agg(r.kind, r.quantity) from resource_requests r
            where r.execution_id = e.id and r.kind in ('gpu','cpu','ram')) as demand,
         (select json_agg(json_build_array(a.started_at, a.exited_at, a.exit_code) order by a.started_at)
            from attempts a where a.execution_id = e.id and a.started_at is not null) as attempts,
         gm.gang_id, gm.rank
  from execution_requests e
  join coordination_scopes p on p.id = e.project_scope_id
  left join coordination_scopes q on q.id = e.queue_scope_id
  left join gang_members gm on gm.execution_id = e.id
) x;
"""

TASKS = """
select json_agg(x) from (
  select t.task_scope_id as id, p.external_key as project, t.name, t.state, t.created_at,
         coalesce((select json_agg(d.dependency_task_scope_id) from orchestration_task_dependencies d
                   where d.task_scope_id = t.task_scope_id), '[]') as deps,
         coalesce((select json_agg(o.execution_id order by o.execution_ordinal) from orchestration_task_executions o
                   where o.task_scope_id = t.task_scope_id), '[]') as executions
  from orchestration_tasks t join coordination_scopes p on p.id = t.project_scope_id
) x;
"""

EXECUTORS = "select json_agg(x) from (select id, node_id, attributes_json from executors) x;"

CAPACITY = """
select json_agg(x) from (
  select r.node_id as node, r.kind, r.stable_identity as identity,
         (select o.total_bytes from resource_observations o where o.resource_id = r.id
          order by o.observed_at desc limit 1) as total
  from resource_instances r where r.kind in ('gpu','cpu','ram')
) x;
"""


GPU_HISTORY = """
select json_agg(x) from (
  select ex.node_id as node, split_part(regexp_replace(li.resource_id, '^gpu-', 'GPU-'), '@', 1) as device,
         li.resource_id like '%@%' as shared, min(l.created_at) as first, max(l.created_at) as last
  from lease_items li join leases l on l.id = li.lease_id join executors ex on ex.id = l.executor_id
  where li.kind = 'gpu' group by 1, 2, 3
) x;
"""


def gpu_membership(history, trace_start):
    """When each GPU belonged to which node, from the leases that used it.

    A GPU that moved is taken to have moved halfway between its last lease on
    the old node and its first on the new one; one first used more than two
    days after the trace starts appeared at its first lease. A shared row
    (another node's view of a GPU) counts from its first lease."""
    out = []
    by_device = {}
    for h in history:
        if h["shared"]:
            out.append({"device": h["device"], "node": h["node"], "start": ts(h["first"]), "end": None})
        else:
            by_device.setdefault(h["device"], []).append(h)
    for device, segs in by_device.items():
        segs.sort(key=lambda h: h["first"])
        starts = []
        for k, h in enumerate(segs):
            if k == 0:
                first = ts(h["first"])
                starts.append(trace_start if first - trace_start < 2 * 86400 else first)
            else:
                starts.append((ts(segs[k - 1]["last"]) + ts(h["first"])) / 2)
        for k, h in enumerate(segs):
            out.append({"device": device, "node": h["node"], "start": starts[k],
                        "end": starts[k + 1] if k + 1 < len(segs) else None})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--container", default="kairo-pg-test")
    ap.add_argument("--database", default="kairo_sim")
    ap.add_argument("--out", default="local/sim/trace.json")
    args = ap.parse_args()

    execs = {e["id"]: e for e in query(args.container, args.database, EXECUTIONS)}
    tasks = query(args.container, args.database, TASKS)
    executors = query(args.container, args.database, EXECUTORS)
    capacity = query(args.container, args.database, CAPACITY)

    nodes = {}
    for c in capacity:
        n = nodes.setdefault(c["node"], {"cpu": 0, "ram": 0, "gpus": []})
        if c["kind"] == "gpu":
            n["gpus"].append(c["identity"])
        else:
            n[c["kind"]] = c["total"] or 0

    # Gang members by gang, so a Task's leader brings its other ranks.
    gangs = {}
    for e in execs.values():
        if e["gang_id"]:
            gangs.setdefault(e["gang_id"], []).append(e)

    def part(e):
        demand = e["demand"] or {}
        return {
            "selector": json.loads(e["selector"]) if e["selector"] else {},
            "gpu": int(demand.get("gpu", 0)),
            "cpu": int(demand.get("cpu", 0)),
            "ram": int(demand.get("ram", 0)),
        }

    def run_seconds(e):
        total = 0.0
        for started, exited, _ in e["attempts"] or []:
            if started and exited:
                total += max(0.0, ts(exited) - ts(started))
        return total

    jobs = []
    covered = set()
    for t in tasks:
        ex = [execs[i] for i in t["executions"] if i in execs]
        covered.update(t["executions"])
        parts, work, first, last_end, argv, queue = None, 0.0, None, None, None, t.get("queue")
        priority, ckpt, preempt = 0, False, False
        for e in ex:
            ranks = sorted(gangs.get(e["gang_id"], [e]), key=lambda m: m["rank"] or 0) if e["gang_id"] else [e]
            covered.update(m["id"] for m in ranks)
            if argv is None:
                argv, queue = json.loads(e["argv"] or "[]"), e["queue"]
            if parts is None and e["demand"]:
                parts = [part(m) for m in ranks]
                priority, ckpt, preempt = e["priority"], e["checkpointable"], e["preemptible"]
            work += run_seconds(e)
            if e["started_at"]:
                s = ts(e["started_at"])
                first = s if first is None else min(first, s)
            for _, exited, _ in e["attempts"] or []:
                if exited:
                    last_end = max(last_end or 0, ts(exited))
        jobs.append({
            "id": t["id"], "project": t["project"], "name": t["name"], "state": t["state"],
            "arrival": ts(t["created_at"]), "deps": t["deps"], "parts": parts or [],
            "work": work, "priority": priority, "checkpointable": ckpt, "preemptible": preempt,
            "actual_start": first, "actual_end": last_end, "executions": len(ex),
            "argv": argv or [], "queue": queue or "",
        })

    for e in execs.values():
        if e["id"] in covered or (e["gang_id"] and e["rank"]):
            continue
        ranks = sorted(gangs.get(e["gang_id"], [e]), key=lambda m: m["rank"] or 0) if e["gang_id"] else [e]
        ends = [ts(x) for _, x, _ in (e["attempts"] or []) if x]
        jobs.append({
            "id": e["id"], "project": e["project"], "name": e["id"], "state": e["terminal_cause"] or "",
            "arrival": ts(e["submitted_at"]), "deps": [], "parts": [part(m) for m in ranks],
            "work": run_seconds(e), "priority": e["priority"], "checkpointable": e["checkpointable"],
            "preemptible": e["preemptible"], "actual_start": ts(e["started_at"]),
            "actual_end": max(ends) if ends else None, "executions": 1,
            "argv": json.loads(e["argv"] or "[]"), "queue": e["queue"] or "",
        })

    trace_start = min(j["arrival"] for j in jobs if j["arrival"])
    trace = {
        "nodes": nodes,
        "gpu_membership": gpu_membership(query(args.container, args.database, GPU_HISTORY), trace_start),
        "executors": [{"id": x["id"], "node": x["node_id"], "attributes": json.loads(x["attributes_json"])} for x in executors],
        "jobs": jobs,
    }
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(trace, f)
    ran = [j for j in jobs if j["work"] > 0]
    print(f"{len(jobs)} jobs ({len(ran)} ran, {sum(j['work'] for j in ran) / 3600:.0f} h of work), "
          f"{sum(len(j['deps']) for j in jobs)} dependency edges, {len(nodes)} nodes -> {args.out}")


if __name__ == "__main__":
    main()
