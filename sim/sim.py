"""Discrete-event replay of a Kairo trace under different admission policies.

Every job of the trace (see extract.py) arrives when it was declared, becomes
ready once its dependencies have finished, and runs for the wall time it
really ran. Nodes, GPUs (one device shared by the nodes that list it), CPU and
RAM are reserved as Kairo reserves them. After every batch of events one
planning pass runs, as Kairo's planner does:

    order the ready jobs -> place each on its best node -> a job that fits
    nowhere either preempts (if the policy allows) or holds, on the node it
    would run on, what it can already use there.

Policies:
    current  Kairo today: priority, then project dominant share, then
             submission; preempts lower-priority checkpointable work whenever
             a job fits nowhere.
    deps     priority replaced by the dependency structure: negative-priority
             work is background (runs last, yields at once, never preempts);
             the rest is ordered by critical path (own work + longest chain of
             dependants) and preempts another batch job only when the two
             chains finish earlier by more than margin x (stop + restart).
    fifo     submission order, no preemption.

Durations are taken from the trace (the policy knows them exactly unless
--noise is given). Pauses, outages and failures of the real run are not
replayed: compare policies with each other, not with the real timeline.

    python sim/sim.py local/sim/trace.json --policy current deps fifo
"""

import argparse
import heapq
import json
import math
import random
import statistics
from collections import defaultdict

STOP_COST = 10.0  # s, suspend request -> lease released (measured median ~3-33 s)


class Part:
    __slots__ = ("gpu", "cpu", "ram", "nodes")

    def __init__(self, p, executors, node_names):
        self.gpu, self.cpu, self.ram = p["gpu"], p["cpu"], p["ram"]
        sel = p.get("selector") or {}
        labels, node = sel.get("labels") or {}, sel.get("node")
        self.nodes = sorted({
            x["node"] for x in executors
            if (not node or x["node"] == node)
            and all(str(x["attributes"].get(k)) == str(v) for k, v in labels.items())
        }) or sorted(node_names)


class Job:
    def __init__(self, j, executors, node_names):
        self.id, self.project, self.name = j["id"], j["project"], j["name"]
        self.arrival, self.work = j["arrival"], j["work"]
        self.deps = j["deps"]
        self.dependants = []
        self.parts = [Part(p, executors, node_names) for p in j["parts"]] or [Part({"gpu": 0, "cpu": 0, "ram": 0}, executors, node_names)]
        self.priority = j["priority"]
        self.cooperative = bool(j["checkpointable"] and j["preemptible"])
        self.background = self.priority < 0
        self.gang = len(self.parts) > 1
        self.actual_start, self.actual_end = j["actual_start"], j["actual_end"]
        self.reset()

    def reset(self):
        self.state = "future"  # future -> waiting -> ready -> running -> done
        self.remaining = self.work
        self.ready_at = self.start = self.end = None
        self.run_token = 0
        self.alloc = None  # [(node, devices, cpu, ram)]
        self.segment_start = None
        self.suspending = False
        self.preemptions = 0
        self.overhead = 0.0
        self.pending_deps = 0


class Sim:
    def __init__(self, trace, policy, restart_cost, margin, noise, seed, min_run=900.0, hold="all", estimates=None):
        self.policy, self.restart, self.margin, self.min_run = policy, restart_cost, margin, min_run
        self.hold_mode = hold
        self.node_names = list(trace["nodes"])
        self.cap = {n: {"cpu": v["cpu"], "ram": v["ram"], "gpus": v["gpus"]} for n, v in trace["nodes"].items()}
        # Which GPU sat in which node when (they moved during the trace).
        self.membership = trace.get("gpu_membership") or []
        t0 = min(j["arrival"] for j in trace["jobs"])
        self.set_inventory(t0)
        self.jobs = {j["id"]: Job(j, trace["executors"], self.node_names) for j in trace["jobs"]}
        for j in self.jobs.values():
            j.deps = [d for d in j.deps if d in self.jobs]
            for d in j.deps:
                self.jobs[d].dependants.append(j)
        rng = random.Random(seed)
        if estimates:
            default = estimates["default"]
            self.estimate = {j.id: estimates["estimates"].get(j.id, default) for j in self.jobs.values()}
        else:
            self.estimate = {j.id: (j.work * math.exp(rng.gauss(0, noise)) if noise else j.work) for j in self.jobs.values()}
        # DAG (weakly connected component) of each job and the estimated work it has left.
        self.comp = {}
        for g in components(self.jobs, keep_empty=True):
            root = g[0].id
            for x in g:
                self.comp[x.id] = root
        self.comp_left = defaultdict(float)
        for x in self.jobs.values():
            self.comp_left[self.comp[x.id]] += self.estimate[x.id]
        self.chain = {}  # critical path from a job's start: own estimate + longest dependant chain
        for j in sorted(self.jobs.values(), key=lambda x: -x.arrival):
            self._chain(j)
        self.holder = {}  # device -> job id
        self.cpu_used = defaultdict(int)
        self.ram_used = defaultdict(int)
        self.events, self.seq = [], 0
        self.ready = set()
        self.running = set()
        self.preemptions = 0
        self.totals = {"gpu": len({d for c in self.cap.values() for d in c["gpus"]}) or 1,
                       "cpu": sum(c["cpu"] for c in self.cap.values()) or 1,
                       "ram": sum(c["ram"] for c in self.cap.values()) or 1}

    def _chain(self, j):
        if j.id in self.chain:
            return self.chain[j.id]
        stack = [(j, False)]
        while stack:
            x, done = stack.pop()
            if x.id in self.chain:
                continue
            if done:
                self.chain[x.id] = self.estimate[x.id] + max((self.chain[d.id] for d in x.dependants), default=0.0)
                continue
            stack.append((x, True))
            stack.extend((d, False) for d in x.dependants if d.id not in self.chain)
        return self.chain[j.id]

    # ---- events -------------------------------------------------------
    def push(self, t, kind, job, token=0):
        self.seq += 1
        heapq.heappush(self.events, (t, self.seq, kind, job.id if job else None, token))

    def set_inventory(self, t):
        if not self.membership:
            return
        for node, c in self.cap.items():
            c["gpus"] = sorted(m["device"] for m in self.membership
                               if m["node"] == node and m["start"] <= t and (m["end"] is None or t < m["end"]))

    def run(self):
        for j in self.jobs.values():
            j.pending_deps = len(j.deps)
            self.push(j.arrival, "arrive", j)
        t0 = min(j.arrival for j in self.jobs.values())
        for m in self.membership:
            for t in (m["start"], m["end"]):
                if t is not None and t > t0:
                    self.push(t, "inventory", None)
        while self.events:
            now = self.events[0][0]
            while self.events and self.events[0][0] == now:
                _, _, kind, jid, token = heapq.heappop(self.events)
                self.handle(now, kind, self.jobs.get(jid), token)
            self.plan(now)
        return self

    def handle(self, now, kind, j, token):
        if kind == "inventory":
            self.set_inventory(now)
        elif kind == "arrive":
            j.state = "waiting"
            self.maybe_ready(now, j)
        elif kind == "finish" and token == j.run_token and j.state == "running" and not j.suspending:
            self.release(j)
            j.state, j.end, j.remaining = "done", now, 0.0
            self.comp_left[self.comp[j.id]] -= self.estimate[j.id]
            for d in j.dependants:
                d.pending_deps -= 1
                self.maybe_ready(now, d)
        elif kind == "stopped" and token == j.run_token:
            self.release(j)
            j.suspending = False
            j.state, j.ready_at = "ready", now  # its continuation is submitted now
            self.ready.add(j.id)

    def maybe_ready(self, now, j):
        if j.state != "waiting" or j.pending_deps > 0:
            return
        if j.work <= 0:  # never ran for real: passes through
            j.state, j.start, j.end = "done", now, now
            for d in j.dependants:
                d.pending_deps -= 1
                self.maybe_ready(now, d)
            return
        j.state, j.ready_at = "ready", now
        self.ready.add(j.id)

    def release(self, j):
        self.running.discard(j.id)
        for node, devices, cpu, ram in j.alloc or []:
            for d in devices:
                self.holder.pop(d, None)
            self.cpu_used[node] -= cpu
            self.ram_used[node] -= ram
        j.alloc = None

    # ---- capacity -------------------------------------------------------
    # A pass's view holds only the holds it has handed out: placements go
    # straight into the real state. A hold reserves resources from `until` on
    # (the blocked job's estimated start); a job that ends by then may use them.
    def free(self, node, view, freed=frozenset(), end=None):
        held_devices, held_cpu, held_ram = set(), 0, 0
        for h in view["holds"]:
            if h["node"] == node and (end is None or end > h["until"]):
                held_devices |= h["devices"]
                held_cpu += h["cpu"]
                held_ram += h["ram"]
        devices = [d for d in self.cap[node]["gpus"]
                   if (self.holder.get(d) is None or self.holder[d] in freed) and d not in held_devices]
        cpu = self.cap[node]["cpu"] - self.cpu_used[node] - held_cpu
        ram = self.cap[node]["ram"] - self.ram_used[node] - held_ram
        for jid in freed:
            for n, _, c, r in self.jobs[jid].alloc or []:
                if n == node:
                    cpu += c
                    ram += r
        return devices, cpu, ram

    def fits(self, part, node, view, freed=frozenset(), cpu_matters=True, end=None):
        devices, cpu, ram = self.free(node, view, freed, end)
        return len(devices) >= part.gpu and (not cpu_matters or cpu >= part.cpu) and ram >= part.ram

    def could_ever(self, part, node):
        c = self.cap[node]
        return len(c["gpus"]) >= part.gpu and c["cpu"] >= part.cpu and c["ram"] >= part.ram

    def assign(self, j, view, test):
        """Distinct nodes for every part, all or nothing; best node first."""
        order = sorted(range(len(j.parts)), key=lambda i: len(j.parts[i].nodes))
        chosen = {}

        def walk(k, used):
            if k == len(order):
                return True
            part = j.parts[order[k]]
            options = [n for n in part.nodes if n not in used and test(part, n)]
            options.sort(key=lambda n: self.rank_node(part, n, view))
            for n in options:
                chosen[order[k]] = n
                if walk(k + 1, used | {n}):
                    return True
            return False

        return [chosen[i] for i in range(len(j.parts))] if walk(0, frozenset()) else None

    def rank_node(self, part, node, view):
        devices, cpu, _ = self.free(node, view)
        return (len(devices) - part.gpu, -(cpu - part.cpu) / max(1, self.cap[node]["cpu"]), node)

    def allocation(self, j, nodes, view, end):
        out = []
        for part, node in zip(j.parts, nodes):
            devices, _, _ = self.free(node, view, end=end)
            out.append((node, devices[:part.gpu], part.cpu, part.ram))
        return out

    def est_end(self, x, now):
        if x.suspending:
            return now + STOP_COST
        elapsed = now - x.segment_start
        left = self.left(x) - elapsed
        if left <= 0:
            # Past its estimate: expect it to run about as long again as it has.
            left = max(60.0, elapsed)
        return now + left

    def shadow(self, j, nodes, view, now):
        """When j could start on nodes if running work ends as estimated."""
        if self.hold_mode == "all":
            return now  # Kairo today: held from now on
        when = now
        for part, node in zip(j.parts, nodes):
            devices, cpu, ram = self.free(node, view)
            devices = set(devices)
            ends = []
            for x in self.running_jobs():
                for n, ds, c, r in x.alloc:
                    on_node = set(ds) & set(self.cap[node]["gpus"])
                    if n == node or on_node:
                        ends.append((self.est_end(x, now), on_node, c if n == node else 0, r if n == node else 0))
            ends.sort(key=lambda e: e[0])
            ok = len(devices) >= part.gpu and cpu >= part.cpu and ram >= part.ram
            t = now if ok else math.inf
            for e, ds, c, r in ends:
                if t < math.inf:
                    break
                devices |= ds
                cpu += c
                ram += r
                if len(devices) >= part.gpu and cpu >= part.cpu and ram >= part.ram:
                    t = e
            when = max(when, t)
        return now if when == math.inf else when  # no estimate frees it: held from now on

    def hold(self, j, nodes, view, until):
        for part, node in zip(j.parts, nodes):
            devices, cpu, ram = self.free(node, view)
            view["holds"].append({"node": node, "devices": set(devices[:part.gpu]),
                                  "cpu": max(0, min(part.cpu, cpu)), "ram": max(0, min(part.ram, ram)),
                                  "until": until})

    # ---- policy -----------------------------------------------------------
    def order(self, now):
        ready = [self.jobs[i] for i in self.ready]
        if self.policy == "fifo":
            return sorted(ready, key=lambda j: (j.ready_at, j.id))
        if self.policy == "deps":
            return sorted(ready, key=lambda j: (j.background, -self.chain[j.id], j.ready_at, j.id))
        if self.policy == "dag":
            # Across DAGs the one with the least work left first; inside one,
            # the longest chain of dependants first.
            return sorted(ready, key=lambda j: (j.background, self.comp_left[self.comp[j.id]], -self.chain[j.id], j.ready_at, j.id))
        # current: priority, then dominant share of the project, recomputed as the pass places work
        return ready

    def share(self, held, project):
        h = held.get(project)
        if not h:
            return 0.0
        return max(h["gpu"] / self.totals["gpu"], h["cpu"] / self.totals["cpu"], h["ram"] / self.totals["ram"])

    def may_preempt(self, j, victim, now):
        if victim.suspending or not victim.cooperative or victim.state != "running" or victim is j:
            return False
        if self.policy == "current":
            return victim.priority < j.priority
        if self.policy in ("deps", "dag"):
            if j.background:
                return False
            if victim.background:
                return True
            # Both batch. A job that has just (re)started first earns back
            # what starting cost it.
            if now - victim.segment_start < self.min_run:
                return False
            # Preempt only if the later of the two chains ends sooner.
            cost = STOP_COST + self.restart
            rem_v = max(0.0, self.left(victim) - (now - victim.segment_start))
            down_v = self.chain[victim.id] - self.estimate[victim.id]
            own_j, down_j = self.left(j), self.chain[j.id] - self.estimate[j.id]
            without = max(rem_v + down_v, rem_v + own_j + down_j)
            with_ = max(STOP_COST + own_j + down_j, cost + own_j + rem_v + down_v)
            return without - with_ > self.margin * cost
        return False

    def left(self, j):
        """Estimated work left at the start of j's current (or next) segment."""
        return self.estimate[j.id] * j.remaining / max(j.work, 1e-9)

    def plan(self, now):
        if not self.ready:
            return
        view = {"holds": []}
        held = defaultdict(lambda: defaultdict(float))
        current = self.policy == "current"
        if current:
            for x in self.running_jobs():
                for node, devices, cpu, ram in x.alloc:
                    held[x.project]["gpu"] += len(devices)
                    held[x.project]["cpu"] += cpu
                    held[x.project]["ram"] += ram
        key = lambda x: (-x.priority, self.share(held, x.project), x.ready_at, x.id)
        remaining = sorted(self.order(now), key=key) if current else self.order(now)
        failed = {}  # demand signature -> target, reused while no hold changed the view
        i = 0
        while i < len(remaining):
            j = remaining[i]
            i += 1
            sig = tuple((p.gpu, p.cpu, p.ram, tuple(p.nodes)) for p in j.parts)
            end = now + self.left(j)
            if sig in failed and not view["holds"]:
                target = failed[sig]
            else:
                nodes = self.assign(j, view, lambda p, n: self.fits(p, n, view, end=end))
                if nodes:
                    alloc = self.allocation(j, nodes, view, end)
                    self.start(now, j, alloc)
                    if current:
                        for node, devices, cpu, ram in alloc:
                            held[j.project]["gpu"] += len(devices)
                            held[j.project]["cpu"] += cpu
                            held[j.project]["ram"] += ram
                        remaining[i:] = sorted(remaining[i:], key=key)
                    continue
                target = self.assign(j, view, lambda p, n: self.could_ever(p, n))
                failed[sig] = target
            if not target:
                continue
            victims = self.find_victims(j, target, view, now) if self.policy != "fifo" else None
            for v in victims or []:
                self.suspend(now, v)
            if victims:
                failed.pop(sig, None)
            if self.hold_mode != "none":
                self.hold(j, target, view, now if victims else self.shadow(j, target, view, now))

    def running_jobs(self):
        return [self.jobs[i] for i in self.running]

    def find_victims(self, j, target, view, now):
        pool = [v for v in self.running_jobs() if self.may_preempt(j, v, now)
                and any(n in target or set(d for _, ds, _, _ in v.alloc for d in ds) & set(self.cap[n]["gpus"])
                        for n, _, _, _ in v.alloc)]
        if not pool:
            return None
        if self.policy == "current":
            pool.sort(key=lambda v: (v.priority, v.gang, -(v.segment_start or 0), v.id))
        else:
            pool.sort(key=lambda v: (not v.background, self.chain[v.id] - self.estimate[v.id], -(v.segment_start or 0), v.id))

        def ok(chosen):
            freed = frozenset(v.id for v in chosen)
            return all(self.fits(p, n, view, freed, cpu_matters=False) for p, n in zip(j.parts, target))

        chosen = []
        for v in pool:
            if ok(chosen):
                break
            chosen.append(v)
        if not ok(chosen):
            return None
        for v in list(chosen):
            rest = [x for x in chosen if x is not v]
            if ok(rest):
                chosen = rest
        return chosen

    def start(self, now, j, alloc):
        for node, devices, cpu, ram in alloc:
            for d in devices:
                self.holder[d] = j.id
            self.cpu_used[node] += cpu
            self.ram_used[node] += ram
        j.alloc, j.state = alloc, "running"
        self.ready.discard(j.id)
        self.running.add(j.id)
        if j.start is None:
            j.start = now
        j.segment_start = now
        j.run_token += 1
        self.push(now + j.remaining, "finish", j, j.run_token)

    def suspend(self, now, v):
        v.suspending = True
        v.preemptions += 1
        self.preemptions += 1
        v.remaining = max(0.0, v.remaining - (now - v.segment_start)) + self.restart
        v.overhead += STOP_COST + self.restart
        v.run_token += 1
        self.push(now + STOP_COST, "stopped", v, v.run_token)


# ---- metrics ---------------------------------------------------------------
def components(jobs, keep_empty=False):
    parent = {j: j for j in jobs}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for j in jobs.values():
        for d in j.deps:
            parent[find(j.id)] = find(d)
    groups = defaultdict(list)
    for j in jobs.values():
        groups[find(j.id)].append(j)
    return [g for g in groups.values() if keep_empty or sum(x.work for x in g) > 0]


def report(sim, label, groups):
    jobs = [j for j in sim.jobs.values() if j.work > 0]
    flows = [j.end - j.arrival for j in jobs if j.end is not None]
    spans = []
    for g in groups:
        ends = [sim.jobs[j.id].end for j in g if j.work > 0]
        if ends and all(e is not None for e in ends) and len(g) > 1:
            spans.append(max(ends) - min(sim.jobs[j.id].arrival for j in g))
    bg = [j for j in jobs if j.background and j.end]
    fg = [j for j in jobs if not j.background and j.end]
    print(f"{label:16s} DAG makespan mean {statistics.mean(spans) / 3600:7.2f} h  median {statistics.median(spans) / 3600:6.2f} h"
          f" | flow mean {statistics.mean(flows) / 3600:6.2f} h  (batch {statistics.mean([j.end - j.arrival for j in fg]) / 3600:6.2f}"
          f", background {statistics.mean([j.end - j.arrival for j in bg]) / 3600 if bg else 0:6.2f})"
          f" | preemptions {sim.preemptions:4d}, overhead {sum(j.overhead for j in jobs) / 3600:5.1f} h"
          f" | unfinished {sum(1 for j in jobs if j.end is None)}")
    return spans


def actual(sim, groups):
    jobs = [j for j in sim.jobs.values() if j.work > 0 and j.actual_end]
    flows = [j.actual_end - j.arrival for j in jobs]
    spans = []
    for g in groups:
        ends = [j.actual_end for j in g if j.work > 0]
        if ends and all(ends) and len(g) > 1:
            spans.append(max(ends) - min(j.arrival for j in g))
    print(f"{'actual':16s} DAG makespan mean {statistics.mean(spans) / 3600:7.2f} h  median {statistics.median(spans) / 3600:6.2f} h"
          f" | flow mean {statistics.mean(flows) / 3600:6.2f} h  (real run: pauses, outages, reruns included)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("trace")
    ap.add_argument("--policy", nargs="+", default=["current", "deps", "dag", "fifo"])
    ap.add_argument("--restart", type=float, default=120.0, help="seconds a resumed job needs before it is back where it stopped")
    ap.add_argument("--margin", type=float, default=2.0)
    ap.add_argument("--min-run", type=float, default=900.0, help="seconds a (re)started batch job runs before deps may preempt it")
    ap.add_argument("--noise", type=float, default=0.0, help="sigma of log-normal error of the duration estimates")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--estimates", help="predicted run times (predict.py) instead of the true ones")
    ap.add_argument("--hold", nargs="+", default=["all"], choices=["all", "none", "backfill"],
                    help="all: a blocked job holds what it can use (Kairo today); none: holds nothing; "
                         "backfill: holds it from its estimated start, others may use it until then")
    args = ap.parse_args()
    trace = json.load(open(args.trace, encoding="utf-8"))
    print(f"restart {args.restart:.0f} s, stop {STOP_COST:.0f} s, margin {args.margin}, "
          f"estimates {'predicted: ' + args.estimates if args.estimates else 'true x noise ' + str(args.noise)}")
    estimates = json.load(open(args.estimates, encoding="utf-8")) if args.estimates else None
    first = None
    for hold in args.hold:
        for policy in args.policy:
            sim = Sim(trace, policy, args.restart, args.margin, args.noise, args.seed, args.min_run, hold, estimates).run()
            groups = components(sim.jobs)
            if first is None:
                first = sim
                actual(sim, groups)
            report(sim, f"{policy}/{hold}", groups)


if __name__ == "__main__":
    main()
