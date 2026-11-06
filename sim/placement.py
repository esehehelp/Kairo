"""Where to run a job when its data is somewhere else: a small model of the
LoRA-sweep case, for comparing placement policies across link bandwidths and
prediction errors (not a replay of Kairo's trace).

Hosts sit at two sites joined by one WAN link whose bandwidth follows a day
profile (busy hours slower) with hourly noise; hosts of a site share a LAN.
Data objects live on hosts; a transfer copies one to another host, and the
transfers on a link share it equally. A job needs all its inputs on the host
it runs on, which it holds (GPU idle) while what is missing is copied there.

Scenario (the case of the thesis slides):
    A  home, fast GPU (speed 1.5), holds the base model
    C  home, small GPU (speed 0.6), holds nothing
    B  org, GPU (speed 1.0), holds the base model and the dataset
    8 LoRA trainings (base + dataset, ~2 h at speed 1, actual run time
    log-normal around it), each followed by an evaluation (base + its LoRA
    weights, 10 min); the weights are written where the training ran.

Policies:
    fixed      the usual assignment: trainings on B, evaluations on A
    locality   run only where all its inputs already are; wait otherwise
    oblivious  the first free host, fastest first, data copied as needed
    eft        earliest finish time: each ready job, in order, planned on the
               host where it is predicted to end first (wait + copy + compute),
               a host's later work counted after what it was planned before;
               copies start once the job has its host
    prefetch   eft, with the copies started as soon as the job is planned, so
               they run while it waits for its host
    informed   prefetch knowing the true run times and bandwidth (a
               reference, not the optimum)

Predictions: a job's run time is predicted as the truth times exp(N(0, sigma))
(--sigma; 0.8 is about Kairo's history medians, median error x2), bandwidth as
the profile's mean at that hour (not its noise).

    python sim/placement.py --wan-mbps 50 100 300 1000 --sigma 0 0.8 --seeds 20
"""

import argparse
import math
import random
import statistics

GB = 1e9
MB = 1e6
DT = 30.0  # s, simulation step
LAN_BPS = 110 * MB


class Host:
    def __init__(self, name, site, speed):
        self.name, self.site, self.speed = name, site, speed
        self.job = None  # the job holding it


class Job:
    def __init__(self, jid, kind, inputs, output, work, predicted, after=None):
        self.id, self.kind, self.inputs, self.output = jid, kind, inputs, output
        self.work, self.predicted = work, predicted  # s at speed 1: true, as predicted
        self.after = after  # a job that must end first
        self.host = None
        self.started = None  # compute started (inputs all there)
        self.done = 0.0  # work done
        self.ended = None


class World:
    def __init__(self, seed, wan_mbps, sigma, noise_b, day_factor, jobs=8):
        self.rng = random.Random(seed)
        self.t = 0.0
        self.hosts = {h.name: h for h in (Host("A", "home", 1.5), Host("C", "home", 0.6), Host("B", "org", 1.0))}
        self.size = {"base": 30 * GB, "data": 20 * GB}
        self.where = {"base": {"A", "B"}, "data": {"B"}}
        self.transfers = []  # [object, dst, bytes left, speculative]
        self.wan_mean = wan_mbps * 1e6 / 8
        self.day_factor = day_factor
        self.hour_noise = [math.exp(self.rng.gauss(0, noise_b)) for _ in range(24 * 14)]
        self.jobs = []
        for i in range(jobs):
            true = 7200 * math.exp(self.rng.gauss(0.3, 0.5))  # declared 2 h, runs longer
            pred = true * math.exp(self.rng.gauss(0, sigma)) if sigma > 0 else true
            train = Job(f"train{i}", "train", ["base", "data"], (f"lora{i}", 100 * MB), true, pred)
            self.jobs.append(train)
            self.jobs.append(Job(f"eval{i}", "eval", ["base", f"lora{i}"], None, 600, 600, after=train))
        self.wan_bytes = 0.0

    # --- links -------------------------------------------------------------
    def wan_bps(self, t, noisy=True):
        hour = int(t // 3600)
        busy = 9 <= (hour % 24) < 18
        mean = self.wan_mean * (self.day_factor if busy else 1.0)
        return mean * (self.hour_noise[hour % len(self.hour_noise)] if noisy else 1.0)

    def link(self, obj, dst):
        """The link a copy of obj to dst takes: 'lan:<site>' or 'wan'."""
        site = self.hosts[dst].site
        if any(self.hosts[h].site == site for h in self.where[obj]):
            return "lan:" + site
        return "wan"

    def copy_seconds(self, obj, dst, informed):
        """Predicted time to copy obj to dst now (0 if there)."""
        if dst in self.where[obj]:
            return 0.0
        for tr in self.transfers:
            if tr[0] == obj and tr[1] == dst:
                return tr[2] / self.rate_of(self.link(obj, dst), informed)
        return self.size[obj] / self.rate_of(self.link(obj, dst), informed)

    def rate_of(self, link, informed):
        cap = LAN_BPS if link.startswith("lan") else self.wan_bps(self.t, noisy=informed)
        n = 1 + sum(1 for tr in self.transfers if self.link(tr[0], tr[1]) == link)
        return cap / n

    def want(self, obj, dst, speculative=False):
        if dst in self.where[obj] or any(tr[0] == obj and tr[1] == dst for tr in self.transfers):
            return
        self.transfers.append([obj, dst, self.size[obj], speculative])

    # --- jobs --------------------------------------------------------------
    def ready(self):
        return [j for j in self.jobs if j.host is None and j.ended is None and (j.after is None or j.after.ended is not None)]

    def gpu_hosts(self):
        return list(self.hosts.values())

    def free_at(self, h, informed):
        """Predicted time the job on h ends (now if free)."""
        j = h.job
        if j is None:
            return self.t
        total = j.work if informed else j.predicted
        left = max(total - j.done, j.done * 0.5 if j.done > total else 0.0)  # past it: as long again, halved
        stage = max((self.copy_seconds(o, h.name, informed) for o in j.inputs), default=0.0) if j.started is None else 0.0
        return self.t + stage + left / h.speed

    def finish_on(self, j, h, informed):
        start = self.free_at(h, informed)
        if h.job is None:
            start += max((self.copy_seconds(o, h.name, informed) for o in j.inputs), default=0.0)
        work = j.work if informed else j.predicted
        return start + work / h.speed

    def assign(self, j, h):
        j.host, h.job = h, j
        for o in j.inputs:
            self.want(o, h.name)

    # --- one step ------------------------------------------------------------
    def step(self):
        # transfers share their link
        by_link = {}
        for tr in self.transfers:
            by_link.setdefault(self.link(tr[0], tr[1]), []).append(tr)
        for link, trs in by_link.items():
            cap = LAN_BPS if link.startswith("lan") else self.wan_bps(self.t)
            share = cap * DT / len(trs)
            for tr in trs:
                moved = min(share, tr[2])
                tr[2] -= moved
                if link == "wan":
                    self.wan_bytes += moved
        for tr in [tr for tr in self.transfers if tr[2] <= 0]:
            self.transfers.remove(tr)
            self.where[tr[0]].add(tr[1])
        # jobs compute once their inputs are there
        for h in self.hosts.values():
            j = h.job
            if j is None:
                continue
            if j.started is None and all(h.name in self.where[o] for o in j.inputs):
                j.started = self.t
            if j.started is not None:
                j.done += h.speed * DT
                if j.done >= j.work:
                    j.ended = self.t + DT
                    h.job = None
                    if j.output:
                        name, size = j.output
                        self.size[name] = size
                        self.where[name] = {h.name}
        self.t += DT


def decide(w, policy):
    """One planning pass. The plan policies (eft, prefetch, informed) plan
    every ready job, in order, onto the host where it is predicted to end
    first given what that host already runs or was planned (earliest finish
    time); a job planned on a free host starts there now."""
    informed = policy == "informed"
    if policy in ("fixed", "locality", "oblivious"):
        for j in w.ready():
            free = [h for h in w.gpu_hosts() if h.job is None]
            if not free:
                return
            if policy == "fixed":
                h = w.hosts["B" if j.kind == "train" else "A"]
                if h.job is None:
                    w.assign(j, h)
            elif policy == "locality":
                holders = [h for h in free if all(h.name in w.where[o] for o in j.inputs)]
                if holders:
                    w.assign(j, max(holders, key=lambda h: h.speed))
            else:
                w.assign(j, max(free, key=lambda h: h.speed))
        return
    overlap = policy in ("prefetch", "informed")  # copies run while the job waits for its host
    avail = {h.name: w.free_at(h, informed) for h in w.gpu_hosts()}
    for j in w.ready():
        work = j.work if informed else j.predicted
        best, best_end = None, None
        for h in w.gpu_hosts():
            stage = max((w.copy_seconds(o, h.name, informed) for o in j.inputs), default=0.0)
            begin = max(avail[h.name], w.t + stage) if overlap else avail[h.name] + stage
            end = begin + work / h.speed
            if best_end is None or end < best_end:
                best, best_end = h, end
        avail[best.name] = best_end
        if best.job is None and avail_free(w, best):
            w.assign(j, best)
        elif overlap:
            for o in j.inputs:
                w.want(o, best.name, speculative=True)


def avail_free(w, h):
    return h.job is None


def run(seed, policy, wan_mbps, sigma, noise_b, day_factor, start_hour, jobs=8):
    w = World(seed, wan_mbps, sigma, noise_b, day_factor, jobs)
    w.t = start_hour * 3600.0
    t0 = w.t
    while any(j.ended is None for j in w.jobs):
        decide(w, policy)
        w.step()
        if w.t - t0 > 14 * 86400:
            raise RuntimeError(f"{policy} did not finish")
    evals = [j.ended - t0 for j in w.jobs if j.kind == "eval"]
    return max(evals) / 3600, statistics.mean(evals) / 3600, w.wan_bytes / GB


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wan-mbps", type=float, nargs="+", default=[50, 100, 300, 1000])
    ap.add_argument("--sigma", type=float, nargs="+", default=[0.0, 0.8])
    ap.add_argument("--seeds", type=int, default=20)
    ap.add_argument("--noise", type=float, default=0.5, help="hourly bandwidth noise (log-normal sigma)")
    ap.add_argument("--day-factor", type=float, default=0.3, help="bandwidth in busy hours (9-18) vs night")
    ap.add_argument("--start-hour", type=float, default=10.0, help="hour of day the sweep is submitted")
    ap.add_argument("--jobs", type=int, default=8, help="LoRA trainings in the sweep")
    args = ap.parse_args()
    policies = ["fixed", "locality", "oblivious", "eft", "prefetch", "informed"]
    print(f"{args.jobs} LoRA trainings + evaluations; WAN noise {args.noise}, busy-hour factor {args.day_factor}, "
          f"submitted at {args.start_hour:g}:00; makespan h (mean eval completion h, WAN GB), median of {args.seeds} seeds")
    for sigma in args.sigma:
        print(f"\nrun-time prediction sigma {sigma:g}")
        print(f"{'WAN Mbps':>9} " + " ".join(f"{p:>20}" for p in policies))
        for mbps in args.wan_mbps:
            cells = []
            for p in policies:
                rs = [run(s, p, mbps, sigma, args.noise, args.day_factor, args.start_hour, args.jobs) for s in range(args.seeds)]
                cells.append(f"{statistics.median(r[0] for r in rs):5.1f} ({statistics.median(r[1] for r in rs):4.1f},"
                             f"{statistics.median(r[2] for r in rs):5.0f})")
            print(f"{mbps:>9g} " + " ".join(f"{c:>20}" for c in cells))


if __name__ == "__main__":
    main()
