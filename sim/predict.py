"""How well can a job's run time be predicted before it starts?

Replays the trace in arrival order. Each job is predicted only from jobs that
had already finished when it was declared: the median of the last runs that
share its most specific key, falling back to coarser keys:

    L4 project, queue, program, subcommand/script, config with digits masked
    L3 project, queue, program, subcommand/script
    L2 project, queue, task name with digits and hashes masked
    L1 project, queue
    L0 project

Prints the error per level and writes the predictions for sim.py --estimates.

    python sim/predict.py local/sim/trace.json --out local/sim/estimates.json
"""

import argparse
import heapq
import json
import math
import os
import re
import statistics
from collections import defaultdict, deque

RUNNERS = {"uv", "uv.exe", "run", "--no-sync", "python", "python.exe", "python3", "-m", "pwsh.exe", "pwsh",
           "-noprofile", "-command", "-c", "cmd", "/c", "bash", "sh", "wsl.exe", "--exec", "env"}
HEX = re.compile(r"\b[0-9a-f]{7,40}\b")
DIGITS = re.compile(r"\d+")


def base(token):
    token = token.replace("\\", "/").rstrip("/").split("/")[-1].lower()
    token = re.sub(r"\.(exe|py|toml|json|ps1|sh)$", "", token)
    token = HEX.sub("#", token)
    return re.sub(r"-#$", "", token)


def mask(text):
    return DIGITS.sub("#", HEX.sub("#", text.lower()))


def keys(j):
    argv = [a for a in j["argv"] if isinstance(a, str)]
    # A PowerShell -Command string: take the words of its last statement.
    if any(a.lower() == "-command" for a in argv):
        words = re.split(r"[\s;&|]+", argv[-1])
        argv = [w.strip("'\"") for w in words if w and not w.startswith("$")]
    words = [a for a in argv if a.lower() not in RUNNERS and not a.lower().endswith(("uv", "uv.exe", "pwsh.exe"))]
    program = base(words[0]) if words else ""
    sub = ""
    config = ""
    for i, w in enumerate(words[1:], 1):
        if w.startswith("-"):
            if w in ("--config", "-c") and i + 1 < len(words):
                config = mask(base(words[i + 1]))
            continue
        if not sub:
            sub = base(w)
        if not config and w.endswith((".toml", ".json", ".yaml")):
            config = mask(base(w))
    p, q = j["project"], j.get("queue", "")
    return [
        ("L4", p, q, program, sub, config),
        ("L3", p, q, program, sub),
        ("L2", p, q, mask(j["name"])),
        ("L1", p, q),
        ("L0", p),
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("trace")
    ap.add_argument("--out", default="local/sim/estimates.json")
    ap.add_argument("--last", type=int, default=5, help="median of this many latest runs per key")
    ap.add_argument("--history", choices=["all", "succeeded"], default="all",
                    help="which finished jobs predict later ones")
    args = ap.parse_args()
    trace = json.load(open(args.trace, encoding="utf-8"))
    jobs = [j for j in trace["jobs"] if j["work"] > 0 and j["actual_end"]]
    jobs.sort(key=lambda j: j["arrival"])
    history = defaultdict(lambda: deque(maxlen=args.last))
    finished = [(j["actual_end"], i) for i, j in enumerate(jobs)]
    heapq.heapify(finished)
    seen_all = []
    out, rows = {}, []
    for j in jobs:
        while finished and finished[0][0] <= j["arrival"]:
            _, i = heapq.heappop(finished)
            done = jobs[i]
            if args.history == "succeeded" and done["state"] != "succeeded":
                continue
            for k in keys(done):
                history[k].append(done["work"])
            seen_all.append(done["work"])
        level, pred = None, None
        for k in keys(j):
            if history[k]:
                level, pred = k[0], statistics.median(history[k])
                break
        if pred is None:
            level, pred = "none", (statistics.median(seen_all) if seen_all else 3600.0)
        pred = max(pred, 1.0)
        out[j["id"]] = pred
        rows.append((level, math.log2(pred / max(j["work"], 1.0)), j["work"]))

    def line(label, rs):
        if not rs:
            return
        errs = [abs(e) for _, e, _ in rs]
        w = sum(x for _, _, x in rs)
        within2 = sum(x for _, e, x in rs if abs(e) <= 1) / w
        under = sum(x for _, e, x in rs if e < -1) / w
        print(f"{label:6s} n={len(rs):5d}  median error x{2 ** statistics.median(errs):5.2f}"
              f"  within 2x {sum(1 for e in errs if e <= 1) / len(rs):5.1%} (by work {within2:5.1%})"
              f"  under by >2x {sum(1 for _, e, _ in rs if e < -1) / len(rs):5.1%} (by work {under:5.1%})")

    for level in ("L4", "L3", "L2", "L1", "L0", "none"):
        line(level, [r for r in rows if r[0] == level])
    line("all", rows)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    json.dump({"default": statistics.median(seen_all) if seen_all else 3600.0, "estimates": out},
              open(args.out, "w", encoding="utf-8"))
    print(f"-> {args.out}")


if __name__ == "__main__":
    main()
