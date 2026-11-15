#!/usr/bin/env bash
# Replace the agent of Linux nodes with the build of SHA (ops/build.sh), keeping each node's configuration:
#   bash ops/replace-linux-agents.sh SHA NODE...        (NODE as KAIRO_NODES in site.env names it)
# A node holding a lease is refused unless ADOPT=1: an agent from main 2026-11-08 on runs its attempts under shims,
# which the new agent adopts (README: Agent restarts; the unit's KillMode=process keeps them, and they run the old
# file, which mv replaces by name only). The old binary is kept as bin/kairo.before-SHA. After the restart the node
# must report within 2 minutes.
set -euo pipefail
cd "$(dirname "$0")/.."
. ops/site.sh
sha=${1:?usage: ops/replace-linux-agents.sh SHA NODE...}
shift
binary=$(build_of "$sha" linux)
want=$(sha256sum "$binary" | cut -d' ' -f1)

holders() { # the executions holding a resource of node $1, one per line
  kairo resource status --json | python -c '
import json, sys
n = sys.argv[1]
ids = set()
for r in json.load(sys.stdin)["resources"]:
    if r.get("node_id") == n:
        for h in r.get("holders") or []:
            if h:
                ids.add(h.get("execution_id") or str(h))
print("\n".join(sorted(ids)))' "$1"
}
fresh() { # whether node $1 reported within $2 seconds
  kairo resource status --json | python -c '
import json, sys
n, limit = sys.argv[1], float(sys.argv[2])
print(any(r.get("node_id") == n and (r.get("observation_age_seconds") or 1e9) < limit for r in json.load(sys.stdin)["resources"]))' "$1" "$2"
}

for n in "$@"; do
  send "$n" "$binary"
  on "$n" "test \"\$(sha256sum bin/kairo.new | cut -d' ' -f1)\" = $want && chmod 755 bin/kairo.new && bin/kairo.new version | head -1"
  held=$(holders "$n")
  if [ -n "$held" ] && [ "${ADOPT:-}" != 1 ]; then say "refused: $n holds leases ($(echo $held))"; exit 1; fi
  if [ -n "$held" ]; then say "$n holds leases ($(echo $held)); adopted by the new agent"; fi
  start=$(date +%s)
  on "$n" "set -e; systemctl stop kairo-agent; test -e bin/kairo.before-$sha || cp -a bin/kairo bin/kairo.before-$sha; mv bin/kairo.new bin/kairo; systemctl start kairo-agent; systemctl is-active kairo-agent; bin/kairo version | head -1"
  say "agent $n replaced by $sha (old: bin/kairo.before-$sha)"
  ok=False
  for _ in $(seq 1 24); do
    sleep 5
    ok=$(fresh "$n" $(($(date +%s) - start)))
    [ "$ok" = True ] && break
  done
  if [ "$ok" != True ]; then say "$n has not reported within 2 min (journalctl -u kairo-agent)"; exit 1; fi
  say "$n reporting"
done
