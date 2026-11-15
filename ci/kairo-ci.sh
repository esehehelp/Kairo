#!/usr/bin/env bash
# Run Kairo's CI for one commit as Kairo tasks (project kairo-ci, ci/tasks.toml.in) on this installation's own nodes:
#   bash ci/kairo-ci.sh [REV] [--wait]
# The commit's tree is copied as a tar to each node the checks run on (where, from site.env: ops/site.sh), the tasks are
# declared with kairo project apply, and with --wait the script waits for them and prints their states (exit 0 only
# when every check succeeded). A commit already declared is not declared again (the tasks are immutable); kairo task
# rerun kairo-ci ci <sha>-<check> runs one again.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
rev=HEAD
wait=
for arg in "$@"; do
  case "$arg" in
  --wait) wait=1 ;;
  -*) echo "unknown flag $arg" >&2; exit 2 ;;
  *) rev=$arg ;;
  esac
done
# The site's settings (site.env) and the script's files are this operator machine's, outside the checkout (ops/site.sh).
# CI_WINDOWS_ROOT: where windows-local's checks unpack (a POSIX path, /c/...); CI_PVE1_HOST: the Proxmox host of
# pve1-work (root@...); CI_PVE1_VMID: pve1-work's container.
. ops/site.sh
sha=$(git rev-parse --short=12 "$rev^{commit}")
work=$kairo_state/ci
mkdir -p "$work/src"
tar=$work/src/$sha.tar
# The tree as committed: no CRLF conversion (core.autocrlf), which would break the scripts and gofmt on Linux.
git -c core.autocrlf=false -c core.eol=lf archive --format=tar -o "$tar" "$sha"
echo "kairo-ci $sha: $(git log -1 --format=%s "$sha")"

# windows-local reads it here
mkdir -p "$CI_WINDOWS_ROOT/src" "$CI_WINDOWS_ROOT/run" "$CI_WINDOWS_ROOT/cache"
cp "$tar" "$CI_WINDOWS_ROOT/src/$sha.tar"
# pve1-work reads it at /srv/kairo-ci/src
scp -q -o BatchMode=yes "$tar" "$CI_PVE1_HOST:/tmp/kairo-ci-$sha.tar"
ssh -o BatchMode=yes "$CI_PVE1_HOST" "pct push $CI_PVE1_VMID /tmp/kairo-ci-$sha.tar /srv/kairo-ci/src/$sha.tar --perms 644 && rm /tmp/kairo-ci-$sha.tar"

manifest=$work/kairo-ci-$sha.toml
sed -e "s/@SHA@/$sha/g" -e "s|@CI_WINDOWS_ROOT@|$CI_WINDOWS_ROOT|g" -e "s|@CI_WINDOWS_DIR@|$(cygpath -m "$CI_WINDOWS_ROOT")|g" ci/tasks.toml.in > "$manifest"
kairo project apply "$manifest"
if [ -z "$wait" ]; then
  echo "declared; follow with: kairo task list kairo-ci | grep $sha"
  exit 0
fi
status=0
kairo task wait kairo-ci --match "$sha-" --timeout 3h || status=$?
kairo task list kairo-ci 2>/dev/null | grep -E "TASK|$sha-" || true
exit $status
