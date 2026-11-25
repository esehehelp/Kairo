#!/usr/bin/env bash
# Run Kairo's CI for one commit as Kairo tasks (project kairo-ci, ci/tasks.toml.in) on this installation's own nodes:
#   bash ci/kairo-ci.sh [REV] [--wait]
# The commit's tree goes to the checks as a code snapshot (made from the commit, not the working copy); the tasks are
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
# CI_LINUX_NODE and CI_LINUX_SITE: the CI node (one with a container executor: the checks run in images) and its site
# label.
. ops/site.sh
sha=$(git rev-parse --short=12 "$rev^{commit}")
work=$kairo_state/ci
mkdir -p "$work/src"
tar=$work/src/$sha.tar
# The tree as committed: no CRLF conversion (core.autocrlf), which would break the scripts and gofmt on Linux.
git -c core.autocrlf=false -c core.eol=lf archive --format=tar -o "$tar" "$sha"
echo "kairo-ci $sha: $(git log -1 --format=%s "$sha")"

# the Linux checks' code snapshot: the commit's tree, unpacked here for kairo project apply to pack
tree=$work/tree/$sha
rm -rf "$tree"
mkdir -p "$tree"
tar -xf "$tar" -C "$tree"
paths=$(git ls-tree --name-only "$sha" | sed 's/.*/"&"/' | paste -sd, -)

manifest=$work/kairo-ci-$sha.toml
sed -e "s/@SHA@/$sha/g" -e "s|@CI_LINUX_NODE@|$CI_LINUX_NODE|g" -e "s|@CI_LINUX_SITE@|$CI_LINUX_SITE|g" -e "s|@TREE@|tree/$sha|g" -e "s|@PATHS@|$paths|g" ci/tasks.toml.in > "$manifest"
kairo project apply "$manifest"
rm -rf "$tree"
if [ -z "$wait" ]; then
  echo "declared; follow with: kairo task list kairo-ci | grep $sha"
  exit 0
fi
status=0
kairo task wait kairo-ci --match "$sha-" --timeout 3h || status=$?
kairo task list kairo-ci 2>/dev/null | grep -E "TASK|$sha-" || true
exit $status
