#!/usr/bin/env bash
# Build the checkout's HEAD for Windows and Linux, as ops/deploy-daemon.sh and the agent replacements take it:
#   bash ops/build.sh            -> <state>/builds/kairo-<sha>.exe and kairo-<sha>-linux (ops/site.sh)
# The tree must be clean: the build carries its commit (kairo version), and a modified one would say so.
set -euo pipefail
cd "$(dirname "$0")/.."
. ops/site.sh
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "the checkout has changes: commit them first (a build names its commit)" >&2
  exit 1
fi
sha=$(git rev-parse --short=7 HEAD)
win=$(build_of "$sha" windows)
linux=$(build_of "$sha" linux)
GOOS=windows GOARCH=amd64 go build -o "$win" ./cmd/kairo
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$linux" ./cmd/kairo
case $(uname -s) in
MINGW* | MSYS* | CYGWIN*) "$win" version 2>/dev/null | head -1 ;;
*) "$linux" version 2>/dev/null | head -1 ;;
esac
echo "$win"
echo "$linux"
