#!/usr/bin/env bash
# One CI check of the tree this script is in (an extracted commit), as a Kairo task of project kairo-ci runs it
# (ci/kairo-ci.sh), or as a job of GitHub's CI (.github/workflows/ci.yml, the checks that need nothing of this
# installation). The check is the first argument:
#   linux       gofmt, go vet (Linux and Windows), go test   (pve1-work, as an unprivileged user)
#   linux-race  go test -race                               (pve1-work, as an unprivileged user)
#   linux-root  the executor and process tests as root      (pve1-work)
#   windows     go vet, go test                             (windows-local)
#   wsl         the tests that run against WSL              (windows-local, KAIRO_TEST_WSL=1)
#   sdk-python  the Python SDK's tests                      (windows-local)
#   sdk-rust    the Rust SDK's tests                        (windows-local)
# Store tests need PostgreSQL at KAIRO_TEST_PG (default 127.0.0.1:55432, kairo/kairo-test).
set -euo pipefail
check=${1:?usage: ci/check.sh CHECK}
cd "$(dirname "$0")/.."
# On the installation's nodes Go, uv and cargo are where they install themselves, not always on a task's PATH; a
# GitHub runner has set up the ones it uses, first on its PATH.
if [ -z "${GITHUB_ACTIONS:-}" ]; then
  case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) export PATH="/c/Program Files/Go/bin:$HOME/.local/bin:$HOME/.cargo/bin:$PATH" ;;
  *) export PATH="/usr/local/go/bin:$PATH" ;;
  esac
fi
step() { printf '\n== %s\n' "$*"; }
pg_reachable() {
  local hostport
  hostport=$(printf '%s' "${KAIRO_TEST_PG:-postgres://kairo:kairo-test@127.0.0.1:55432/kairo}" | sed -E 's#^[a-z]+://[^@]*@([^/?]+).*#\1#')
  (exec 3<>"/dev/tcp/${hostport%:*}/${hostport##*:}") 2>/dev/null
}
need_pg() {
  if ! pg_reachable; then
    echo "PostgreSQL for the store tests is not reachable (KAIRO_TEST_PG or 127.0.0.1:55432)" >&2
    exit 2
  fi
}
step "kairo-ci $check on $(hostname) ($(go version | cut -d' ' -f3-))"
case "$check" in
linux)
  step gofmt
  unformatted=$(gofmt -l .)
  if [ -n "$unformatted" ]; then
    echo "not gofmt-formatted:" >&2
    echo "$unformatted" >&2
    exit 1
  fi
  step "go vet (linux)"
  go vet ./...
  step "go vet (windows)"
  GOOS=windows go vet ./...
  need_pg
  step "go test"
  go test -count=1 ./...
  ;;
linux-race)
  need_pg
  step "go test -race"
  go test -race -count=1 -p 2 ./...
  ;;
linux-root)
  [ "$(id -u)" = 0 ] || { echo "linux-root runs as root" >&2; exit 2; }
  step "go test as root (executor, process identity, provider)"
  go test -count=1 ./internal/executor/ ./internal/processidentity/ ./internal/provider/
  ;;
windows)
  step "go vet"
  go vet ./...
  need_pg
  step "go test"
  go test -count=1 ./...
  ;;
wsl)
  step "go test against WSL"
  KAIRO_TEST_WSL=1 go test -count=1 -run 'WSL' ./internal/executor/ ./internal/processidentity/
  ;;
sdk-python)
  step "pytest (Python SDK)"
  uv run --project sdk/python pytest -q
  ;;
sdk-rust)
  step "cargo test (Rust SDK)"
  cargo test --manifest-path sdk/rust/Cargo.toml
  ;;
*)
  echo "unknown check $check" >&2
  exit 2
  ;;
esac
step "kairo-ci $check passed"
