#!/usr/bin/env bash
# One CI check of the tree this script is in (an extracted commit), as a Kairo task of project kairo-ci runs it
# (ci/kairo-ci.sh), or as a job of GitHub's CI (.github/workflows/ci.yml, the checks that need nothing of this
# installation). The check is the first argument:
#   linux       gofmt, go vet (Linux and Windows), go test   (the Linux CI node, as an unprivileged user)
#   linux-race  go test -race                               (the Linux CI node, as an unprivileged user)
#   linux-root  the executor and process tests as root      (GitHub's runner, with sudo: Kairo's attempts run as an
#               ordinary user, README: Confinement)
#   start-pg    start a test PostgreSQL 17 on 127.0.0.1:55432 when none is reachable (in a check's container):
#               portable binaries kept in KAIRO_CACHE_DIR, the data in KAIRO_SCRATCH, run as the check's user (as
#               nobody when that is root, which PostgreSQL refuses)
#   windows     go vet, go test                             (GitHub's Windows runner)
#   sdk-python  the Python SDK's tests                      (the CI node, in a uv image)
#   sdk-rust    the Rust SDK's tests                        (the CI node, in a Rust image)
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
# start_pg starts the test server a check's container has none of: PostgreSQL 17 from the portable builds of
# io.zonky.test (Maven Central; checked by sha256), unpacked once into the project's cache, its data in the scratch,
# trusting local connections (role kairo, database kairo), fsync off, run as the check's user, or as nobody when that
# is root (PostgreSQL refuses root).
pg_version=17.12.0
pg_sha256=7a8951dc117490f2d4161600836c8b1055306aff86e0a472aad55457651e5cbd
start_pg() {
  pg_reachable && return 0
  local cache=${KAIRO_CACHE_DIR:?start-pg needs KAIRO_CACHE_DIR} scratch=${KAIRO_SCRATCH:?start-pg needs KAIRO_SCRATCH}
  local pg=$cache/postgres-$pg_version
  if [ ! -x "$pg/bin/postgres" ]; then
    step "fetch PostgreSQL $pg_version"
    local jar=$cache/postgres-$pg_version.jar.partial
    curl -fsSL -o "$jar" "https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-linux-amd64/$pg_version/embedded-postgres-binaries-linux-amd64-$pg_version.jar"
    echo "$pg_sha256  $jar" | sha256sum -c - >/dev/null
    rm -rf "$pg.partial"
    mkdir -p "$pg.partial"
    python3 - "$jar" "$pg.partial" <<'PY'
import lzma, sys, tarfile, zipfile
jar = zipfile.ZipFile(sys.argv[1])
name = next(n for n in jar.namelist() if n.endswith(".txz"))
with tarfile.open(fileobj=lzma.open(jar.open(name)), mode="r|") as archive:
    archive.extractall(sys.argv[2])
PY
    mv "$pg.partial" "$pg"
    rm -f "$jar"
  fi
  step "start PostgreSQL $pg_version"
  local data=$scratch/pgdata
  mkdir -p "$data"
  if [ "$(id -u)" = 0 ]; then
    chown 65534:65534 "$data"
    as_nobody() { setpriv --reuid=65534 --regid=65534 --clear-groups "$@"; }
  else
    as_nobody() { "$@"; }
  fi
  chmod 700 "$data"
  as_nobody "$pg/bin/initdb" -D "$data" -U kairo --auth=trust -E UTF8 >/dev/null
  # the portable build has no createdb: the database is made in single-user mode, before the server starts
  echo "CREATE DATABASE kairo;" | as_nobody "$pg/bin/postgres" --single -D "$data" postgres >/dev/null
  as_nobody "$pg/bin/pg_ctl" -D "$data" -l "$data/server.log" -w start \
    -o "-p 55432 -k /tmp -c listen_addresses=127.0.0.1 -c max_connections=300 -c fsync=off -c synchronous_commit=off -c full_page_writes=off" >/dev/null
}
need_pg() {
  if ! pg_reachable; then
    echo "PostgreSQL for the store tests is not reachable (KAIRO_TEST_PG or 127.0.0.1:55432)" >&2
    exit 2
  fi
}
# (the SDK checks' containers have no Go)
step "kairo-ci $check on $(hostname) ($( (go version 2>/dev/null || echo no-go) | cut -d' ' -f3-))"
case "$check" in
start-pg)
  start_pg
  ;;
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
sdk-python)
  step "pytest (Python SDK)"
  uv run --project sdk/python pytest -q
  ;;
sdk-rust)
  step "cargo test (Rust SDK)"
  cargo test --manifest-path sdk/rust/Cargo.toml
  # the optional features too (signals: the SDK's one unsafe block)
  cargo test --manifest-path sdk/rust/Cargo.toml --all-features
  ;;
*)
  echo "unknown check $check" >&2
  exit 2
  ;;
esac
step "kairo-ci $check passed"
