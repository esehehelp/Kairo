#!/usr/bin/env bash
# A test installation of Kairo beside the production one (issue #47: the WAN fabric is built on a branch and tried
# here, never on production agents): its own container on a Proxmox host with PostgreSQL and the daemon, its own CA,
# and later its own agents. Nothing of production is touched.
#   bash ops/test-instance.sh create          make the container (DHCP), PostgreSQL, the database, the TLS and config
#   bash ops/test-instance.sh deploy SHA      put the build of SHA (ops/build.sh) in it and (re)start the daemon
#   bash ops/test-instance.sh env             print the environment the CLI talks to the test daemon with
# site.env (ops/site.sh) says where:
#   KAIRO_TEST_PVE_HOST=root@HOST   the Proxmox host
#   KAIRO_TEST_VMID=N               the container's id
#   KAIRO_TEST_TEMPLATE=STORAGE:vztmpl/debian-13-standard_...tar.zst
#   KAIRO_TEST_ROOTFS=STORAGE:GB    (default local-lvm:8)
#   KAIRO_TEST_BRIDGE=BRIDGE        (default vmbr0)
# What the CLI needs (CA, operator token) is kept in <kairo config>/test/; the CA's key stays there and never goes to
# the container.
set -euo pipefail
cd "$(dirname "$0")/.."
. ops/site.sh
: "${KAIRO_TEST_PVE_HOST:?KAIRO_TEST_PVE_HOST in site.env}" "${KAIRO_TEST_VMID:?KAIRO_TEST_VMID in site.env}"
test_dir=$kairo_config/test
mkdir -p "$test_dir"
tpve() { ssh -n -o BatchMode=yes "$KAIRO_TEST_PVE_HOST" "$1"; }
# inside CMD: CMD as root in the test container
inside() { tpve "pct exec $KAIRO_TEST_VMID -- bash -c $(printf '%q' "$1")"; }
# push LOCAL PATH: a file into the test container
push() {
  scp -q -o BatchMode=yes "$1" "$KAIRO_TEST_PVE_HOST:/tmp/kairo-test-push"
  tpve "pct push $KAIRO_TEST_VMID /tmp/kairo-test-push $2 && rm /tmp/kairo-test-push"
}
address() { inside "hostname -I | awk '{print \$1}'"; }

provision_script() {
  cat <<'SCRIPT'
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq postgresql ca-certificates >/dev/null
mkdir -p /srv/kairo/bin /srv/kairo/tls /srv/kairo/attempts
umask 077
head -c 24 /dev/urandom | base64 | tr -d '/+=' >/srv/kairo/db-password
password=$(cat /srv/kairo/db-password)
printf "CREATE ROLE kairo LOGIN PASSWORD '%s';\n" "$password" | su postgres -c "psql -q -v ON_ERROR_STOP=1"
su postgres -c 'createdb -O kairo -E UTF8 --locale=C -T template0 kairo'
SCRIPT
}

daemon_config() {
  cat <<CONFIG
# Kairo's test installation (ops/test-instance.sh). Not production.
database_url = "postgres://kairo@127.0.0.1:5432/kairo?sslmode=disable"
database_password_file = "/srv/kairo/db-password"
listen = "0.0.0.0:7474"
advertise_url = "https://$1:7474"
tls_cert_file = "/srv/kairo/tls/server.pem"
tls_key_file = "/srv/kairo/tls/server-key.pem"
tls_ca_file = "/srv/kairo/tls/ca.pem"
log_directory = "/srv/kairo/attempts"
observe_only = false
observation_interval_seconds = 5

[node]
id = "kairo-test-daemon"
name = "kairo-test-daemon"
labels = { site = "test" }

[data]
confine_from = "$(date -Iseconds)"
CONFIG
}

daemon_unit() {
  cat <<'UNIT'
[Unit]
Description=Kairo daemon (test installation)
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
WorkingDirectory=/srv/kairo
ExecStart=/srv/kairo/bin/kairo serve --config /srv/kairo/kairo.toml
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
UNIT
}

case ${1:-} in
create)
  : "${KAIRO_TEST_TEMPLATE:?KAIRO_TEST_TEMPLATE in site.env}"
  if tpve "pct status $KAIRO_TEST_VMID" >/dev/null 2>&1; then
    echo "container $KAIRO_TEST_VMID exists" >&2
    exit 1
  fi
  tpve "pct create $KAIRO_TEST_VMID $KAIRO_TEST_TEMPLATE --hostname kairo-test --cores 2 --memory 1536 --swap 512 \
    --rootfs ${KAIRO_TEST_ROOTFS:-local-lvm:8} --net0 name=eth0,bridge=${KAIRO_TEST_BRIDGE:-vmbr0},firewall=1,ip=dhcp \
    --unprivileged 1 --features nesting=1 --onboot 1 --ostype debian && pct start $KAIRO_TEST_VMID"
  ip=
  for _ in $(seq 60); do
    ip=$(address 2>/dev/null || true)
    [ -n "$ip" ] && break
    sleep 2
  done
  [ -n "$ip" ] || { echo "the container got no address" >&2; exit 1; }
  say "test container $KAIRO_TEST_VMID up at $ip"
  provision_script >"$test_dir/provision.sh"
  push "$test_dir/provision.sh" /root/provision.sh
  inside "bash /root/provision.sh && rm /root/provision.sh"
  # the test CA and the daemon's certificate, for the address it got
  kairo tls init --dir "$test_dir/tls" --host "$ip"
  push "$test_dir/tls/ca.pem" /srv/kairo/tls/ca.pem
  push "$test_dir/tls/server.pem" /srv/kairo/tls/server.pem
  push "$test_dir/tls/server-key.pem" /srv/kairo/tls/server-key.pem
  daemon_config "$ip" >"$test_dir/kairo.toml"
  push "$test_dir/kairo.toml" /srv/kairo/kairo.toml
  daemon_unit >"$test_dir/kairo-daemon.service"
  push "$test_dir/kairo-daemon.service" /etc/systemd/system/kairo-daemon.service
  inside "chmod 600 /srv/kairo/tls/server-key.pem /srv/kairo/db-password && systemctl daemon-reload && systemctl enable kairo-daemon"
  echo "$ip" >"$test_dir/address"
  say "test installation created at $ip (bash ops/test-instance.sh deploy SHA)"
  ;;
deploy)
  sha=${2:?usage: ops/test-instance.sh deploy SHA}
  linux=$(build_of "$sha" linux)
  [ -f "$linux" ] || { echo "$linux is missing (bash ops/build.sh)" >&2; exit 1; }
  push "$linux" /srv/kairo/bin/kairo.new
  inside "set -e; cd /srv/kairo/bin; chmod 755 kairo.new; ./kairo.new version | head -1; mv kairo.new kairo; systemctl restart kairo-daemon; sleep 5; systemctl is-active kairo-daemon"
  if [ ! -s "$test_dir/token" ]; then
    inside "cd /srv/kairo && umask 077 && bin/kairo token create --config kairo.toml --name operator --role admin --out /tmp/operator.token >/dev/null && cat /tmp/operator.token && rm /tmp/operator.token" >"$test_dir/token"
    chmod 600 "$test_dir/token"
  fi
  say "test daemon replaced by $sha"
  ;;
env)
  printf 'export KAIRO_API=https://%s:7474\nexport KAIRO_TOKEN_FILE=%s\nexport KAIRO_CA_FILE=%s\n' "$(cat "$test_dir/address")" "$test_dir/token" "$test_dir/tls/ca.pem"
  ;;
*)
  echo "usage: ops/test-instance.sh create | deploy SHA | env" >&2
  exit 2
  ;;
esac
