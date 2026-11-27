#!/usr/bin/env bash
# A test installation of Kairo beside the production one (issue #47: the WAN fabric is built on a branch and tried
# here, never on production agents): its own container on a Proxmox host with PostgreSQL and the daemon, its own CA,
# and later its own agents. Nothing of production is touched.
#   bash ops/test-instance.sh create          make the container (DHCP), PostgreSQL, the database, the TLS and config
#   bash ops/test-instance.sh deploy SHA      put the build of SHA (ops/build.sh) in it and (re)start the daemon
#   bash ops/test-instance.sh env             print the environment the CLI talks to the test daemon with
#   bash ops/test-instance.sh config          push <kairo config>/test/kairo.toml (edited by hand) and restart the daemon
#   bash ops/test-instance.sh tls SHA         re-issue the daemon's certificate for its address and its fabric address
#   bash ops/test-instance.sh agent NODE SHA  a test agent on NODE's host (a KAIRO_NODES node reached by ssh), beside
#                                             its production agent: node NODE-t, service kairo-test-agent, /srv/kairo-test,
#                                             transfer port 7485, containers kairo-t-*, CPU and RAM only (no GPU)
#   bash ops/test-instance.sh winagent SHA    a test agent on this Windows machine, run in the foreground (start it in the
#                                             background): node windows-t, not elevated, so its wslc containers (kairo-t-*)
#                                             are in this user's wslc session, apart from production's; transfer port 7486,
#                                             CPU and RAM only; on the fabric from its wslc VM, with no endpoints (the VM
#                                             takes no connection from outside: it reaches out to its peers)
# site.env (ops/site.sh) says where:
#   KAIRO_TEST_PVE_HOST=root@HOST   the Proxmox host
#   KAIRO_TEST_VMID=N               the container's id
#   KAIRO_TEST_TEMPLATE=STORAGE:vztmpl/debian-13-standard_...tar.zst
#   KAIRO_TEST_ROOTFS=STORAGE:GB    (default local-lvm:8)
#   KAIRO_TEST_FABRIC_PREFIX=a.b.c.d/n  the test fabric's network (issue #47; none: no fabric)
#   KAIRO_TEST_FABRIC_PORT=N            the test agents' WireGuard port (default 51821)
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

# fabric_address SHA NODE: NODE's fabric address as the test daemon has it ("" when none), asked with the build of SHA
fabric_address() {
  [ -n "${KAIRO_TEST_FABRIC_PREFIX:-}" ] || return 0
  (
    eval "$(bash "$0" env)"
    "$(build_of "$1" windows)" fabric list --json 2>/dev/null | python -c "import json,sys
for n in json.load(sys.stdin).get('nodes') or []:
    if n['node_id'] == '$2': print(n['address'])" || true
  )
}

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
  if [ -n "${KAIRO_TEST_FABRIC_PREFIX:-}" ]; then
    printf '\n[fabric]\nprefix = "%s"\n' "$KAIRO_TEST_FABRIC_PREFIX"
  fi
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
agent)
  node=${2:?usage: ops/test-instance.sh agent NODE SHA}
  sha=${3:?usage: ops/test-instance.sh agent NODE SHA}
  route=$(route_of "$node")
  case $route in ssh:*) host=${route#ssh:} ;; *) echo "node $node is not reached by ssh ($route)" >&2; exit 1 ;; esac
  addr=${host#*@}
  linux=$(build_of "$sha" linux)
  [ -f "$linux" ] || { echo "$linux is missing (bash ops/build.sh)" >&2; exit 1; }
  test_node=$node-t
  # its node token, from the test daemon's database
  # (a token it had before is revoked: the agent is given a new one)
  token=$(inside "cd /srv/kairo && umask 077 && { bin/kairo token revoke --config kairo.toml $test_node >/dev/null 2>&1 || true; } && bin/kairo token create --config kairo.toml --name $test_node --role node --node $test_node --out /tmp/node.token >/dev/null && cat /tmp/node.token && rm /tmp/node.token")
  # its fabric address, once the test daemon gave it one (the agent's first start with a fabric), and the daemon's:
  # the agent then talks to the daemon over the fabric, its LAN address for bootstrapping
  fabric_addr=$(fabric_address "$sha" "$test_node")
  daemon_fabric=$(fabric_address "$sha" kairo-test-daemon)
  server_url="https://$(cat "$test_dir/address"):7474"
  bootstrap_url=
  if [ -n "$daemon_fabric" ]; then
    bootstrap_url=$server_url
    server_url="https://$daemon_fabric:7474"
  fi
  # its transfer endpoint's certificate, from the test CA (issued aside: the daemon's stays), for its LAN address
  # and its fabric address
  work=$(mktemp -d)
  cp "$test_dir/tls/ca.pem" "$test_dir/tls/ca-key.pem" "$work/"
  if [ -n "$fabric_addr" ]; then
    kairo tls issue --dir "$work" --host "$addr" --host "$fabric_addr" >/dev/null
  else
    kairo tls issue --dir "$work" --host "$addr" >/dev/null
  fi
  cat >"$work/agent.toml" <<CONFIG
# Kairo's test installation (ops/test-instance.sh): a test agent beside this host's production agent.
server_url = "$server_url"
token = "$token"
ca_file = "/etc/kairo-test/ca.pem"
log_directory = "/srv/kairo-test/attempts"
observation_interval_seconds = 5
container_prefix = "kairo-t-"

[node]
id = "$test_node"
name = "$test_node"
labels = { site = "test" }

[[providers]]
id = "$test_node-host"
kind = "host"

[[providers]]
id = "$test_node-fs"
kind = "filesystem"
filesystems = ["/"]

[packs]
listen = "0.0.0.0:7485"
$(if [ -z "${KAIRO_TEST_FABRIC_PREFIX:-}" ]; then echo "addr = \"$addr:7485\""; fi)
cert_file = "/etc/kairo-test/pack-host.pem"
key_file = "/etc/kairo-test/pack-host-key.pem"
$(if [ -n "${KAIRO_TEST_FABRIC_PREFIX:-}" ]; then printf '\n[fabric]\nenabled = true\ninterface = "kairo-t0"\nlisten_port = %s\nkey_file = "/etc/kairo-test/fabric.key"\nendpoints = ["%s:%s"]\n' "${KAIRO_TEST_FABRIC_PORT:-51821}" "$addr" "${KAIRO_TEST_FABRIC_PORT:-51821}"; fi)
$(if [ -n "$bootstrap_url" ]; then printf 'bootstrap_url = "%s"\n' "$bootstrap_url"; fi)

[[executors]]
id = "$test_node-incus"
kind = "incus"
labels = { site = "test" }
CONFIG
  cat >"$work/kairo-test-agent.service" <<'UNIT'
[Unit]
Description=Kairo test agent (ops/test-instance.sh; not production)
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/srv/kairo-test
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ExecStart=/srv/kairo-test/bin/kairo agent --config /srv/kairo-test/agent.toml
Restart=on-failure
RestartSec=10
KillMode=process

[Install]
WantedBy=multi-user.target
UNIT
  ssh -o BatchMode=yes "$host" "mkdir -p /srv/kairo-test/bin /srv/kairo-test/attempts /srv/kairo-test-store /srv/kairo-test-durable /etc/kairo-test && chmod 700 /etc/kairo-test"
  scp -q -o BatchMode=yes "$test_dir/tls/ca.pem" "$host:/etc/kairo-test/ca.pem"
  scp -q -o BatchMode=yes "$work/server.pem" "$host:/etc/kairo-test/pack-host.pem"
  scp -q -o BatchMode=yes "$work/server-key.pem" "$host:/etc/kairo-test/pack-host-key.pem"
  scp -q -o BatchMode=yes "$work/agent.toml" "$host:/srv/kairo-test/agent.toml"
  scp -q -o BatchMode=yes "$work/kairo-test-agent.service" "$host:/etc/systemd/system/kairo-test-agent.service"
  scp -q -o BatchMode=yes "$linux" "$host:/srv/kairo-test/bin/kairo.new"
  rm -rf "$work"
  ssh -n -o BatchMode=yes "$host" "set -e; chmod 600 /srv/kairo-test/agent.toml /etc/kairo-test/pack-host-key.pem; cd /srv/kairo-test/bin; chmod 755 kairo.new; ./kairo.new version | head -1; mv kairo.new kairo; systemctl daemon-reload; systemctl enable kairo-test-agent >/dev/null 2>&1; systemctl restart kairo-test-agent; sleep 5; systemctl is-active kairo-test-agent"
  say "test agent $test_node on $node's host: $sha"
  ;;
winagent)
  sha=${2:?usage: ops/test-instance.sh winagent SHA}
  exe=$(build_of "$sha" windows)
  linux=$(build_of "$sha" linux)
  [ -f "$exe" ] && [ -f "$linux" ] || { echo "the builds of $sha are missing (bash ops/build.sh)" >&2; exit 1; }
  test_node=windows-t
  home=$(cygpath -u "$LOCALAPPDATA")/kairo/test-agent
  conf=$test_dir/$test_node
  mkdir -p "$home/bin" "$home/attempts" "$conf"
  # the agent (and kairo-linux beside it: the wslc containers' init, and the fabric's work in the wslc VM)
  cp "$exe" "$home/bin/kairo.exe"
  cp "$linux" "$home/bin/kairo-linux"
  token=$(inside "cd /srv/kairo && umask 077 && { bin/kairo token revoke --config kairo.toml $test_node >/dev/null 2>&1 || true; } && bin/kairo token create --config kairo.toml --name $test_node --role node --node $test_node --out /tmp/node.token >/dev/null && cat /tmp/node.token && rm /tmp/node.token")
  lan=$(powershell.exe -NoProfile -Command "(Get-NetIPAddress -AddressFamily IPv4 | Where-Object { \$_.PrefixOrigin -in 'Dhcp','Manual' -and \$_.InterfaceAlias -notmatch 'vEthernet|Tailscale|Loopback' } | Select-Object -First 1).IPAddress" | tr -d '\r')
  [ -n "$lan" ] || { echo "no LAN address found" >&2; exit 1; }
  if [ ! -f "$conf/pack.pem" ]; then
    work=$(mktemp -d)
    cp "$test_dir/tls/ca.pem" "$test_dir/tls/ca-key.pem" "$work/"
    kairo tls issue --dir "$work" --host "$lan" >/dev/null
    cp "$work/server.pem" "$conf/pack.pem"
    cp "$work/server-key.pem" "$conf/pack-key.pem"
    rm -rf "$work"
  fi
  w() { cygpath -m "$1"; }
  cat >"$conf/agent.toml" <<CONFIG
# Kairo's test installation (ops/test-instance.sh winagent): a test agent on this Windows machine, not elevated.
server_url = "https://$(cat "$test_dir/address"):7474"
token = "$token"
ca_file = "$(w "$test_dir/tls/ca.pem")"
log_directory = "$(w "$home/attempts")"
observation_interval_seconds = 5
container_prefix = "kairo-t-"

[node]
id = "$test_node"
name = "$test_node"
os = "windows"
architecture = "amd64"
labels = { site = "test" }

[[providers]]
id = "$test_node-host"
kind = "host"

[packs]
listen = ":7486"
addr = "$lan:7486"
cert_file = "$(w "$conf/pack.pem")"
key_file = "$(w "$conf/pack-key.pem")"
$(if [ -n "${KAIRO_TEST_FABRIC_PREFIX:-}" ]; then printf '\n[fabric]\nenabled = true\ninterface = "kairo-t0"\nlisten_port = %s\nkey_file = "%s"\nendpoints = []\n' "${KAIRO_TEST_FABRIC_PORT:-51821}" "$(w "$conf/fabric.key")"; fi)

[[executors]]
id = "$test_node-wslc"
kind = "wslc"
labels = { site = "test" }
CONFIG
  say "test agent $test_node: $sha (foreground; stop it to end it)"
  exec "$home/bin/kairo.exe" agent --config "$(w "$conf/agent.toml")"
  ;;
tls)
  sha=${2:?usage: ops/test-instance.sh tls SHA}
  ip=$(cat "$test_dir/address")
  daemon_fabric=$(fabric_address "$sha" kairo-test-daemon)
  if [ -n "$daemon_fabric" ]; then
    kairo tls issue --dir "$test_dir/tls" --host "$ip" --host "$daemon_fabric" >/dev/null
  else
    kairo tls issue --dir "$test_dir/tls" --host "$ip" >/dev/null
  fi
  push "$test_dir/tls/server.pem" /srv/kairo/tls/server.pem
  push "$test_dir/tls/server-key.pem" /srv/kairo/tls/server-key.pem
  inside "chmod 600 /srv/kairo/tls/server-key.pem && systemctl restart kairo-daemon; sleep 4; systemctl is-active kairo-daemon"
  say "test daemon's certificate for $ip ${daemon_fabric:-}"
  ;;
config)
  push "$test_dir/kairo.toml" /srv/kairo/kairo.toml
  inside "systemctl restart kairo-daemon; sleep 4; systemctl is-active kairo-daemon"
  say "test daemon's config pushed"
  ;;
env)
  printf 'export KAIRO_API=https://%s:7474\nexport KAIRO_TOKEN_FILE=%s\nexport KAIRO_CA_FILE=%s\n' "$(cat "$test_dir/address")" "$test_dir/token" "$test_dir/tls/ca.pem"
  ;;
*)
  echo "usage: ops/test-instance.sh create | deploy SHA | agent NODE SHA | config | tls SHA | env" >&2
  exit 2
  ;;
esac
