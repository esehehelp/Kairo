# Sourced by ops/*.sh and ci/kairo-ci.sh: where this operator machine keeps what Kairo's operation needs, and how
# the site's machines are reached. Nothing of it is in the checkout (README: Operating an installation).
#   kairo_config  $KAIRO_CONFIG_DIR, else %APPDATA%\kairo on Windows, else ${XDG_CONFIG_HOME:-~/.config}/kairo:
#                 the CLI's token and CA (os.UserConfigDir), site.env, the agent's configuration
#   kairo_state   $KAIRO_STATE_DIR, else %LOCALAPPDATA%\kairo on Windows, else ${XDG_STATE_HOME:-~/.local/state}/kairo:
#                 builds, CI trees, logs
# site.env (sourced) says how the site's machines are reached:
#   KAIRO_PVE_HOST=root@HOST          the Proxmox host of the daemon's and the database's containers
#   KAIRO_DAEMON_VMID=N, KAIRO_DB_VMID=N
#   KAIRO_NODES="NODE=ssh:root@HOST NODE=pct:root@HOST:VMID ..."   the Linux nodes whose agents ops/ replaces
#   CI_WINDOWS_ROOT, CI_PVE1_HOST, CI_PVE1_VMID                      ci/kairo-ci.sh
upath() { if command -v cygpath >/dev/null 2>&1; then cygpath -u "$1"; else printf '%s\n' "$1"; fi; }
if [ -n "${KAIRO_CONFIG_DIR:-}" ]; then
  kairo_config=$KAIRO_CONFIG_DIR
elif [ -n "${APPDATA:-}" ]; then
  kairo_config=$(upath "$APPDATA")/kairo
else
  kairo_config=${XDG_CONFIG_HOME:-$HOME/.config}/kairo
fi
if [ -n "${KAIRO_STATE_DIR:-}" ]; then
  kairo_state=$KAIRO_STATE_DIR
elif [ -n "${LOCALAPPDATA:-}" ]; then
  kairo_state=$(upath "$LOCALAPPDATA")/kairo
else
  kairo_state=${XDG_STATE_HOME:-$HOME/.local/state}/kairo
fi
site_env=$kairo_config/site.env
[ -f "$site_env" ] || { echo "$site_env is missing (README: Operating an installation)" >&2; exit 2; }
# shellcheck source=/dev/null
. "$site_env"

# route_of NODE: how NODE is reached (ssh:USER@HOST or pct:USER@HOST:VMID), from KAIRO_NODES
route_of() {
  local entry
  for entry in ${KAIRO_NODES:-}; do
    if [ "${entry%%=*}" = "$1" ]; then
      printf '%s\n' "${entry#*=}"
      return 0
    fi
  done
  echo "node $1 is not in KAIRO_NODES ($site_env)" >&2
  return 1
}

# on NODE CMD: CMD as root in /srv/kairo on a Linux node (stdin is not read: a script piped to bash goes on)
on() {
  local route
  route=$(route_of "$1") || return 1
  case $route in
  ssh:*) ssh -n -o BatchMode=yes "${route#ssh:}" "cd /srv/kairo && $2" ;;
  pct:*)
    local rest=${route#pct:}
    ssh -n -o BatchMode=yes "${rest%:*}" "pct exec ${rest##*:} -- bash -c $(printf '%q' "cd /srv/kairo && $2")"
    ;;
  esac
}

# send NODE FILE: FILE to the node as /srv/kairo/bin/kairo.new
send() {
  local route
  route=$(route_of "$1") || return 1
  case $route in
  ssh:*) scp -q -o BatchMode=yes "$2" "${route#ssh:}:/srv/kairo/bin/kairo.new" ;;
  pct:*)
    local rest=${route#pct:}
    scp -q -o BatchMode=yes "$2" "${rest%:*}:/tmp/kairo-agent.new" &&
      ssh -n -o BatchMode=yes "${rest%:*}" "pct push ${rest##*:} /tmp/kairo-agent.new /srv/kairo/bin/kairo.new && rm /tmp/kairo-agent.new"
    ;;
  esac
}

# pve CMD: CMD as root on the Proxmox host of the daemon and the database
pve() { ssh -n -o BatchMode=yes "$KAIRO_PVE_HOST" "$1"; }

# build_of SHA OS: the build of SHA for OS (windows, linux) made by ops/build.sh
build_of() {
  case $2 in
  windows) printf '%s\n' "$kairo_state/builds/kairo-$1.exe" ;;
  linux) printf '%s\n' "$kairo_state/builds/kairo-$1-linux" ;;
  esac
}

say() { echo "$(date -Iseconds) $*" | tee -a "$kairo_state/logs/ops.log"; }
mkdir -p "$kairo_state/logs" "$kairo_state/builds"
