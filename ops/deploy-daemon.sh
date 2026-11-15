#!/usr/bin/env bash
# Replace the daemon with the build of SHA (ops/build.sh), then the CLI on this machine's PATH:
#   bash ops/deploy-daemon.sh SHA NAME
# A dump of the database is taken first (<db container>:/var/lib/postgresql/kairo-before-NAME-<time>.dump) and the
# old binary is kept as /srv/kairo/bin/kairo.before-NAME (once: a second deploy under one name keeps the first),
# so a rollback is that binary, with the dump when the new version migrated the schema (an older binary refuses a
# newer schema). Restarting the daemon loses no attempt (the agents run them), but a worker using the SDK's
# AttemptSession directly may fail while it is down: deploy inside :05-:25 or :35-:55 (README: Operating).
set -euo pipefail
cd "$(dirname "$0")/.."
. ops/site.sh
sha=${1:?usage: ops/deploy-daemon.sh SHA NAME}
name=${2:?usage: ops/deploy-daemon.sh SHA NAME}
case $name in *[!a-z0-9-]* | "") echo "NAME is letters, digits and -" >&2; exit 1 ;; esac
win=$(build_of "$sha" windows)
linux=$(build_of "$sha" linux)
version=$("$win" version 2>/dev/null | head -1 | awk '{print $3}')
case $version in "$sha"*) ;; *) echo "$win is $version, not $sha (bash ops/build.sh)" >&2; exit 1 ;; esac
scp -q -o BatchMode=yes "$linux" "$KAIRO_PVE_HOST:/tmp/kairo-$name"
pve "set -e
ts=\$(date +%Y%m%d-%H%M%S)
pct exec $KAIRO_DB_VMID -- su postgres -c \"pg_dump -Fc -d kairo -f /var/lib/postgresql/kairo-before-$name-\$ts.dump\"
pct exec $KAIRO_DB_VMID -- ls -la /var/lib/postgresql/kairo-before-$name-\$ts.dump
pct push $KAIRO_DAEMON_VMID /tmp/kairo-$name /srv/kairo/bin/kairo.new
rm /tmp/kairo-$name
pct exec $KAIRO_DAEMON_VMID -- bash -c 'set -e; cd /srv/kairo/bin; chmod 755 kairo.new; ./kairo.new version | head -1; test -e kairo.before-$name || cp -a kairo kairo.before-$name; mv kairo.new kairo; systemctl restart kairo-daemon; sleep 6; systemctl is-active kairo-daemon; journalctl -u kairo-daemon --since -1min --no-pager | tail -12'"
say "daemon replaced by $sha (old: /srv/kairo/bin/kairo.before-$name)"
# the CLI on PATH (a running kairo.exe cannot be overwritten, but it can be renamed)
cli=$(command -v kairo || true)
if [ -n "$cli" ]; then
  case $cli in *.exe) ;; *) [ -e "$cli.exe" ] && cli=$cli.exe ;; esac
  mv "$cli" "${cli%.exe}.old-$(date +%Y%m%d%H%M%S).exe"
  cp "$win" "$cli"
fi
kairo version
