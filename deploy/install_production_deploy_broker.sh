#!/usr/bin/env bash
# Explicit, out-of-band maintenance installer for the root deployment broker.
# This file is never invoked by CI or by the deploy user.  An authorized root
# operator first copies the reviewed broker to a root-owned staging path, then
# supplies its independently recorded SHA-256 to this installer.

set -Eeuo pipefail
umask 077

readonly TARGET=/usr/local/sbin/probiga-production-deploy
readonly EXPECTED_CAPABILITIES=$'probiga.production-deploy.capabilities.v1\ndeploy_protocol=probiga-production-deploy-v4\nrecovery_protocol=probiga-database-guard-recovery-v2\nartifact_protocol=probiga-trusted-artifacts-v2\nsnapshot_only_recovery=true\ninput_and_freeze_digests=true\ngovernance_task_snapshot=true\nreceipt_pending_recovery=true\nactivation_release_identity=true\nrelease_tree_and_adapter_seal=true'

fail() {
  echo "production broker installer: $*" >&2
  exit 2
}

test "${EUID:-$(id -u)}" -eq 0 || fail "must run as root"
test "$#" -eq 2 || fail "usage: $0 ROOT_OWNED_SOURCE EXPECTED_SHA256"
SOURCE="$1"
EXPECTED_SHA256="$2"
[[ "$SOURCE" = /* ]] || fail "source path must be absolute"
[[ "$EXPECTED_SHA256" =~ ^[0-9a-f]{64}$ ]] || fail "invalid expected SHA-256"
case "$SOURCE" in
  /root/*|/var/lib/probiga/broker-maintenance/*) ;;
  *) fail "source must be staged under a root-only maintenance directory" ;;
esac
test -f "$SOURCE" || fail "source is not a regular file"
test ! -L "$SOURCE" || fail "source must not be a symlink"
test "$(stat -c '%U:%G' "$SOURCE")" = root:root || fail "source owner differs"
test $((8#$(stat -c '%a' "$SOURCE") & 8#022)) -eq 0 || \
  fail "source is group/other writable"
test "$(sha256sum "$SOURCE" | cut -d' ' -f1)" = "$EXPECTED_SHA256" || \
  fail "source digest differs"
/usr/bin/bash --noprofile --norc -n "$SOURCE" || fail "source is not valid Bash"

TARGET_PARENT="$(dirname "$TARGET")"
test -d "$TARGET_PARENT" || fail "target parent is missing"
test ! -L "$TARGET_PARENT" || fail "target parent must not be a symlink"
test "$(stat -c '%U:%G' "$TARGET_PARENT")" = root:root || \
  fail "target parent owner differs"
test $((8#$(stat -c '%a' "$TARGET_PARENT") & 8#022)) -eq 0 || \
  fail "target parent is group/other writable"

TARGET_TMP="$(mktemp "$TARGET_PARENT/.probiga-production-deploy.XXXXXX")"
cleanup() {
  [ -z "$TARGET_TMP" ] || rm -f -- "$TARGET_TMP"
}
trap cleanup EXIT
install -o root -g root -m 0755 "$SOURCE" "$TARGET_TMP" || \
  fail "staged broker install failed"
sync -f "$TARGET_TMP" || fail "staged broker sync failed"
test "$(sha256sum "$TARGET_TMP" | cut -d' ' -f1)" = "$EXPECTED_SHA256" || \
  fail "staged broker digest differs"
STAGED_CAPABILITIES="$(SUDO_USER=probiga-deploy PROBIGA_BROKER_CLEAN_ENV=0 \
  "$TARGET_TMP" --capabilities)" || fail "staged broker capability probe failed"
test "$STAGED_CAPABILITIES" = "$EXPECTED_CAPABILITIES" || \
  fail "staged broker capabilities differ"
mv -fT "$TARGET_TMP" "$TARGET" || fail "atomic broker replacement failed"
TARGET_TMP=""
sync -f "$TARGET_PARENT" || fail "broker parent sync failed"
test -f "$TARGET" || fail "installed broker is missing"
test ! -L "$TARGET" || fail "installed broker is a symlink"
test "$(stat -c '%U:%G' "$TARGET")" = root:root || fail "installed owner differs"
test "$(stat -c '%a' "$TARGET")" = 755 || fail "installed mode differs"
test "$(sha256sum "$TARGET" | cut -d' ' -f1)" = "$EXPECTED_SHA256" || \
  fail "installed broker digest differs"
CAPABILITIES="$(SUDO_USER=probiga-deploy PROBIGA_BROKER_CLEAN_ENV=0 \
  "$TARGET" --capabilities)" || fail "installed broker capability probe failed"
test "$CAPABILITIES" = "$EXPECTED_CAPABILITIES" || \
  fail "installed broker capabilities differ"
echo "Installed reviewed production broker atomically; no service or database was changed."
