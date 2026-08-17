#!/usr/bin/env bash
# Root-owned production deployment broker invoked through restricted sudo.

set -Eeuo pipefail
umask 077

readonly LEGACY_REPOSITORY=/opt/ProBigA
readonly RELEASE_SOURCE_ROOT=/var/lib/probiga/release-sources
readonly CODE_GIT_CACHE="$RELEASE_SOURCE_ROOT/probiga.git"
readonly BROKER_LOCK_ROOT=/run/probiga
readonly BROKER_LOCK_FILE="$BROKER_LOCK_ROOT/production-broker.lock"
readonly DEPLOY_PROTOCOL_VERSION=probiga-production-deploy-v2
readonly TRUSTED_REMOTE=git@github.com:MingMG/probiga.git
readonly DEPLOY_USER=probiga-deploy
readonly GITHUB_SSH_KEY=/etc/probiga/github-readonly-ed25519
readonly GITHUB_KNOWN_HOSTS=/etc/probiga/github_known_hosts
readonly REMOTE_GIT_SSH="ssh -i $GITHUB_SSH_KEY -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=$GITHUB_KNOWN_HOSTS"

fail() {
  echo "production deploy broker: $*" >&2
  exit 2
}

test "${EUID:-$(id -u)}" -eq 0 || fail "must run as root"
test "${SUDO_USER:-}" = "$DEPLOY_USER" || fail "unexpected sudo caller"
test "$#" -eq 4 || fail "expected four release identity arguments"
test ! -L "$BROKER_LOCK_ROOT" || fail "broker lock root must not be a symlink"
install -d -o root -g root -m 0700 "$BROKER_LOCK_ROOT"
test "$(readlink -f "$BROKER_LOCK_ROOT")" = "$BROKER_LOCK_ROOT" || \
  fail "broker lock root resolves unexpectedly"
test ! -L "$BROKER_LOCK_FILE" || fail "broker lock file must not be a symlink"
touch "$BROKER_LOCK_FILE"
chown root:root "$BROKER_LOCK_FILE"
chmod 0600 "$BROKER_LOCK_FILE"
exec 8>"$BROKER_LOCK_FILE"
if ! flock -n 8; then
  fail "another production deploy broker is active"
fi
REPOSITORY_BUILD=""
REQUIREMENTS_FILE=""
BOOTSTRAP_FILE=""
cleanup() {
  rm -f -- "$REQUIREMENTS_FILE" "$BOOTSTRAP_FILE"
  if [ -n "$REPOSITORY_BUILD" ]; then
    case "$REPOSITORY_BUILD" in
      "$RELEASE_SOURCE_ROOT"/probiga-git.*) rm -rf -- "$REPOSITORY_BUILD" ;;
    esac
  fi
}
trap cleanup EXIT

EXPECTED_SHA="$1"
EXPECTED_REQUIREMENTS_SHA256="$2"
EXPECTED_ADATA_SHA="$3"
EXPECTED_ADATA_TREE_SHA256="$4"

[[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || fail "invalid release SHA"
[[ "$EXPECTED_REQUIREMENTS_SHA256" =~ ^[0-9a-f]{64}$ ]] || \
  fail "invalid requirements digest"
[[ "$EXPECTED_ADATA_SHA" =~ ^[0-9a-f]{40}$ ]] || fail "invalid adata SHA"
[[ "$EXPECTED_ADATA_TREE_SHA256" =~ ^[0-9a-f]{64}$ ]] || \
  fail "invalid adata tree digest"

IFS= read -r RESOLVED_REQUIREMENTS_B64 || \
  fail "resolved requirements payload is missing"
test -n "$RESOLVED_REQUIREMENTS_B64" || \
  fail "resolved requirements payload is empty"
test "${#RESOLVED_REQUIREMENTS_B64}" -le 2097152 || \
  fail "resolved requirements payload is too large"
[[ "$RESOLVED_REQUIREMENTS_B64" =~ ^[A-Za-z0-9+/=]+$ ]] || \
  fail "resolved requirements payload is not canonical base64"

test -d "$LEGACY_REPOSITORY/.git" || fail "production repository is missing"
test -r "$GITHUB_SSH_KEY" || fail "GitHub read-only deploy key is missing"
test -r "$GITHUB_KNOWN_HOSTS" || fail "GitHub known-hosts file is missing"
REMOTE_SHA="$(GIT_SSH_COMMAND="$REMOTE_GIT_SSH" \
  git ls-remote "$TRUSTED_REMOTE" refs/heads/main | awk 'NR == 1 {print $1}')"
test "$REMOTE_SHA" = "$EXPECTED_SHA" || \
  fail "requested revision is not the current trusted main revision"

test ! -L "$RELEASE_SOURCE_ROOT" || fail "release source root must not be a symlink"
install -d -o root -g root -m 0755 "$RELEASE_SOURCE_ROOT"
test "$(readlink -f "$RELEASE_SOURCE_ROOT")" = "$RELEASE_SOURCE_ROOT" || \
  fail "release source root resolves unexpectedly"
test ! -L "$CODE_GIT_CACHE" || fail "release mirror must not be a symlink"
if [ ! -d "$CODE_GIT_CACHE" ]; then
  REPOSITORY_BUILD="$(mktemp -d "$RELEASE_SOURCE_ROOT/probiga-git.XXXXXX")"
  if ! git init --bare "$REPOSITORY_BUILD/repository.git" || \
    ! git --git-dir="$REPOSITORY_BUILD/repository.git" remote add origin \
      "$TRUSTED_REMOTE" || \
    ! mv "$REPOSITORY_BUILD/repository.git" "$CODE_GIT_CACHE" || \
    ! rmdir "$REPOSITORY_BUILD"; then
    rm -rf "$REPOSITORY_BUILD"
    fail "independent release mirror could not be initialized"
  fi
  REPOSITORY_BUILD=""
fi
test "$(git --git-dir="$CODE_GIT_CACHE" rev-parse --is-bare-repository)" = true || \
  fail "release source is not a bare Git mirror"
test "$(git --git-dir="$CODE_GIT_CACHE" remote get-url origin)" = \
  "$TRUSTED_REMOTE" || fail "release mirror remote differs"
GIT=(git --git-dir="$CODE_GIT_CACHE")
GIT_SSH_COMMAND="$REMOTE_GIT_SSH" \
  "${GIT[@]}" fetch --no-tags origin \
    "+refs/heads/main:refs/remotes/origin/main"
test "$("${GIT[@]}" rev-parse refs/remotes/origin/main)" = "$EXPECTED_SHA" || \
  fail "fetched release mirror tip differs"
"${GIT[@]}" cat-file -e "${EXPECTED_SHA}^{commit}" || \
  fail "requested revision is absent from the release mirror"

REQUIREMENTS_FILE="$(mktemp /root/probiga-requirements.XXXXXX)"
BOOTSTRAP_FILE="$(mktemp /root/probiga-production-deploy.XXXXXX)"

printf '%s' "$RESOLVED_REQUIREMENTS_B64" | base64 -d > "$REQUIREMENTS_FILE" || \
  fail "resolved requirements payload could not be decoded"
test "$(sha256sum "$REQUIREMENTS_FILE" | cut -d' ' -f1)" = \
  "$EXPECTED_REQUIREMENTS_SHA256" || fail "resolved requirements digest differs"

"${GIT[@]}" show "${EXPECTED_SHA}:deploy/production_deploy.sh" > \
  "$BOOTSTRAP_FILE"
chmod 0700 "$BOOTSTRAP_FILE"

export EXPECTED_SHA RESOLVED_REQUIREMENTS_B64 EXPECTED_REQUIREMENTS_SHA256
export EXPECTED_ADATA_SHA EXPECTED_ADATA_TREE_SHA256
export PROBIGA_DEPLOY_PROTOCOL_VERSION="$DEPLOY_PROTOCOL_VERSION"
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=safe.directory
export GIT_CONFIG_VALUE_0="$LEGACY_REPOSITORY"

bash "$BOOTSTRAP_FILE"
