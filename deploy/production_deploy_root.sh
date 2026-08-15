#!/usr/bin/env bash
# Root-owned production deployment broker invoked through restricted sudo.

set -Eeuo pipefail
umask 077

readonly REPOSITORY=/opt/ProBigA
readonly TRUSTED_REMOTE=https://github.com/MingMG/probiga.git
readonly DEPLOY_USER=probiga-deploy

fail() {
  echo "production deploy broker: $*" >&2
  exit 2
}

test "${EUID:-$(id -u)}" -eq 0 || fail "must run as root"
test "${SUDO_USER:-}" = "$DEPLOY_USER" || fail "unexpected sudo caller"
test "$#" -eq 4 || fail "expected four release identity arguments"

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

test -d "$REPOSITORY/.git" || fail "production repository is missing"
REMOTE_SHA="$(git ls-remote "$TRUSTED_REMOTE" refs/heads/main | awk 'NR == 1 {print $1}')"
test "$REMOTE_SHA" = "$EXPECTED_SHA" || \
  fail "requested revision is not the current trusted main revision"

GIT=(git -c safe.directory="$REPOSITORY" -C "$REPOSITORY")
"${GIT[@]}" cat-file -e "${EXPECTED_SHA}^{commit}" || \
  fail "requested revision is absent from the production repository"

REQUIREMENTS_FILE="$(mktemp /root/probiga-requirements.XXXXXX)"
BOOTSTRAP_FILE="$(mktemp /root/probiga-production-deploy.XXXXXX)"
cleanup() {
  rm -f "$REQUIREMENTS_FILE" "$BOOTSTRAP_FILE"
}
trap cleanup EXIT

printf '%s' "$RESOLVED_REQUIREMENTS_B64" | base64 -d > "$REQUIREMENTS_FILE" || \
  fail "resolved requirements payload could not be decoded"
test "$(sha256sum "$REQUIREMENTS_FILE" | cut -d' ' -f1)" = \
  "$EXPECTED_REQUIREMENTS_SHA256" || fail "resolved requirements digest differs"

"${GIT[@]}" show "${EXPECTED_SHA}:deploy/production_deploy.sh" > \
  "$BOOTSTRAP_FILE"
chmod 0700 "$BOOTSTRAP_FILE"

export EXPECTED_SHA RESOLVED_REQUIREMENTS_B64 EXPECTED_REQUIREMENTS_SHA256
export EXPECTED_ADATA_SHA EXPECTED_ADATA_TREE_SHA256
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=safe.directory
export GIT_CONFIG_VALUE_0="$REPOSITORY"

bash "$BOOTSTRAP_FILE"
