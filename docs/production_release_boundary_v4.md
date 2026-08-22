# Production release boundary v4

This document records the release prerequisites introduced by the v4 broker.
It is an operational boundary, not authorization to change production.

## Current readiness

Production deployment is deliberately fail-closed. `deploy/production_release.env`
has `PROBIGA_PRODUCTION_LOCK_STATUS=BLOCKED_CROSS_PLATFORM_REGEN_REQUIRED`.
The workflow checks that field before SSH, and the root broker independently checks
the trusted-main manifest before it touches a production service, database, or
runtime symlink.

The deployment gate may be changed to `READY` only in one reviewed commit that
contains all of the following for CPython 3.14 on
`manylinux_2_17_x86_64`:

- a complete, transitively pinned requirements lock with a SHA-256 hash on every
  installable requirement;
- an exact wheel manifest containing every wheel filename and SHA-256 digest;
- updated lock and wheel-manifest digests in `production_release.env`; and
- a Linux validation proving the wheelhouse installs with `--require-hashes`,
  `--no-index`, and no inherited pip configuration.

A partial direct-dependency lock, a Windows-only resolution, or an unreviewed
cross-platform download must remain `BLOCKED`.

## External maintenance prerequisites

Before a v4 deployment can be authorized, an operator must separately verify:

1. The non-login `probiga-build` account exists. It is used only for wheel
   download/build; root performs the sealed, hash-required install.
2. GitHub deploy key and known-host files, bare source caches, and their parent
   directories meet the broker's exact root ownership, mode, no-symlink, and
   no-alternates/hooks/replace-ref/config-extension contracts.
3. Any legacy mutable code or adata checkout has been replaced by a sealed,
   root-owned release suitable for rollback. Mutable legacy paths are not accepted
   as rollback seeds.
4. The installed root broker reports the exact v4 capability document. An old v3
   broker fails before any production mutation.

The reviewed broker is installed only through the explicit out-of-band maintenance
script `deploy/install_production_deploy_broker.sh`. A root operator supplies a
root-owned staged source and an independently recorded SHA-256. The installer
syntax-checks and capability-checks the staged candidate before its atomic rename.
CI never invokes this installer.

## Recovery invariant

The activation snapshot seals both the old and proposed unit/static sets. A
mid-install failure or SIGKILL can therefore be retried to an exact old set; once
the new runtime is verified, snapshot-only recovery converges to the exact new set.
Old-runtime verification has a distinct phase and can never be mistaken for a new
runtime finalization. Persistent restore/guard state and the activation snapshot are
cleared only after the selected runtime set and writer state have been verified.
Controlled recovery loads its engine only from the recursively verified root-owned
bare cache; it does not require GitHub, a deploy key, known-hosts, or package indexes.

`DEPLOYED` receipts are written only after transaction finalization. Input-lock and
resolved-freeze SHA-256 values remain separate receipt fields.
