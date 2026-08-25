# Production release boundary v4

This document records the release prerequisites introduced by the v4 broker.
It is an operational boundary, not authorization to change production.

## Current readiness

Production deployment is deliberately fail-closed. The value in
`deploy/production_release.env` is an input to the release decision, not proof that
a deployment is ready. CI must resolve the exact reviewed `main` commit, run the
required validation, freeze the Linux dependency and wheel evidence, verify the
release manifest, and derive an explicit `READY` decision before it creates any
production SSH connection. Every non-`READY` value must terminate the workflow
before SSH; readiness may not be decided after connecting to production. The root
broker then independently checks the trusted-main manifest before it touches a
production service, database, or runtime symlink.

The deployment gate may be changed to `READY` only in one reviewed commit that
contains all of the following for CPython 3.14 on
`manylinux_2_28_x86_64`:

- a complete, transitively pinned requirements lock with a SHA-256 hash on every
  installable requirement;
- an exact wheel manifest containing every wheel filename and SHA-256 digest;
- updated lock and wheel-manifest digests in `production_release.env`; and
- a Linux validation proving the wheelhouse installs with `--require-hashes`,
  `--no-index`, and no inherited pip configuration.

A partial direct-dependency lock, a Windows-only resolution, or an unreviewed
cross-platform download must remain `BLOCKED`.

The database release proof must also bind the exact frozen inventory of 68
triggers: 40 governance, 6 point-in-time data, 6 completed-run QMT attestation,
10 immutable QMT reference, 4 QMT historical-coverage certification, and 2
scheduler-history/release-receipt triggers. Missing, additional, renamed,
cross-group substituted, or metadata-drifted triggers keep the release `BLOCKED`.

The runtime database grant transition is deliberately staged so an existing
production account does not deadlock a no-downtime release. Preflight and resume
accept exactly two contracts, and report which one was observed:

- `TARGET_LEAST_PRIVILEGE`: `biga.* = SELECT`, `probiga.* = SELECT, INSERT,
  UPDATE, DELETE, CREATE TEMPORARY TABLES`, and
  `probiga_qmt_history.* = SELECT`;
- `LEGACY_DDL_COMPATIBILITY`: the same grants plus the frozen five legacy
  `probiga.*` privileges `CREATE, ALTER, DROP, INDEX, REFERENCES`.

No partial legacy set and no additional privilege is accepted. Evidence includes
both `observed_contract` and the exact `persistent_ddl_privileges` list; validators
derive both from the observed schema grants and reject inconsistent labels or an
empty-list claim for the legacy account. This compatibility state is not a claim
that least privilege has already been reached. Production runtime code is already
prohibited from persistent DDL and all persistent schema migration goes through
the fenced migrator. In a separate reviewed maintenance window, the five legacy
privileges are revoked; the same release checks then automatically classify the
account as `TARGET_LEAST_PRIVILEGE` without a deployment-policy edit.

Neither CI `READY` nor a `DEPLOYED` receipt grants trading authority. Strategy
governance remains simulation-only, with `automatic_real_order_submission=false`
and `real_order_authority=false`. An empty eligible set is a valid result and must
remain cash; no release state is a promise of profitability.

Production also fixes `PROBIGA_IN_APP_DEPLOY_ENABLED=0`. The `/deploy` page and
all `/api/deploy/*` status, run, and detail endpoints must fail closed before any
Git, thread, or subprocess work starts; hiding a browser button is not sufficient.
When the desktop-only console is explicitly enabled, its history is stored under
an absolute protected runtime root outside the code tree. A sealed production
release must never write deploy history under `runtime/`, `data/`, or another
tracked directory.

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
