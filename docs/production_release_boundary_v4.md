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

### One-time database boundary bootstrap

The installed v4 root broker can complete the credential-file boundary without
another Cloud Assistant/root-login step. This is a one-time local handoff, not a
database-account creator and not a general secret upload channel. Before the
first eligible release, the operator places exactly these two files in
`/home/probiga-deploy/.probiga-db-boundary-stage` through the existing protected
file-transfer channel:

- `mysql-trigger-admin.ini` for `probiga_trigger_admin`;
- `mysql-migrator.ini` for `probiga_migrator`.

The stage directory must be a real `probiga-deploy:probiga-deploy` directory at
`0700`. Both entries must be regular, single-link, `probiga-deploy`-owned `0600`
files no larger than 4 KiB, and there may be no third entry. Each file has one
strict `[client]` section containing only `protocol=tcp`,
`host=127.0.0.1`, `port=13306`, its expected user, and a 48--160 character
base64url password. Passwords must never be placed in a command line, environment
variable, ticket, log, or deployment receipt.

After the immutable release has been prepared, and only on a non-same-SHA
activation, the root deploy engine validates the complete stage, fixed CA,
runtime `.env`, owners, modes, link counts, and target state. It runs only when
both target option files are absent. `/home` and `/etc` must be on the same
filesystem; otherwise the bootstrap fails before mutation rather than replacing
an atomic claim with an unsafe copy fallback.

The handoff is a durable two-phase transaction. `prepare` first writes and
fsyncs root-only option and `.env` snapshots plus original metadata under
`/etc/probiga/.database-boundary-bootstrap.transaction`. It then checks the
stage directory immediately before rename and checks the same device/inode and
non-symlink directory immediately afterwards, before any metadata change. All
directory ownership and mode changes use a no-follow directory descriptor. The
transaction provisionally installs the two option paths, pins the unique
`MYSQL_SSL_CA=/etc/probiga/mysql84-ca.pem` with unique
`MYSQL_TLS_REQUIRED=true`, sets `/opt/ProBigA/.env` to `root:probiga 0640`, and
sets only the top `/opt/ProBigA` directory to `root:root 0755`; it never applies
a recursive ownership change. The claimed stage and original state remain in
the root-only transaction while the read-only credential, TLS, identity, grant,
and schema preflight runs.

The deploy engine persists its normal writer-restore journal before `commit`.
Only then does the bootstrap durably choose commit, atomically move the
transaction to a root-only committed tombstone, remove duplicate snapshots, and
leave both final option files as single-link `root:root 0600` files. A failure
before that decision invokes `rollback`, which restores the original `.env`
bytes/ownership/mode/timestamps, application-directory metadata and exact stage,
then removes provisional targets. SIGTERM, SIGINT and SIGHUP use the same path;
SIGKILL, process loss or host loss leaves an fsynced state that the next
`prepare`, `commit`, or `rollback` resumes idempotently. Once the committing
decision is durable, recovery finishes commit instead of ambiguously restoring
credentials after database preflight has passed.

Any partial target without a matching transaction, symlink, extra stage entry,
unsafe CA, unexpected `.env` shape, permission drift, cross-filesystem claim or
concurrent replacement fails closed. Output is limited to status and SHA-256
evidence; it never includes option or `.env` contents. Same-SHA deployments use
read-only `verify` and refuse a reappeared stage. Database accounts and exact
grants are still provisioned separately in an approved DBA maintenance window;
the deploy bootstrap cannot create or broaden them.

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
