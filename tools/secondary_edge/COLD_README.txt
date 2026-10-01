PROBIGA - COMPLETE PAUSED WINDOWS MIGRATION

OLD COMPUTER: RUN start_target_migration.cmd FROM THIS PACKAGE.
Do NOT run start_source_migration.cmd or a SOURCEONLY/source-preparation entry
on the old computer. Those entries prepare a package on the original source;
they are not installation entries. Do not run the target installer on the source.

Copy this ENTIRE sealed directory to your mobile disk. Keep the source computer
paused. On the OLD Windows 11 64-bit computer, run start_target_migration.cmd.
Approve the Windows UAC prompt yourself. The elevated installation progress
window is visible. Keep it AND the original-user command window open. Do not
close either window, start the installer again, or unplug the mobile disk.

The progress window shows verification, software/environment installation,
copy and database stages. Long package SHA verification uses streaming blocks with
numeric progress about every two seconds: cumulative size, file count and
elapsed time, not private filenames or credentials. These are progress reports,
not a promise of a fixed completion time. Verification reaching 100 percent or
'Package verification complete' does NOT mean installation is complete.
Even a successful elevated helper means the software and stopped database are
installed; original-user account readiness can still be pending. Only the
original-user entry's final PAUSED-INSTALLED message means paused installation
AND its account-readiness checks have completed. Production is still paused.

The database materialization subprocess separately reports its fixed stage,
elapsed seconds and still-running status about every five seconds while active.
This is liveness information, NOT verified bytes, a completion percentage or
proof of installation. Its real exit status and the subsequent database,
bootstrap/TLS and final Stopped/Disabled checks must still pass.

If a software installer requires restart, restart Windows and sign back in as
the SAME original Windows user. Keep the mobile disk connected at its original
path. The owned login continuation resumes automatically; do not start another
script. Official login prompts stay on the original user's desktop, not the
elevated administrator's account. A failure reports a fixed CODE and Diagnostics
path; the failure window waits for Enter so its safe error remains readable.
The protected per-run diagnostic is normally
C:\ProgramData\ProBigA\MigrationDiagnostics\<run-GUID>\status.json (the system's
CommonApplicationData directory). It records safe stage/status/error codes,
not original provider exceptions, filenames or credentials. Keep this diagnostic
for support. A machine-wide helper lock rejects concurrent installations;
that rejection is not permission to interfere with the active run.

The package includes the stopped MySQL 8.4.11 physical data, all logs, its formal
configuration and TLS certificates; the complete stopped QMT directory; exact
merged-main code; both Windows Python wheelhouses and signed installers; the
complete Codex CLI bundle; and the two exact production history archives.

Private source-project-state archives also preserve BOTH source project roots'
complete data (including local main.db), backfill evidence, caches, logs and
reports, backtest outputs and acceptance artifacts when present. Legacy
embedded Python 3.6, emquant wheels and installer
artifacts are preserved only in this private archive. They are NOT installed
or copied into the active target runtime, and Goldminer does not replace QMT.
The source DeepSeek browser must be closed before its data can be cold-copied.
Two reviewed historical backtest report node_modules junctions are preserved
as complete ordinary directories, not source-machine absolute links. Only
those exact junction locations and the reviewed report Node dependency root
are allowed; unknown links or inner dependency links fail closed. The matching
Node executable is preserved privately under machine-assets/report-node-runtime.
These historical report dependencies are archive-only: no Node installation,
PATH change, automatic report execution or production activation is performed.
Source link provenance is recorded; all materialized files are byte-verified.
All selected archives are preflighted before any large software copy starts.
The archived machine-bound browser state is NOT reusable-login authorization.
Source .env files and Windows credential stores are not part of these archives.
Only four reviewed, hash-pinned Windows app registration TOOL SOURCE files are
preserved separately under operation-tools/windows-app-registration. This does
NOT install/run the tool, copy credentials, or enroll a target QMT account.
Private installation/login-verification receipts and other files from that
source credential-tool directory are excluded. Any changed tool needs review.
The two exact EasyOCR model files, when present, are preserved under private
machine-assets/easyocr/model; an optional proven non-secret legacy QMT alert
state is preserved under machine-assets/legacy-alert-state. That exact reviewed
JSON is hash-pinned; changed/unreviewed alert state is excluded without copying
its values. These assets are NOT loaded, activated,
or given runtime environment settings. No Tesseract engine is claimed installed
merely because its Python wrapper is included in the Python environment.
Two exact historical business SQL backups are preserved when present. Other
historical _archive code/deployment scripts stay on the source, are not deleted,
and are outside this runtime migration scope. The package does not claim to
clone every file under the entire source working directory.

An NTFS target disk is required. The installer checks the measured package size
plus 30 GiB reserve, with a minimum of 250 GiB free. A 1 TB mechanical disk can
take many hours to copy and verify. Do not unplug the mobile disk during work.
Keep the old computer connected to AC power with its lid open. Both the
original-user entry and elevated installer hold their own installation-lifetime
Windows SystemRequired power request before lengthy work. They check AC power
and the active AC policy allowing these requests; unavailable/disabled protection
blocks installation before large verification or target changes. The requests
are released on success, error and reboot-required exit, and reacquired by the
same login continuation after restart. No permanent power-plan setting changes.
This does NOT override deliberate Sleep, lid closure, power loss, an existing
administrator request override, or later changes to power policy/AC supply.
Production power policy is a separate restoration acceptance requirement.

SECURITY: This is a confidential physical backup. It contains original database
account password hashes, TLS private keys, QMT private data and private history.
Keep this disk physically secure. The installer creates a separate target root
credential, generates a new database UUID and locks inherited business accounts.
Locking accounts does NOT remove their hashes from the physical copy.

The source is NOT automatically restarted after either success or failure.
Interrupted exports are never overwritten or treated as complete; preserve the
failed directory, resolve the cause and use a NEW output directory. READY seals
both the merged build and the manifest hash. Modified/incomplete packages fail.
READY is published only after complete content verification, source pause
rechecks and portable-entry permissions are complete. Do not run an unpublished
or still-running export, even when its copied-byte count has reached 100 percent.

Before any target installation has started, an installer-only merged-main
release can formally republish an already verified complete cold package using
the source-only republish_cold_package.ps1 workflow. It requires an explicitly
pinned ExpectedPreviousBuild, a TargetInstallationNotStarted declaration and a
NEW external JournalRoot. Wait for the previous exporter to exit successfully
before using it. It validates the complete
old and new payload, preserves the original database snapshot, software and
archive bytes, and rebuilds the main bundle and all installation entries from
one clean merged revision. Dependency, database/protocol or other runtime
changes cannot reuse these assets. An external protected release journal keeps
the previous code and seal. Checks that block before withdrawing the old READY
leave the unchanged old release intact. Failure after withdrawal has no READY
and never resumes the project. This is not permission to edit a sealed package
by hand, update an already-started target installation, or resume an incomplete
export.
The new manifest date describes the new code publication, not a new database
snapshot. Original snapshot identity, pause evidence and times remain unchanged.
Installer-only code publication does not replace or alter the existing physical
MySQL snapshot, QMT/software data or archived business data. A package with no
valid READY is not a reusable complete package; it must not be resumed/sealed
as though an interrupted export had succeeded.

The old computer installs and verifies locally, then remains PAUSED. It does not
start collection, production scheduled tasks, an AI queue worker, native QMT
strategy, orders, or the production reverse SSH tunnel. The MySQL production
service must finish Stopped and Disabled. Any failed graceful stop is an error,
not a successful paused migration. Do not manually turn production tasks on.

Logins tied to Windows identity are not portable. Follow the original-user
prompts for official Codex login and DeepSeek login. QMT broker login and native
client validation are deferred until separately authorized restoration, because
the copied client configuration can auto-run a strategy when opened. Archived
stock/general history is preserved; preservation alone is NOT proof that the
provider has enrolled the old machine or can resume the original thread IDs.
Do not copy personal CODEX_HOME authentication databases or DPAPI credentials.

The Linux service stays in its existing location. Production restoration is a
SEPARATE, explicitly authorized coordinated action after this migration passes:
new hostname/database UUID enrollment and seal; source-owner fencing; correct
Linux credentials/TLS binding; exactly one production reverse SSH 13306 owner;
exactly one Windows/AI writer; and verified QMT and history continuity. No
compatibility bypass, source-host impersonation or automatic cutover is allowed.

Preserve the original stopped source and the immutable package for rollback.
After new production business writes begin, reverting to a stale old snapshot
is not a data-safe rollback and needs a reviewed recovery procedure.
