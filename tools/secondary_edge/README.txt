OLD PC INSTALLATION: run start_target_migration.cmd from the ENTIRE sealed
mobile-disk package. Approve UAC. Do NOT run start_source_migration.cmd or any
SOURCEONLY/source-preparation entry on the old PC; those are source-only package
preparation, not target installation. Never run the target entry on the source.

Keep the original-user command window and visible elevated progress window open.
Do not close them, repeat the installer or unplug the mobile disk while working.
Long package SHA verification reports numeric size/file-count/elapsed progress about
every two seconds, without private filenames or credentials. Stages also cover
offline software, environments, copies and the database. Verification at 100
percent is NOT complete installation. Elevated helper success can still leave
original-user official account readiness pending. Only the final PAUSED-INSTALLED
message from the original-user entry means paused installation is complete.
It does not mean QMT, collection, AI workers, orders or production are running.
The database subprocess separately shows its fixed stage, elapsed seconds and
still-running status about every five seconds. These are NOT verified bytes,
a completion percentage or proof that installation is complete. Its real exit
status and the subsequent database/bootstrap/TLS/final pause checks still apply.

If a restart is required, restart Windows, leave the mobile disk connected at
the original path, and sign in as the SAME Windows user. The owned continuation
resumes automatically; do not start a second script. Official account logins
remain on that original user's desktop. QMT broker login remains deferred until
separately authorized restoration, because opening its saved configuration can
auto-run strategies. Logins/DPAPI credentials are not made portable by this copy.

Failure shows a fixed CODE and a safe Diagnostics path, and keeps the failed
window readable until Enter. The protected per-run status.json is normally under
C:\ProgramData\ProBigA\MigrationDiagnostics\<run-GUID> (system CommonApplicationData).
It records safe status/stage/error codes, not private provider/error inputs.
A machine-wide lock rejects another elevated helper while one is active.
Preserve failure evidence; no automatic source resume or production rollback.

Source preparation uses start_source_migration.cmd on the SOURCE only. A formal
installer-only code publication can reuse a verified COMPLETE sealed package
before target installation starts, through the reviewed source-only release
workflow. It does not alter the existing physical MySQL snapshot or data and is
not an incomplete-export continuation. Never edit the sealed package by hand.
See COLD_README.txt for the complete publication rules, archive scope, power
protection, official-login limits and permanent paused-installation lifecycle.
The prior parallel/private-candidate migration entry points have been removed.
Linux remains at its current location. Production restoration is a later,
explicitly authorized coordinated operation, never an installation finally step.
Migration does not deploy or restart Linux. Restoring production needs separate
coordinated identity/TLS ownership, source fencing and sole tunnel/writer checks.
