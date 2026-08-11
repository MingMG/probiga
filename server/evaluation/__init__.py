"""Read-only experiment comparison and baseline governance.

This package is intentionally outside :mod:`server.trading_v4`.  Evaluation
may read frozen V3 and committed V4 outputs, while the V4 decision runtime is
not allowed to depend on evaluation or on any legacy decision package.
"""

from .baseline_epoch import (
    BaselineEpoch,
    BaselineEpochError,
    hash_artifact_paths,
)
from .v3_baseline_manifest import (
    REQUIRED_EVIDENCE_TYPES,
    V3_BASELINE_MANIFEST_SCHEMA,
    V3BaselineEvidenceFile,
    V3BaselineEvidenceManifest,
    V3BaselineManifestError,
    build_v3_baseline_manifest,
    load_v3_baseline_manifest,
    resolve_repository_commit,
    verify_v3_baseline_manifest,
)

__all__ = (
    "BaselineEpoch",
    "BaselineEpochError",
    "REQUIRED_EVIDENCE_TYPES",
    "V3_BASELINE_MANIFEST_SCHEMA",
    "V3BaselineEvidenceFile",
    "V3BaselineEvidenceManifest",
    "V3BaselineManifestError",
    "build_v3_baseline_manifest",
    "hash_artifact_paths",
    "load_v3_baseline_manifest",
    "resolve_repository_commit",
    "verify_v3_baseline_manifest",
)
