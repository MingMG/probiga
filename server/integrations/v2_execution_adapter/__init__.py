"""Pure compatibility mappings from frozen V2 execution contracts."""

from .accounting import (
    V2_MARKET_TIMEZONE,
    apply_v2_compatible_fill,
    build_v2_accounting_fill_request,
    empty_v2_accounting_state,
)

from .fees import v2_fee_profile_to_neutral_schedule, v2_fill_gross
from .matcher import (
    V2MatchProjection,
    V2NeutralMatcherInput,
    map_v2_level1_match_inputs,
    match_v2_level1_read_only,
    project_neutral_match_to_v2,
)
from .snapshot import (
    V2NeutralSnapshotMatcherInput,
    V2_SNAPSHOT_SOURCE,
    map_v2_snapshot_match_inputs,
    match_v2_snapshot_read_only,
    project_neutral_snapshot_match_to_v2,
)

__all__ = [
    "V2MatchProjection",
    "V2NeutralMatcherInput",
    "V2NeutralSnapshotMatcherInput",
    "V2_SNAPSHOT_SOURCE",
    "V2_MARKET_TIMEZONE",
    "map_v2_level1_match_inputs",
    "match_v2_level1_read_only",
    "map_v2_snapshot_match_inputs",
    "match_v2_snapshot_read_only",
    "project_neutral_match_to_v2",
    "project_neutral_snapshot_match_to_v2",
    "apply_v2_compatible_fill",
    "build_v2_accounting_fill_request",
    "empty_v2_accounting_state",
    "v2_fee_profile_to_neutral_schedule",
    "v2_fill_gross",
]
