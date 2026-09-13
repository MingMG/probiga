"""Native index minute coverage, including bond and cross-market sessions.

Price indices do not all share the A-share 241-bar clock. Bond indices
continue to 15:30. CNI cross-market indices also publish during the Hong Kong
session and closing auction; those extra observations are sparse and their
timestamps vary by date. Require the complete mainland grid, retain every
native extension, and bind the complete captured frame to database readback.
"""
from datetime import date, datetime, time, timedelta

from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_history_coverage import minute_time_grid

BOND_INDICES = frozenset({
    "000012.SH", "000013.SH", "000022.SH", "000061.SH", "000101.SH", "000116.SH",
    "399289.SZ", "399290.SZ", "399298.SZ", "399299.SZ", "399301.SZ", "399302.SZ", "399481.SZ",
})
CROSS_MARKET_INDICES = frozenset({"980001.SZ", "980023.SZ", "980068.SZ", "988201.SZ"})


def _range(hour, minute, count):
    start = datetime.combine(date(2000, 1, 1), time(hour, minute))
    return tuple((start + timedelta(minutes=i)).strftime("%H:%M:%S") for i in range(count))


CORE_GRID = minute_time_grid()
BOND_GRID = (*CORE_GRID, *_range(15, 1, 30))
CROSS_GRID = tuple(sorted((*CORE_GRID, *_range(11, 31, 30), *_range(15, 1, 70))))
CONTRACT = {
    "schema": "probiga.qmt-index-minute-grid.v1",
    "core_grid": list(CORE_GRID), "bond_grid": list(BOND_GRID),
    "cross_market_allowed_grid": list(CROSS_GRID),
    "bond_indices": sorted(BOND_INDICES), "cross_market_indices": sorted(CROSS_MARKET_INDICES),
    "cross_market_extension": "retain_all_native_observations_without_filling",
}
CONTRACT_HASH = canonical_digest(CONTRACT)


def index_minute_grids(qmt_code):
    if qmt_code in BOND_INDICES:
        return BOND_GRID, BOND_GRID
    if qmt_code in CROSS_MARKET_INDICES:
        return CORE_GRID, CROSS_GRID
    return CORE_GRID, CORE_GRID


def index_minute_scope(catalog, expected_by_session):
    by_code = {member.index_code: member.qmt_code for member in catalog}
    members = []
    required_count = extra_capacity = 0
    for session, codes in sorted(expected_by_session.items()):
        for code in sorted(codes):
            required, allowed = index_minute_grids(by_code[code])
            required_count += len(required)
            extra_capacity += len(allowed) - len(required)
            members.append([session, code, canonical_digest(required), canonical_digest(allowed)])
    return {"contract_hash": CONTRACT_HASH, "required_row_count": required_count,
            "extension_capacity": extra_capacity, "member_grid_hash": canonical_digest(members)}
