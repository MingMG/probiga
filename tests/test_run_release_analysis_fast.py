from __future__ import annotations

from tools import run_release_analysis_fast as release_analysis


class _Result:
    def __init__(self, *, scalar_value=0, rows=()):
        self._scalar_value = scalar_value
        self._rows = rows

    def scalar(self):
        return self._scalar_value

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _Connection:
    def __init__(self, *, flow_rows: int, upper_rows=()):
        self._results = iter((
            _Result(scalar_value=flow_rows),
            _Result(rows=upper_rows),
        ))

    def execute(self, *_args, **_kwargs):
        return next(self._results)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Engine:
    def __init__(self, *, flow_rows: int, upper_rows=()):
        self._flow_rows = flow_rows
        self._upper_rows = upper_rows

    def connect(self):
        return _Connection(
            flow_rows=self._flow_rows,
            upper_rows=self._upper_rows,
        )


def test_release_readiness_allows_complete_core_data_without_upper_evidence():
    result = release_analysis._upper_readiness(
        _Engine(flow_rows=5207),
        target="2026-09-04",
        build_sha="1" * 40,
    )

    assert result == {
        "ready": True,
        "flow_rows": 5207,
        "upper_evidence_available": False,
        "upper_run_id": "",
        "decision_at": "",
    }


def test_release_readiness_still_blocks_incomplete_core_data():
    result = release_analysis._upper_readiness(
        _Engine(flow_rows=4999),
        target="2026-09-04",
        build_sha="1" * 40,
    )

    assert result["ready"] is False
    assert result["upper_evidence_available"] is False
