# -*- coding: utf-8 -*-
import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from integrations.myquant import bridge as myquant_bridge
from integrations.myquant.bridge import (
    MyQuantBridgeError,
    UPPER_LIMIT_HISTORY_ACTION,
    UPPER_LIMIT_HISTORY_COLUMNS,
    UPPER_LIMIT_HISTORY_FIELDS,
    to_gm_symbol,
    to_stock_code,
    upper_limit_history_evidence,
)
from biz.stock_market.sync_stock_market import _myquant_daily_to_sm_kline


def _upper_limit_worker_result():
    return {
        "ok": True,
        "action": UPPER_LIMIT_HISTORY_ACTION,
        "fields": UPPER_LIMIT_HISTORY_FIELDS,
        "columns": list(UPPER_LIMIT_HISTORY_COLUMNS),
        "requested_symbols": ["SHSE.600519", "SZSE.000001"],
        "start_date": "2026-08-21",
        "end_date": "2026-08-21",
        "request_started_at": "2026-08-27T12:30:00+08:00",
        "captured_at": "2026-08-27T12:30:01+08:00",
        "timezone": "Asia/Shanghai",
        "sdk_version": "3.0.114",
        "python_version": "3.6.8",
        "entitlement_status": "SUPPORTED",
        "rows": [
            {
                "symbol": "SHSE.600519",
                "trade_date": "2026-08-21T00:00:00+08:00",
                "pre_close": 1418.0,
                "upper_limit": 1559.8,
                "lower_limit": 1276.2,
                "is_suspended": 0,
            },
            {
                "symbol": "SZSE.000001",
                "trade_date": "2026-08-21T00:00:00+08:00",
                "pre_close": 11.4,
                "upper_limit": 12.54,
                "lower_limit": 10.26,
                "is_suspended": 0,
            },
        ],
        "errors": {},
    }


def _transport_capture():
    return {
        "raw_stdout": '{"ok":true}\r\n',
        "raw_stdout_sha256": "a" * 64,
        "canonical_request_json": "{}",
        "canonical_request_sha256": "b" * 64,
        "worker_sha256": "c" * 64,
    }


def _load_worker(get_history_instruments, *, set_token=None):
    gm_module = types.ModuleType("gm")
    gm_module.__path__ = []
    api_module = types.ModuleType("gm.api")
    api_module.current = lambda **_kwargs: []
    api_module.history = lambda **_kwargs: []
    api_module.set_token = set_token or (lambda _token: None)
    api_module.get_history_instruments = get_history_instruments
    version_module = types.ModuleType("gm.__version__")
    version_module.__version__ = "test-sdk"
    spec = importlib.util.spec_from_file_location(
        "_probiga_test_myquant_worker", myquant_bridge.WORKER
    )
    if spec is None or spec.loader is None:
        raise AssertionError("could not load MyQuant worker")
    worker = importlib.util.module_from_spec(spec)
    with mock.patch.dict(
        sys.modules,
        {
            "gm": gm_module,
            "gm.api": api_module,
            "gm.__version__": version_module,
        },
    ):
        spec.loader.exec_module(worker)
    return worker


class MyQuantBridgeTest(unittest.TestCase):
    def test_runtime_prefers_explicit_then_legacy_then_qmt_python(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            explicit = root / "explicit-python.exe"
            legacy = root / "legacy-python.exe"
            qmt = root / "qmt-python.exe"
            for path in (explicit, legacy, qmt):
                path.write_bytes(b"runtime")
            with (
                mock.patch.object(myquant_bridge, "DEFAULT_PYTHON", legacy),
                mock.patch.dict(
                    "os.environ",
                    {"MYQUANT_PYTHON": str(explicit), "QMT_PYTHON": str(qmt)},
                    clear=False,
                ),
            ):
                self.assertEqual(myquant_bridge._python_path(), explicit)
            with (
                mock.patch.object(myquant_bridge, "DEFAULT_PYTHON", legacy),
                mock.patch.dict(
                    "os.environ",
                    {"MYQUANT_PYTHON": "", "EMQUANT_PYTHON": "", "QMT_PYTHON": str(qmt)},
                    clear=False,
                ),
            ):
                self.assertEqual(myquant_bridge._python_path(), legacy)
            legacy.unlink()
            with (
                mock.patch.object(myquant_bridge, "DEFAULT_PYTHON", legacy),
                mock.patch.dict(
                    "os.environ",
                    {"MYQUANT_PYTHON": "", "EMQUANT_PYTHON": "", "QMT_PYTHON": str(qmt)},
                    clear=False,
                ),
            ):
                self.assertEqual(myquant_bridge._python_path(), qmt)

    def test_symbol_mapping_supports_sh_sz_and_skips_bj(self):
        self.assertEqual(to_gm_symbol("600519"), "SHSE.600519")
        self.assertEqual(to_gm_symbol("000001"), "SZSE.000001")
        self.assertEqual(to_gm_symbol("300750"), "SZSE.300750")
        self.assertIsNone(to_gm_symbol("830799"))
        self.assertEqual(to_stock_code("SHSE.600519"), "600519")

    def test_daily_conversion_preserves_share_volume(self):
        raw = pd.DataFrame(
            [
                {
                    "symbol": "SHSE.600519",
                    "eob": "2026-06-12T00:00:00+08:00",
                    "open": 1271.18,
                    "high": 1295.0,
                    "low": 1265.01,
                    "close": 1291.91,
                    "pre_close": 1279.0,
                    "volume": 5049478,
                    "amount": 6477910214.0,
                }
            ]
        )

        converted = _myquant_daily_to_sm_kline(raw, {"600519": "贵州茅台"})

        self.assertEqual(len(converted), 1)
        row = converted.iloc[0]
        self.assertEqual(row["stock_code"], "600519")
        self.assertEqual(row["trade_date"], "2026-06-12")
        self.assertEqual(row["volume"], 5049478)
        self.assertAlmostEqual(row["change"], 12.91, places=2)
        self.assertAlmostEqual(row["change_pct"], 1.0094, places=3)

    def test_fixed_worker_action_ignores_payload_fields_override(self):
        calls = []

        def fake_get_history_instruments(**kwargs):
            calls.append(kwargs)
            return pd.DataFrame(
                [
                    {
                        "symbol": "SZSE.000001",
                        "trade_date": pd.Timestamp(
                            "2026-08-21 00:00:00", tz="Asia/Shanghai"
                        ),
                        "pre_close": 11.4,
                        "upper_limit": 12.54,
                        "lower_limit": 10.26,
                        "is_suspended": 0,
                    }
                ],
                columns=UPPER_LIMIT_HISTORY_COLUMNS,
            )

        worker = _load_worker(fake_get_history_instruments)

        result = worker._history_instruments_upper_limit(
            {
                "symbols": ["SZSE.000001"],
                "start_date": "2026-08-21",
                "end_date": "2026-08-21",
                "fields": "symbol",
            }
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(
            calls[0],
            {
                "symbols": ["SZSE.000001"],
                "fields": UPPER_LIMIT_HISTORY_FIELDS,
                "start_date": "2026-08-21",
                "end_date": "2026-08-21",
                "df": True,
            },
        )
        self.assertEqual(result["fields"], UPPER_LIMIT_HISTORY_FIELDS)
        self.assertEqual(result["entitlement_status"], "SUPPORTED")
        self.assertEqual(result["timezone"], "Asia/Shanghai")
        self.assertTrue(result["request_started_at"].endswith("+08:00"))
        self.assertTrue(result["captured_at"].endswith("+08:00"))

    def test_fixed_worker_action_fails_closed_on_empty_partial_and_duplicate_data(self):
        row = {
            "symbol": "SZSE.000001",
            "trade_date": pd.Timestamp(
                "2026-08-21 00:00:00", tz="Asia/Shanghai"
            ),
            "pre_close": 11.4,
            "upper_limit": 12.54,
            "lower_limit": 10.26,
            "is_suspended": 0,
        }
        cases = (
            (
                "empty",
                pd.DataFrame(columns=UPPER_LIMIT_HISTORY_COLUMNS),
                ["SZSE.000001"],
                "no evidence rows",
            ),
            (
                "partial",
                pd.DataFrame([row], columns=UPPER_LIMIT_HISTORY_COLUMNS),
                ["SZSE.000001", "SHSE.600519"],
                "silently omitted",
            ),
            (
                "duplicate",
                pd.DataFrame([row, row], columns=UPPER_LIMIT_HISTORY_COLUMNS),
                ["SZSE.000001"],
                "duplicate symbol/date",
            ),
        )
        for name, frame, symbols, message in cases:
            with self.subTest(name=name):
                worker = _load_worker(lambda **_kwargs: frame)
                with self.assertRaisesRegex((RuntimeError, ValueError), message):
                    worker._history_instruments_upper_limit(
                        {
                            "symbols": symbols,
                            "start_date": "2026-08-21",
                            "end_date": "2026-08-21",
                        }
                    )

    def test_fixed_worker_action_rejects_bad_symbol_before_sdk_call(self):
        get_history = mock.Mock()
        worker = _load_worker(get_history)

        with self.assertRaisesRegex(ValueError, "unsupported GM stock symbol"):
            worker._history_instruments_upper_limit(
                {
                    "symbols": ["SZSE.000001;DROP"],
                    "start_date": "2026-08-21",
                    "end_date": "2026-08-21",
                }
            )

        get_history.assert_not_called()

    def test_worker_failure_json_never_contains_token(self):
        token = "sensitive-gm-token-for-test"

        def denied(**_kwargs):
            raise RuntimeError("provider rejected {}".format(token))

        worker = _load_worker(denied)
        payload = {
            "action": UPPER_LIMIT_HISTORY_ACTION,
            "symbols": ["SZSE.000001"],
            "start_date": "2026-08-21",
            "end_date": "2026-08-21",
        }
        stdin = io.StringIO(json.dumps(payload))
        stdout = io.StringIO()
        with (
            mock.patch.dict(worker.os.environ, {"GM_TOKEN": token}),
            mock.patch.object(worker.sys, "stdin", stdin),
            mock.patch.object(worker.sys, "stdout", stdout),
        ):
            exit_code = worker.main()

        output = stdout.getvalue()
        self.assertEqual(exit_code, 1)
        self.assertNotIn(token, output)
        self.assertNotIn("traceback", output.lower())
        self.assertIn("<redacted>", output)

    def test_upper_limit_evidence_returns_exact_transport_capture(self):
        result = _upper_limit_worker_result()
        capture = _transport_capture()
        with mock.patch.object(
            myquant_bridge,
            "_run_capture",
            return_value=(result, capture),
        ) as run_capture:
            evidence = upper_limit_history_evidence(
                ["600519", "000001"],
                start_date="2026-08-21",
                end_date="2026-08-21",
                timeout=30,
            )

        run_capture.assert_called_once_with(
            {
                "action": UPPER_LIMIT_HISTORY_ACTION,
                "symbols": ["SHSE.600519", "SZSE.000001"],
                "start_date": "2026-08-21",
                "end_date": "2026-08-21",
            },
            timeout=30,
        )
        for key, value in capture.items():
            self.assertEqual(evidence[key], value)
        self.assertEqual(evidence["rows"], result["rows"])
        self.assertEqual(evidence["entitlement_status"], "SUPPORTED")

    def test_upper_limit_evidence_never_silently_drops_symbols(self):
        with mock.patch.object(myquant_bridge, "_run_capture") as run_capture:
            with self.assertRaisesRegex(MyQuantBridgeError, "unsupported symbols"):
                upper_limit_history_evidence(
                    ["600519", "830799"],
                    start_date="2026-08-21",
                    end_date="2026-08-21",
                )
            run_capture.assert_not_called()

        result = _upper_limit_worker_result()
        result["rows"] = result["rows"][:1]
        with mock.patch.object(
            myquant_bridge,
            "_run_capture",
            return_value=(result, _transport_capture()),
        ):
            with self.assertRaisesRegex(MyQuantBridgeError, "silently omitted"):
                upper_limit_history_evidence(
                    ["600519", "000001"],
                    start_date="2026-08-21",
                    end_date="2026-08-21",
                )

    def test_upper_limit_evidence_rejects_duplicate_symbols_and_noncanonical_dates(self):
        with mock.patch.object(myquant_bridge, "_run_capture") as run_capture:
            with self.assertRaisesRegex(MyQuantBridgeError, "must be unique"):
                upper_limit_history_evidence(
                    ["600519", "SHSE.600519"],
                    start_date="2026-08-21",
                    end_date="2026-08-21",
                )
            with self.assertRaisesRegex(MyQuantBridgeError, "canonical YYYY-MM-DD"):
                upper_limit_history_evidence(
                    ["600519"],
                    start_date="20260821",
                    end_date="2026-08-21",
                )
            run_capture.assert_not_called()

    def test_upper_limit_evidence_rejects_duplicate_and_out_of_range_rows(self):
        result = _upper_limit_worker_result()
        result["rows"].append(dict(result["rows"][0]))
        with mock.patch.object(
            myquant_bridge,
            "_run_capture",
            return_value=(result, _transport_capture()),
        ):
            with self.assertRaisesRegex(MyQuantBridgeError, "duplicate key"):
                upper_limit_history_evidence(
                    ["600519", "000001"],
                    start_date="2026-08-21",
                    end_date="2026-08-21",
                )

        result = _upper_limit_worker_result()
        result["rows"][0]["trade_date"] = "2026-08-20T00:00:00+08:00"
        with mock.patch.object(
            myquant_bridge,
            "_run_capture",
            return_value=(result, _transport_capture()),
        ):
            with self.assertRaisesRegex(MyQuantBridgeError, "date is out of range"):
                upper_limit_history_evidence(
                    ["600519", "000001"],
                    start_date="2026-08-21",
                    end_date="2026-08-21",
                )

    def test_upper_limit_evidence_rejects_mutated_worker_contract(self):
        mutations = (
            ("action", "history", "action mismatch"),
            ("fields", "symbol", "fields mismatch"),
            ("columns", ["symbol"], "columns mismatch"),
            ("errors", {"SZSE.000001": "denied"}, "reported symbol errors"),
            ("entitlement_status", "UNKNOWN", "entitlement is not supported"),
        )
        for field, value, message in mutations:
            with self.subTest(field=field):
                result = _upper_limit_worker_result()
                result[field] = value
                with mock.patch.object(
                    myquant_bridge,
                    "_run_capture",
                    return_value=(result, _transport_capture()),
                ):
                    with self.assertRaisesRegex(MyQuantBridgeError, message):
                        upper_limit_history_evidence(
                            ["600519", "000001"],
                            start_date="2026-08-21",
                            end_date="2026-08-21",
                        )

    def test_upper_limit_evidence_requires_is_suspended_per_row(self):
        result = _upper_limit_worker_result()
        del result["rows"][0]["is_suspended"]
        with mock.patch.object(
            myquant_bridge,
            "_run_capture",
            return_value=(result, _transport_capture()),
        ):
            with self.assertRaisesRegex(MyQuantBridgeError, "row schema is not exact"):
                upper_limit_history_evidence(
                    ["600519", "000001"],
                    start_date="2026-08-21",
                    end_date="2026-08-21",
                )

    def test_run_capture_hashes_exact_binary_stdout_and_request(self):
        stdout = b'{"ok":true}\r\n'
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=stdout, stderr=b""
        )
        payload = {"symbols": ["SZSE.000001"], "action": "probe"}
        canonical = json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with tempfile.TemporaryDirectory() as folder:
            worker_path = Path(folder) / "worker.py"
            worker_path.write_bytes(b"print('worker')\n")
            with (
                mock.patch.object(myquant_bridge, "WORKER", worker_path),
                mock.patch.object(myquant_bridge, "_get_token", return_value="secret"),
                mock.patch.object(
                    myquant_bridge, "_python_path", return_value=Path(sys.executable)
                ),
                mock.patch.object(
                    myquant_bridge.subprocess, "run", return_value=completed
                ) as run,
            ):
                result, capture = myquant_bridge._run_capture(payload, timeout=5)

        self.assertEqual(result, {"ok": True})
        self.assertEqual(capture["raw_stdout"], stdout.decode("utf-8"))
        self.assertEqual(
            capture["raw_stdout_sha256"], hashlib.sha256(stdout).hexdigest()
        )
        self.assertEqual(capture["canonical_request_json"], canonical)
        self.assertEqual(
            capture["canonical_request_sha256"],
            hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(
            capture["worker_sha256"],
            hashlib.sha256(b"print('worker')\n").hexdigest(),
        )
        self.assertEqual(run.call_args.kwargs["input"], canonical.encode("utf-8"))
        self.assertNotIn("text", run.call_args.kwargs)

    def test_run_capture_never_surfaces_worker_output_containing_token(self):
        token = "sensitive-gm-token-for-test"
        completed = subprocess.CompletedProcess(
            args=[],
            returncode=1,
            stdout=b'{"ok":false,"error":"denied"}\n',
            stderr=("provider rejected {}".format(token)).encode("utf-8"),
        )
        with tempfile.TemporaryDirectory() as folder:
            worker_path = Path(folder) / "worker.py"
            worker_path.write_bytes(b"print('worker')\n")
            with (
                mock.patch.object(myquant_bridge, "WORKER", worker_path),
                mock.patch.object(myquant_bridge, "_get_token", return_value=token),
                mock.patch.object(
                    myquant_bridge, "_python_path", return_value=Path(sys.executable)
                ),
                mock.patch.object(
                    myquant_bridge.subprocess, "run", return_value=completed
                ),
            ):
                with self.assertRaises(MyQuantBridgeError) as raised:
                    myquant_bridge._run_capture({"action": "probe"}, timeout=5)

        self.assertNotIn(token, str(raised.exception))
        self.assertIn("configured credential", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
