"""Small independent acquisition runs; no old scheduler, provider or release imports."""
from contextlib import contextmanager
from datetime import date, datetime, time as day_time, timedelta
import json
import os
from pathlib import Path
import time
from uuid import uuid4
from zoneinfo import ZoneInfo

from .datasets import get_spec
from .models import WorkUnit, DatasetSpec, NormalizedBatch, NormalizedUnit
from .normalize import normalize_batch, NormalizationError, _timestamp
from .plan import eligible_codes, flow_dependency, latest_closed, plan_units, sessions, summarize, refresh_cutoff
from .qmt_model import publish_json, read_json, MAX_RESULT_BYTES, MAX_REQUEST_BYTES, history_allowed
from .qmt_transport import QmtTransport
from .store import Store, safe_error

SHANGHAI = ZoneInfo("Asia/Shanghai")
SOURCE_ERRORS = {"SOURCE_ACCESS_DENIED", "SOURCE_UNAVAILABLE", "INVALID_RETRY_AFTER", "NATIVE_CALL_FAILED"}
CONFIG_ERRORS = {"UNSUPPORTED_UNITS", "UNSUPPORTED_TIME_GRID", "INVALID_RESPONSE", "MISSING_SOURCE_METHOD"}


def source_group(dataset):
    spec = get_spec(dataset)
    if spec.source == "guojin_qmt":
        return spec.source
    # Different Eastmoney hosts are separate failure domains.
    return "eastmoney.alist" if dataset in {"alist_daily", "alist_detail"} else "eastmoney." + dataset


def reference_spec(asset_class, period):
    table, key = {"stock": ("si_all_code", "stock_code"), "index": ("si_all_index_code", "index_code"),
                  "etf": ("si_etf_code", "etf_code")}[asset_class]
    keys = (key,)
    if period == "sector":
        table, key, keys = "qmt_sector_member", "qmt_code", ("sector_name", "qmt_code")
    elif period == "calendar":
        table, key, keys = "si_trade_calendar", "trade_date", ("trade_date",)
    elif period != "instrument":
        raise ValueError("unsupported reference period")
    return DatasetSpec("reference", "guojin_qmt", table, "primary", key, keys, period,
                       ("none",), asset_class, day_time(8, 30), persisted_source="gj_big_qmt_inner")


@contextmanager
def process_lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError("acquisition command is already running") from exc
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def units_from_request(request):
    return [WorkUnit(request["dataset"], request["source"], request["start_date"], code,
                     request["period"], request["adjustment"]) for code in request["codes"]]


def make_request(units, now, timeout=180):
    first = units[0]
    if any((u.dataset, u.source, u.target_date, u.period, u.adjustment) !=
           (first.dataset, first.source, first.target_date, first.period, first.adjustment) for u in units):
        raise ValueError("a batch must have one dataset, date and adjustment")
    return {"request_id": uuid4().hex, "dataset": first.dataset, "source": first.source,
            "codes": [u.code for u in units], "start_date": first.target_date,
            "end_date": first.target_date, "period": first.period, "adjustment": first.adjustment,
            "requested_at": now.isoformat(), "deadline_at": (now + timedelta(seconds=timeout)).isoformat()}


class Runner:
    def __init__(self, config, *, engines=None, provider=None, clock=None):
        self.config = config
        self.clock = clock or (lambda: datetime.now(SHANGHAI))
        self.provider = provider
        self._engines = engines or {}
        self._stores = {}
        self.qmt = None  # read-only status does not even create transport folders
        self.errors = []

    def store(self, database):
        if database not in self._stores:
            if database not in self._engines:
                self._engines[database] = self.config.engine(database)
            self._stores[database] = Store(self._engines[database])
        return self._stores[database]

    def close(self):
        for engine in self._engines.values():
            engine.dispose()

    def catalog(self, spec):
        result = self.store("primary").catalog(spec.asset_class)
        for row in result.values():
            row["instrument_asset"] = spec.asset_class
            row.setdefault("asset_class", spec.asset_class)
        return result

    def _target(self, spec, requested="latest"):
        now = self.clock()
        if requested != "latest":
            target = date.fromisoformat(requested)
            if target > now.date():
                raise ValueError("future target is not allowed")
        elif spec.event_data:
            target = now.date() if now.time().replace(tzinfo=None) >= spec.ready_time else now.date() - timedelta(days=1)
        else:
            calendar = self.store("primary").calendar(now.date() - timedelta(days=31), now.date())
            return latest_closed(calendar, now, spec.ready_time)
        if not spec.event_data:
            calendar = self.store("primary").calendar(target, target)
            if not sessions(calendar, target.isoformat(), target.isoformat()):
                raise ValueError("target is not a trading session")
            if target == now.date() and now.time().replace(tzinfo=None) < spec.ready_time:
                raise ValueError("target is not ready for this dataset")
        return target.isoformat()

    def _consume(self, raw):
        request = raw["request"]
        if request["dataset"] == "reference":
            return self._consume_reference(raw)
        spec = get_spec(request["dataset"])
        batch = normalize_batch(spec, raw, **self.config.normalization(self.catalog(spec)))
        if spec.period == "tick":
            for unit in batch.units:
                if unit.status == "complete" and any(
                    (self.clock() - _timestamp(row["trade_time"])).total_seconds() > 180 for row in unit.rows
                ):
                    unit.status, unit.rows, unit.error_code, unit.error = "error", [], "STALE_QUOTE", "native quote is older than 180 seconds"
        result = self.store(spec.database).commit(spec, batch)
        result["error_codes"] = sorted({u.error_code for u in batch.units if u.status == "error"})
        return result

    def _consume_reference(self, raw):
        from .reference import normalize_reference, extract_sector_codes, merge_calendar_rows
        request = raw["request"]
        asset, period = request["asset_class"], request["period"]
        if period == "instrument":
            spec, batch = normalize_reference(raw, asset)
            classifications = self.config.data.get("etf_asset_classes", {})
            for result in batch.units:
                if asset == "etf" and result.status == "complete" and result.unit.code in classifications:
                    result.rows[0]["asset_class"] = classifications[result.unit.code]
        else:
            spec = reference_spec(asset, period)
            converted = []
            for unit in units_from_request(request):
                local = dict(raw, request=dict(request, codes=[unit.code]),
                             outcomes={unit.code: raw.get("outcomes", {}).get(unit.code)})
                try:
                    if period == "sector":
                        codes = extract_sector_codes(local)
                        rows = [{"sector_name": unit.code, "qmt_code": code, "stock_code": code.split(".")[0],
                                 "exchange": code.split(".")[1]} for code in codes]
                    else:
                        existing = self.store("primary").calendar(unit.target_date, unit.target_date)
                        rows = merge_calendar_rows(local, existing)
                    converted.append(NormalizedUnit(unit, "complete", rows))
                except NormalizationError as exc:
                    converted.append(NormalizedUnit(unit, "error", [], exc.code, str(exc)))
            batch = NormalizedBatch(request["request_id"], converted, _timestamp(raw["received_at"]))
        result = self.store("primary").commit(spec, batch)
        result["error_codes"] = sorted({u.error_code for u in batch.units if u.status == "error"})
        return result

    def _qmt_transport(self):
        if self.qmt is None:
            self.qmt = QmtTransport(str(self.config.state_dir / "qmt"))
        return self.qmt

    def recover_qmt(self):
        transport = self._qmt_transport()
        inventory = transport.recover()
        active = inventory["active"]
        # Receive persisted results FIRST. A timed out native operation is not
        # assumed cancelled, and a late response is still valuable.
        if active:
            request_id = active["request_id"]
            result = transport.read_result(request_id)
            if result is None:
                return False
            self._consume(result)
            transport.archive(request_id)
        for request_id in inventory["prepared"]:
            if active and request_id == active["request_id"]:
                continue
            request = read_json(str(Path(transport.root) / (request_id + ".prepared.json")), MAX_REQUEST_BYTES)
            if not request:
                continue
            spec = get_spec(request["dataset"])
            self.store(spec.database).begin_request(units_from_request(request), request_id, self.clock())
            transport.activate(request_id)
            result = transport.read_result(request_id)
            if result is None:
                return False
            self._consume(result)
            transport.archive(request_id)
        return True

    def _http_root(self):
        root = self.config.state_dir / "http"
        root.mkdir(parents=True, exist_ok=True)
        (root / "processed").mkdir(exist_ok=True)
        return root

    def recover_http(self):
        root = self._http_root()
        for prepared in sorted(root.glob("*.prepared.json")):
            try:
                self._recover_http_request(root, prepared)
            except Exception as exc:
                # A retained bad/DB-blocked batch must not stop other products.
                self.errors.append({"request_file": prepared.name, "error": safe_error(exc)})

    def _recover_http_request(self, root, prepared):
        request = read_json(str(prepared), MAX_REQUEST_BYTES)
        spec = get_spec(request["dataset"])
        store = self.store(spec.database)
        units = units_from_request(request)
        store.begin_request(units, request["request_id"], self.clock())
        ready = root / (request["request_id"] + ".ready.json")
        raw = read_json(str(ready), MAX_RESULT_BYTES)
        if raw is None:
            # No network fetch survives process exit. Preserve the old
            # attempt as an error, then let normal bounded planning retry.
            raw = {"request": request, "received_at": self.clock().isoformat(), "source_method": "not_called",
                   "outcomes": {u.code: {"status": "error", "rows": [], "error_code": "INTERRUPTED_HTTP",
                                          "reason": "previous HTTP attempt has no persisted result"} for u in units}}
            publish_json(str(ready), raw, MAX_RESULT_BYTES, immutable=True)
        if raw.get("request") != request:
            raise ValueError("persisted HTTP result identity differs")
        self._consume(raw)
        self._archive_http(root, request["request_id"])

    @staticmethod
    def _archive_http(root, request_id):
        # Results move first; a retained prepared file is the recovery pointer.
        # Keep hard links until both archived artifacts are durable.
        for suffix in (".ready.json", ".prepared.json"):
            origin, destination = root / (request_id + suffix), root / "processed" / (request_id + suffix)
            if not destination.exists():
                os.link(origin, destination)
        for suffix in (".prepared.json", ".ready.json"):
            (root / (request_id + suffix)).unlink(missing_ok=True)

    def _check_qmt_available(self, transport):
        heartbeat = transport.heartbeat()
        try:
            age = (self.clock() - _timestamp(heartbeat.get("updated_at"))).total_seconds()
        except (NormalizationError, TypeError):
            age = float("inf")
        if heartbeat.get("status") not in {"idle", "awaiting_commit"} or not -5 <= age <= 30:
            raise RuntimeError("QMT_MODEL_UNAVAILABLE")

    def acquire(self, units, budget_remaining, *, request=None, spec=None):
        request = request or make_request(units, self.clock(), min(180, max(1, budget_remaining)))
        spec = spec or get_spec(units[0].dataset)
        store = self.store(spec.database)
        store.validate_spec(spec)  # cached schema/UNIQUE check, not a deep data audit
        if spec.source == "guojin_qmt":
            transport = self._qmt_transport()
            if transport.recover()["active"]:
                return {"waiting": True}
            self._check_qmt_available(transport)
            transport.prepare(request)
            store.begin_request(units, request["request_id"], self.clock())
            transport.activate(request["request_id"])
            raw = transport.wait_result(request["request_id"], timeout=min(180, budget_remaining))
            result = self._consume(raw)
            transport.archive(request["request_id"])
            return result
        root = self._http_root()
        publish_json(str(root / (request["request_id"] + ".prepared.json")), request, MAX_REQUEST_BYTES, immutable=True)
        store.begin_request(units, request["request_id"], self.clock())
        if self.provider is None:
            from .providers.eastmoney import EastmoneyProvider
            self.provider = EastmoneyProvider()
        raw = self.provider.fetch_batch(spec.name, request)
        if raw.get("request") != request:
            raise ValueError("HTTP result identity differs")
        publish_json(str(root / (request["request_id"] + ".ready.json")), raw, MAX_RESULT_BYTES, immutable=True)
        result = self._consume(raw)
        self._archive_http(root, request["request_id"])
        return result

    def run(self, datasets, requested="latest", *, start=None, end=None, budget_seconds=1200, due=False):
        self.config.require_writes()
        self.errors = []
        results = {}
        blocked_sources = set()
        deadline = time.monotonic() + min(float(budget_seconds), 1200)
        with process_lock(self.config.state_dir / "daily.lock"):
            self.recover_http()
            try:
                qmt_ready = self.recover_qmt()
            except Exception as exc:
                qmt_ready = False
                self.errors.append({"source": "guojin_qmt", "error": safe_error(exc)})
            # One bounded progress query per configured DB, not a business-data audit.
            databases = {get_spec(n).database for n in (self.config.data.get("datasets") or datasets)}
            for database in databases:
                try:
                    for state in self.store(database).retrying_sources(self.clock()):
                        blocked_sources.add(source_group(state["dataset"]))
                except Exception as exc:
                    self.errors.append({"database": database, "error": safe_error(exc)})
            for name in datasets:
                if time.monotonic() >= deadline:
                    break
                spec = get_spec(name)
                if source_group(name) in blocked_sources:
                    results[name] = {"status": "source_cooldown"}
                    self.errors.append({"dataset": name, "error": "SOURCE_COOLDOWN"})
                    continue
                if spec.period == "tick" or spec.name == "reference":
                    self.errors.append({"dataset": name, "error": "USE_SPECIALIZED_ENTRY"})
                    continue
                if spec.source == "guojin_qmt" and not qmt_ready:
                    self.errors.append({"dataset": name, "error": "QMT_ACTIVE_REQUEST_RETAINED"})
                    continue
                if spec.source == "guojin_qmt" and not history_allowed(self.clock()):
                    results[name] = {"status": "waiting_for_history_window"}
                    continue
                try:
                    target = self._target(spec, end or requested)
                    catalog = self.catalog(spec)
                    states = self.store(spec.database).states(name)
                    first = start or self.config.data["start_date"]
                    if first > target:
                        continue
                    if spec.event_data:
                        days = [(date.fromisoformat(first) + timedelta(days=i)).isoformat()
                                for i in range((date.fromisoformat(target) - date.fromisoformat(first)).days + 1)]
                    else:
                        calendar = self.store("primary").calendar(first, target)
                        days = sessions(calendar, first, target)
                    # Latest first, then old gaps. This does not discard holes
                    # merely because a later date already exists.
                    dataset_deadline = min(deadline, time.monotonic() + 300)
                    cutoff = refresh_cutoff(spec, self.clock())
                    completed, failed = 0, 0
                    stop_dataset = False
                    for day in reversed(days):
                        if time.monotonic() >= dataset_deadline or stop_dataset:
                            break
                        subset = catalog
                        if name == "capital_flow_daily":
                            relevant = {code: catalog[code] for code in eligible_codes(spec, catalog, day)}
                            dep = flow_dependency(relevant, day, self.store("history").states("stock_daily", day))
                            subset = {code: relevant[code] for code in dep["traded"]}
                            if dep["missing_daily"]:
                                self.errors.append({"dataset": name, "target_date": day, "error": "MISSING_DAILY_DEPENDENCY",
                                                    "missing": len(dep["missing_daily"])})
                        # Event scans include today plus three prior natural days.
                        refresh = due and day in days[-(4 if spec.event_data else 3):]
                        units = plan_units(spec, day, subset, states, now=self.clock(),
                                           refresh=refresh, refresh_after=cutoff)
                        size = 20 if spec.period == "1m" else 40
                        for adjustment in spec.adjustments:
                            selected = [u for u in units if u.adjustment == adjustment]
                            for offset in range(0, len(selected), size):
                                remaining = dataset_deadline - time.monotonic()
                                if remaining < 1 or stop_dataset:
                                    break
                                current_batch = selected[offset:offset + size]
                                outcome = self.acquire(current_batch, remaining)
                                completed += outcome.get("complete", 0) + outcome.get("no_data", 0)
                                failed += outcome.get("error", 0)
                                if set(outcome.get("error_codes", [])) & SOURCE_ERRORS:
                                    blocked_sources.add(source_group(name))
                                # A malformed security stays retryable without
                                # withholding all later securities. Stop only
                                # when the whole batch proves a product-wide
                                # contract/configuration failure.
                                product_failure = (outcome.get("error", 0) == len(current_batch)
                                                   and bool(set(outcome.get("error_codes", [])) & CONFIG_ERRORS))
                                if outcome.get("waiting") or set(outcome.get("error_codes", [])) & SOURCE_ERRORS or product_failure:
                                    stop_dataset = True
                                if outcome.get("error"):
                                    self.errors.append({"dataset": name, "target_date": day,
                                                        "error_codes": outcome.get("error_codes", []), "failed": outcome["error"]})
                    results[name] = {"completed_units": completed, "failed_units": failed,
                                     "status": "budget_exhausted" if time.monotonic() >= dataset_deadline else "attempted"}
                except Exception as exc:
                    self.errors.append({"dataset": name, "error": safe_error(exc)})
                    if spec.source == "guojin_qmt":
                        qmt_ready = False
            report = self.status([n for n in datasets if get_spec(n).period != "tick" and n != "reference"], end or requested)
            return {"status": "partial" if self.errors or report["status"] != "complete" else "complete",
                    "errors": self.errors, "runs": results, "latest": report}

    def reference(self, asset_class, codes, period="instrument", target=None):
        self.config.require_writes()
        target = target or self.clock().date().isoformat()
        if date.fromisoformat(target) > self.clock().date():
            raise ValueError("reference target cannot be in the future")
        spec = reference_spec(asset_class, period)
        if not codes or len(set(codes)) != len(codes):
            raise ValueError("reference scope must be explicit, unique and nonempty")
        units = [WorkUnit("reference", "guojin_qmt", target, code, period, "none") for code in codes]
        if any(len(u.partition_key) > 64 for u in units):
            raise ValueError("reference partition key exceeds existing progress column")
        results = []
        with process_lock(self.config.state_dir / "daily.lock"):
            if not self.recover_qmt():
                return {"status": "waiting_for_active_request"}
            for offset in range(0, len(units), 40):
                selected = units[offset:offset + 40]
                request = make_request(selected, self.clock())
                request["asset_class"] = asset_class
                result = self.acquire(selected, 180, request=request, spec=spec)
                results.append(result)
                if result.get("error_codes") or result.get("waiting"):
                    break
        return {"status": "partial" if any(r.get("error") or r.get("waiting") for r in results) else "complete", "batches": results}

    def status(self, datasets, requested="latest"):
        results = []
        for name in datasets:
            try:
                spec = get_spec(name)
                target = self._target(spec, requested)
                catalog = self.catalog(spec)
                result = summarize(spec, target, catalog, self.store(spec.database).states(name, target))
                if name == "capital_flow_daily":
                    relevant = {code: catalog[code] for code in eligible_codes(spec, catalog, target)}
                    dep = flow_dependency(relevant, target, self.store("history").states("stock_daily", target))
                    result = summarize(spec, target, {code: relevant[code] for code in dep["traded"]},
                                       self.store(spec.database).states(name, target))
                    result["missing_daily"] = len(dep["missing_daily"])
                    result["native_no_trade"] = len(dep["no_trade"])
                    result["unsupported_market"] = len(catalog) - len(relevant)
                    if dep["missing_daily"]:
                        result["status"] = "partial"
                    elif not dep["traded"] and dep["no_trade"]:
                        result["status"] = "complete"
                results.append(result)
            except Exception as exc:
                results.append({"dataset": name, "status": "unavailable", "error": safe_error(exc)})
        return {"status": "complete" if results and all(r["status"] == "complete" for r in results) else "partial",
                "checked_at": self.clock().isoformat(), "datasets": results}

    def live_once(self):
        self.config.require_writes()
        now = self.clock()
        if not (day_time(9, 15) <= now.time().replace(tzinfo=None) <= day_time(11, 31)
                or day_time(13) <= now.time().replace(tzinfo=None) <= day_time(15, 1)):
            return {"status": "outside_live_window"}
        if not self.store("primary").calendar(now.date(), now.date()).get(now.date().isoformat()):
            return {"status": "closed_or_calendar_missing"}
        from .qmt_model import publish_json
        transport = self._qmt_transport()
        with process_lock(self.config.state_dir / "live.lock"):
            plan = {}
            for name in ("stock_current", "index_current"):
                if name in self.config.data.get("datasets", []):
                    plan[name] = list(self.catalog(get_spec(name)))
            publish_json(str(Path(transport.root) / "live_plan.json"), plan, MAX_REQUEST_BYTES)
            results = {}
            for name in plan:
                pending = Path(transport.root) / (name + ".pending.json")
                raw = read_json(str(pending), MAX_RESULT_BYTES)
                recovering = raw is not None
                if raw is None:
                    raw = read_json(str(Path(transport.root) / (name + ".snapshot.json")), MAX_RESULT_BYTES)
                if raw is None:
                    results[name] = {"status": "waiting_for_snapshot"}
                    continue
                if not recovering and (now - _timestamp(raw.get("received_at"))).total_seconds() > 30:
                    results[name] = {"status": "stale_snapshot"}
                    continue
                spec = get_spec(name)
                if raw.get("request", {}).get("dataset") != name:
                    raise ValueError("live product identity differs")
                units = units_from_request(raw["request"])
                if not recovering:
                    publish_json(str(pending), raw, MAX_RESULT_BYTES, immutable=True)
                self.store(spec.database).begin_request(units, raw["request"]["request_id"], self.clock())
                results[name] = self._consume(raw)
                pending.unlink()  # Only this product's committed runtime snapshot.
            return results
