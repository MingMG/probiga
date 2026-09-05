"""Business rows and partition progress commit on the SAME database connection."""
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
import json
import re

from sqlalchemy import (Column, Date, DateTime, Index, Integer, MetaData, String, Table,
                        Text, UniqueConstraint, and_, inspect, select, text)

from .models import WorkUnit

metadata = MetaData()
STATE = Table(
    "acquisition_partition_state", metadata,
    Column("dataset", String(32), primary_key=True),
    Column("source", String(32), primary_key=True),
    Column("target_date", Date, primary_key=True),
    Column("partition_key", String(64), primary_key=True),
    Column("status", String(16), nullable=False, server_default="pending"),
    Column("request_id", String(64)), Column("written_rows", Integer, nullable=False, server_default="0"),
    Column("last_attempt_at", DateTime), Column("last_success_at", DateTime),
    Column("next_retry_at", DateTime), Column("last_error_code", String(64)),
    Column("last_error", String(512)), Column("detail_json", Text),
    Column("updated_at", DateTime, nullable=False),
    Index("idx_acquisition_due", "status", "next_retry_at"),
)


def local_time(value):
    from zoneinfo import ZoneInfo
    return (value.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
            if value.tzinfo else value)


def safe_error(exc):
    """SQL/HTTP exceptions can contain credentials, SQL values or response bodies."""
    code = getattr(exc, "code", None)
    if isinstance(code, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{1,63}", code):
        return code
    if isinstance(exc, (RuntimeError, ValueError)) and re.fullmatch(r"[A-Z][A-Z0-9_]{1,63}", str(exc)):
        return str(exc)
    return type(exc).__name__


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)


def _identity(unit):
    return dict(dataset=unit.dataset, source=unit.source,
                target_date=date.fromisoformat(unit.target_date), partition_key=unit.partition_key)


def _where(unit):
    return and_(*(STATE.c[key] == value for key, value in _identity(unit).items()))


class SchemaMismatch(RuntimeError):
    pass


class StaleRequest(RuntimeError):
    pass


class Store:
    def __init__(self, engine):
        self.engine = engine
        self._tables = {}
        self._validated = set()

    def prepare_progress_schema(self):
        """Explicit installation only. Normal acquisition never executes DDL."""
        metadata.create_all(self.engine, tables=[STATE])

    def table(self, name):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name):
            raise SchemaMismatch("invalid fixed table name")
        if name not in self._tables:
            self._tables[name] = Table(name, MetaData(), autoload_with=self.engine)
        return self._tables[name]

    def validate_spec(self, spec):
        validation_key = (spec.table, tuple(spec.key_columns))
        if validation_key in self._validated:
            return
        table = self.table(spec.table)
        wanted = set(spec.key_columns)
        if not wanted or not wanted.issubset(table.c.keys()):
            raise SchemaMismatch(f"{spec.table}: business identity columns are missing")
        reader = inspect(self.engine)
        unique_columns = [tuple(item["column_names"]) for item in reader.get_unique_constraints(spec.table)]
        indexes = [tuple(item["column_names"]) for item in reader.get_indexes(spec.table)]
        indexes += unique_columns
        unique_shapes = [set(columns) for columns in unique_columns]
        unique_shapes += [set(item["column_names"]) for item in reader.get_indexes(spec.table) if item.get("unique")]
        primary = tuple(reader.get_pk_constraint(spec.table).get("constrained_columns") or [])
        if primary:
            indexes.append(primary)
            unique_shapes.append(set(primary))
        # Existing large legacy tables may have a covering non-unique index.
        # The fixed single writer and indexed duplicate check below provide
        # idempotency without rebuilding a 97M-row table merely for admission.
        if not any(set(columns[:len(spec.key_columns)]) == wanted for columns in indexes):
            raise SchemaMismatch(f"{spec.table}: an indexed business identity is required")
        if any(shape and shape < wanted for shape in unique_shapes):
            raise SchemaMismatch(f"{spec.table}: a legacy UNIQUE key collapses distinct business rows")
        if spec.name == "finance":
            self.table("st_pit_finance_revision")
        self._validated.add(validation_key)

    @contextmanager
    def _transaction(self, dataset):
        # One writer per dataset is configured. GET_LOCK also excludes a manual
        # duplicate CLI on the same database; it is never held during fetching.
        with self.engine.connect() as conn:
            lock_name = "direct-acquisition:" + dataset
            locked = False
            try:
                if conn.dialect.name == "mysql":
                    locked = conn.execute(text("SELECT GET_LOCK(:name, 0)"), {"name": lock_name}).scalar() == 1
                    if not locked:
                        raise RuntimeError("dataset writer is already active")
                    conn.commit()
                with conn.begin():
                    yield conn
            finally:
                if locked:
                    conn.execute(text("SELECT RELEASE_LOCK(:name)"), {"name": lock_name})
                    conn.commit()

    def states(self, dataset, target_date=None):
        query = select(STATE).where(STATE.c.dataset == dataset)
        if target_date:
            query = query.where(STATE.c.target_date == date.fromisoformat(str(target_date)))
        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def retrying_sources(self, now):
        """Read only active source-level cooldowns, once per database per run."""
        query = select(STATE.c.dataset, STATE.c.source, STATE.c.next_retry_at).where(
            STATE.c.status == "error", STATE.c.next_retry_at > local_time(now),
            STATE.c.last_error_code.in_(("SOURCE_ACCESS_DENIED", "SOURCE_UNAVAILABLE",
                                       "INVALID_RETRY_AFTER", "NATIVE_CALL_FAILED"))).distinct()
        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def begin_request(self, units, request_id, now):
        if not units or len({u.dataset for u in units}) != 1:
            raise ValueError("one request needs a single nonempty dataset")
        now = local_time(now)
        with self._transaction(units[0].dataset) as conn:
            for unit in units:
                current = conn.execute(select(STATE).where(_where(unit)).with_for_update()).mappings().first()
                if current and current["request_id"] == request_id:
                    continue  # prepared/running/committed crash recovery is idempotent
                if current and current["status"] == "running":
                    raise StaleRequest("unfinished request must be recovered before a replacement")
                values = dict(status="running", request_id=request_id, last_attempt_at=now,
                              next_retry_at=None, last_error_code=None, last_error=None, updated_at=now)
                if current:
                    conn.execute(STATE.update().where(_where(unit)).values(**values))
                else:
                    conn.execute(STATE.insert().values(**_identity(unit), **values))

    def fail_request(self, units, request_id, code, now, retry_seconds=900):
        now = local_time(now)
        if not units:
            return
        with self._transaction(units[0].dataset) as conn:
            for unit in units:
                conn.execute(STATE.update().where(_where(unit), STATE.c.request_id == request_id,
                                                 STATE.c.status == "running").values(
                    status="error", last_error_code=str(code)[:64], last_error=str(code)[:512],
                    next_retry_at=now + timedelta(seconds=retry_seconds), updated_at=now))

    def commit(self, spec, batch):
        self.validate_spec(spec)
        reference_spec = None
        if spec.name == "reference" and spec.period == "instrument":
            reference_spec = replace(spec, table="qmt_instrument_detail", code_column="qmt_code",
                                     key_columns=("qmt_code",))
            self.validate_spec(reference_spec)
        now = local_time(batch.received_at)
        counts = {"complete": 0, "no_data": 0, "error": 0, "replayed": 0}
        with self._transaction(spec.name) as conn:
            for result in batch.units:
                unit = result.unit
                state = conn.execute(select(STATE).where(_where(unit)).with_for_update()).mappings().first()
                if not state or state["request_id"] != batch.request_id:
                    raise StaleRequest("late result does not own the partition")
                if state["status"] in {"complete", "no_data", "error"}:
                    counts["replayed"] += 1
                    continue
                if result.status not in counts or result.status == "replayed":
                    raise ValueError("unknown normalized outcome")
                if result.status == "complete" and not result.rows:
                    raise ValueError("complete outcome must contain business rows")
                if result.status != "complete" and result.rows:
                    raise ValueError("non-complete outcome cannot carry business rows")
                if result.status == "complete":
                    for row in result.rows:
                        if reference_spec is not None:
                            native = result.detail.get("instrument_raw") or {}
                            instrument = dict(qmt_code=unit.code, stock_code=unit.code.split(".")[0],
                                exchange=row.get("exchange"), list_date=row.get("list_date"),
                                expire_date=row.get("expire_date", row.get("last_trade_date")),
                                short_name=row.get("short_name", row.get("name", native.get("InstrumentName"))),
                                instrument_type=spec.asset_class.upper())
                            self._upsert_row(conn, reference_spec, instrument, unit, batch.request_id, now)
                        if spec.name == "finance":
                            promotion = self._append_finance_revision(conn, row, batch.request_id, now)
                            if not promotion:
                                result.detail["cache_not_promoted"] = True
                                continue
                            if promotion == "baseline":
                                result.detail["cache_baseline_established"] = True
                                result.detail["prior_cache_version_unverified"] = True
                        self._upsert_row(conn, spec, row, unit, batch.request_id, now)
                conn.execute(STATE.update().where(_where(unit)).values(
                    status=result.status, written_rows=len(result.rows), updated_at=now,
                    last_success_at=now if result.status in {"complete", "no_data"} else state["last_success_at"],
                    next_retry_at=now + timedelta(minutes=15) if result.status == "error" else None,
                    last_error_code=result.error_code[:64] or None,
                    last_error=result.error[:512] or None, detail_json=_json(result.detail)))
                counts[result.status] += 1
        return counts

    @staticmethod
    def _typed_values(table, values):
        """Use reflected SQL types, rather than relying on MySQL string casts."""
        converted = dict(values)
        for key, value in converted.items():
            if value is None or key not in table.c:
                continue
            column_type = table.c[key].type
            if isinstance(column_type, DateTime):
                if isinstance(value, datetime):
                    converted[key] = local_time(value)
                elif isinstance(value, str) and re.search(r"[T ]\d{2}:\d{2}", value):
                    converted[key] = local_time(datetime.fromisoformat(value.replace("Z", "+00:00")))
                else:
                    raise SchemaMismatch(f"{table.name}.{key}: native datetime is required, not a date-only value")
            elif isinstance(column_type, Date):
                if isinstance(value, datetime):
                    converted[key] = local_time(value).date()
                elif isinstance(value, date):
                    converted[key] = value
                elif isinstance(value, str):
                    converted[key] = (local_time(datetime.fromisoformat(value.replace("Z", "+00:00"))).date()
                                      if re.search(r"[T ]\d{2}:\d{2}", value) else date.fromisoformat(value))
                else:
                    raise SchemaMismatch(f"{table.name}.{key}: native date is required")
        return converted

    def _upsert_row(self, conn, spec, row, unit, request_id, now):
        table = self.table(spec.table)
        values = dict(row)
        defaults = dict(data_source=spec.persisted_source or spec.source, qmt_code=unit.code,
                        received_at=now, etl_sync_at=now, batch_id=request_id,
                        source_time=row.get("source_time", row.get("trade_time")))
        for key, value in defaults.items():
            if key in table.c and key not in values:
                values[key] = value
        if "association_validated" in table.c and spec.name == "notices":
            values["association_validated"] = 1  # normalizer proved source association
        if "data_version" in table.c and "data_version" not in values:
            values["data_version"] = hashlib.sha256(_json(row).encode()).hexdigest()
        # Source-only diagnostics remain in raw files/revision payload, not in
        # arbitrary columns selected from an HTTP response.
        values = {key: value for key, value in values.items() if key in table.c}
        values = self._typed_values(table, values)
        if any(values.get(key) is None for key in spec.key_columns):
            raise SchemaMismatch(f"{spec.table}: a business key has no value")
        predicate = and_(*(table.c[key] == values[key] for key in spec.key_columns))
        matches = conn.execute(select(table).where(predicate).limit(2).with_for_update()).mappings().all()
        if len(matches) > 1:
            raise SchemaMismatch(f"{spec.table}: duplicate rows exist for the touched business identity")
        previous = matches[0] if matches else None
        required = [c.name for c in table.c if not c.nullable and c.server_default is None
                    and not (c.primary_key and isinstance(c.type, Integer) and c.autoincrement in (True, "auto"))]
        if previous:
            for key in required:
                if values.get(key) is None and previous.get(key) is not None:
                    values.pop(key, None)  # Preserve known required metadata on an update.
        effective = {**dict(previous or {}), **values}
        if any(effective.get(key) is None for key in required):
            raise SchemaMismatch(f"{spec.table}: a required column has no honest value")
        if previous and spec.period == "tick":
            for timestamp in ("source_time", "trade_time", "snapshot_at"):
                old, new = previous.get(timestamp), values.get(timestamp)
                if old is not None and new is not None:
                    if new < old:
                        return
                    break
        if previous:
            updates = {key: value for key, value in values.items() if key not in spec.key_columns and key != "id"}
            if updates:
                conn.execute(table.update().where(predicate).values(**updates))
        else:
            conn.execute(table.insert().values(**values))

    def _append_finance_revision(self, conn, row, request_id, now):
        """Append facts, never rewrite publication/knowledge history or issue a fake seal."""
        table = self.table("st_pit_finance_revision")
        source = "eastmoney.finance.mainfinadata.direct"
        identity_fields = self._typed_values(table, dict(stock_code=row["stock_code"], report_date=row["report_date"],
                               report_type=row["report_type"], source=source))
        predicate = and_(*(table.c[k] == v for k, v in identity_fields.items()))
        previous = conn.execute(select(table).where(predicate).order_by(table.c.revision_no.desc()).with_for_update()).mappings().all()
        payload = {k: v for k, v in row.items() if k not in {"received_at", "etl_sync_at", "batch_id"}}
        encoded = _json(payload)
        fingerprint = hashlib.sha256(encoded.encode()).hexdigest()
        if any(item["revision_fingerprint_hash"] == fingerprint for item in previous):
            return False  # Replayed historical facts must never roll back the display cache.
        cache_table = self.table("si_stock_finance")
        cache_keys = self._typed_values(cache_table, {key: row[key] for key in ("stock_code", "report_date")})
        cache = conn.execute(select(cache_table).where(and_(
            *(cache_table.c[key] == value for key, value in cache_keys.items()))).with_for_update()).mappings().first()
        # Compare the version that actually owns the display row. A newly
        # appended old fact is not evidence of the cache's current version.
        # A legacy row without a source version gets one explicit fresh-source
        # baseline; we cannot claim to prove that unversioned old row's order.
        baseline = cache is not None and cache.get("source_update_date") is None
        promote = cache is None or baseline or self._newer_source_update(
            row.get("source_update_date"), cache.get("source_update_date"))
        identity_hash = previous[0]["identity_hash"] if previous else hashlib.sha256(_json(identity_fields).encode()).hexdigest()
        revision_no = previous[0]["revision_no"] + 1 if previous else 1
        # A calendar date (including a provider's midnight string) does not
        # prove an exact publication timestamp. Preserve it as unverified.
        publication_text = str(row.get("notice_date") or "")
        conn.execute(table.insert().values(
            revision_id=hashlib.sha256((identity_hash + ":" + fingerprint).encode()).hexdigest(),
            identity_hash=identity_hash, **identity_fields,
            published_at=None, source_published_text=publication_text,
            publication_time_status="TIME_UNVERIFIED", known_at=now, received_at=now,
            revision_no=revision_no, supersedes_revision_id=previous[0]["revision_id"] if previous and promote else None,
            batch_id=request_id, content_hash=fingerprint, revision_fingerprint_hash=fingerprint,
            payload_json=encoded, created_at=now))
        return "baseline" if baseline else promote

    @staticmethod
    def _newer_source_update(new, old):
        if new is None or old is None:
            return False
        try:
            newer = local_time(datetime.fromisoformat(str(new).replace("Z", "+00:00")))
            older = local_time(datetime.fromisoformat(str(old).replace("Z", "+00:00")))
        except (ValueError, TypeError):
            return False
        if newer.date() != older.date():
            return newer.date() > older.date()
        # Date-only or minute-only precision cannot order all changes within
        # that same day. Keep the revision without guessing a cache overwrite.
        precise = lambda value: isinstance(value, datetime) or bool(re.search(r"[T ]\d{2}:\d{2}:\d{2}", str(value)))
        return precise(new) and precise(old) and newer > older

    def catalog(self, asset_class):
        table_name, column = {"stock": ("si_all_code", "stock_code"),
                              "index": ("si_all_index_code", "index_code"),
                              "etf": ("si_etf_code", "etf_code")}[asset_class]
        table = self.table(table_name)
        with self.engine.connect() as conn:
            rows = conn.execute(select(table)).mappings().all()
        if asset_class == "index":
            rows = [row for row in rows if not str(row[column]).zfill(6).startswith("395")]
        instrument_symbols = {}
        if asset_class == "index" and any(not record.get("qmt_code") and not record.get("exchange") and not record.get("market") for record in rows):
            details = self.table("qmt_instrument_detail")
            index_names = {str(row[column]).zfill(6): str(row.get("name") or "").strip() for row in rows}
            with self.engine.connect() as conn:
                for record in conn.execute(select(details)).mappings():
                    code, symbol = str(record["stock_code"]), str(record["qmt_code"])
                    kind = str(record.get("asset_class") or record.get("instrument_type") or "").upper()
                    is_index = kind == "INDEX"
                    if not is_index and kind not in {"STOCK", "ETF", "FUND", "BOND", "FUTURE", "OPTION"}:
                        # Legacy native ProductType values can be opaque. An
                        # exact directory/detail name match is explicit metadata,
                        # not a guessed exchange from the six-digit prefix.
                        is_index = bool(index_names.get(code)) and str(record.get("short_name") or "").strip() == index_names[code]
                    if is_index and re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", symbol) and symbol.split(".")[0] == code:
                        instrument_symbols.setdefault(code, set()).add(symbol)
        result = {}
        for record in rows:
            row = dict(record)
            symbol = str(row.get("qmt_code") or "").upper()
            code = str(row[column]).zfill(6)
            if not symbol:
                exchange = str(row.get("exchange") or row.get("market") or "").upper()
                exchange = {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}.get(exchange, exchange)
                if exchange not in {"SH", "SZ", "BJ"} and asset_class == "index":
                    matches = instrument_symbols.get(code, set())
                    if len(matches) != 1:
                        raise SchemaMismatch(f"{table_name}: instrument mapping is missing or ambiguous")
                    symbol = next(iter(matches))
                elif exchange not in {"SH", "SZ", "BJ"}:
                    raise SchemaMismatch(f"{table_name}: explicit security exchange is required")
                else:
                    symbol = code + "." + exchange
            if not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", symbol):
                raise SchemaMismatch("catalog contains an invalid qualified symbol")
            row["qmt_code"] = symbol
            result[symbol] = row
        if not result:
            raise SchemaMismatch("independent catalog is empty")
        return result

    def calendar(self, start, end):
        table = self.table("si_trade_calendar")
        with self.engine.connect() as conn:
            rows = conn.execute(select(table.c.trade_date, table.c.trade_status).where(
                table.c.trade_date >= start, table.c.trade_date <= end)).all()
        result = {}
        for day, status in rows:
            key = str(day)[:10]
            if key in result and result[key] != int(status):
                raise SchemaMismatch("conflicting calendar rows")
            result[key] = int(status)
        return result
