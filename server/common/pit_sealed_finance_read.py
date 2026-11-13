"""Materialize formal scoring facts from an already verified finance seal."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
import json
from time import monotonic
from typing import TYPE_CHECKING, Any, Callable, Iterable

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from server.common.pit_facts import PITFactBatch, SealedFinanceInputs


def load_sealed_finance_facts(
    engine: Engine,
    *,
    selection: SealedFinanceInputs,
    codes: Iterable[str],
    decision_at: datetime | str,
    fact_cutoff_at: datetime | str | None,
    as_of_date: date | str,
    read_progress: Callable[[int, int], None] | None = None,
) -> PITFactBatch:
    from server.common import pit_facts as pit
    from server.common.qmt_announcement_pit import (
        _live_announcement_read_hint,
        _live_announcement_reply_budget,
    )

    fact_cutoff, decision = pit._fact_and_decision_times(
        decision_at=decision_at, fact_cutoff_at=fact_cutoff_at,
    )
    target = pit._date_value(as_of_date, required=True)
    requested = sorted({str(code).strip().zfill(6) for code in codes if str(code).strip()})
    if (
        selection.engine is not engine
        or selection.decision_at != decision
        or selection.fact_cutoff_at != fact_cutoff
        or selection.as_of_date != target
        or not set(requested) <= selection.members.keys()
        or not pit._HASH_RE.fullmatch(selection.seal_id)
        or not pit._HASH_RE.fullmatch(selection.batch_root_sha256)
    ):
        raise ValueError("PIT_SEALED_FINANCE_SELECTION_DIFFERS")

    facts: dict[str, Any] = {}
    coverage: dict[str, Any] = {}
    statuses: dict[str, str] = {}
    reasons: dict[str, str] = {}
    deadline = monotonic() + pit.FINANCE_REVISION_MYSQL_DEADLINE_SECONDS

    def read_rows(connection, columns, table, where, parameters, *, stage, index=""):
        mysql = connection.dialect.name == "mysql"
        hint = _live_announcement_read_hint(mysql=mysql, deadline=deadline, stage=stage)
        index_hint = f" FORCE INDEX ({index})" if mysql and index else ""
        statement = text(f"SELECT {hint}{columns} FROM {table}{index_hint} WHERE {where}").bindparams(
            bindparam("keys", expanding=True),
        )
        try:
            with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
                with connection.execute(statement.execution_options(stream_results=True), parameters) as result:
                    return [dict(row) for row in result.mappings()]
        except Exception as exc:
            raise RuntimeError(f"PIT_SEALED_FINANCE_READ_FAILED:{stage}:{type(exc).__name__}") from exc

    def read_payloads(connection, *, table, id_column, keys, stage):
        if not keys:
            return {}
        mysql = connection.dialect.name == "mysql"
        scope = f"{id_column} IN :keys AND known_at<=:cutoff AND received_at<=:cutoff"
        params = {"keys": sorted(keys), "cutoff": fact_cutoff}
        if mysql:
            hint = _live_announcement_read_hint(mysql=True, deadline=deadline, stage=stage + "-inventory")
            statement = text(f"SELECT {hint}{id_column} FROM {table} FORCE INDEX (PRIMARY) WHERE {scope}")
            statement = statement.bindparams(bindparam("keys", expanding=True))
            try:
                packed = pit._read_pit_payload_rows(
                    connection, table_name=table, statement=statement, parameters=params,
                    deadline=deadline, stage_prefix=stage,
                )
                rows = [pit._coverage_payload_row(row, mysql=True, stage=stage + "-decode") for row in packed]
            except Exception as exc:
                failed_stage = getattr(exc, "_pit_read_stage", stage)
                raise RuntimeError(f"PIT_SEALED_FINANCE_READ_FAILED:{failed_stage}:{type(exc).__name__}") from exc
        else:
            rows = read_rows(connection, "*", table, scope, params, stage=stage)
        observed = {row[id_column]: row for row in rows}
        if len(rows) != len(keys) or observed.keys() != keys:
            raise ValueError(f"PIT_SEALED_FINANCE_MEMBERSHIP_DIFFERS:{stage}")
        return observed

    def load_chunk(connection, chunk):
        members = {code: selection.members[code] for code in chunk if code not in selection.exclusions}
        by_coverage_id = {str(member["coverage_id"]): code for code, member in members.items()}
        if len(by_coverage_id) != len(members) or any(not pit._HASH_RE.fullmatch(key) for key in by_coverage_id):
            raise ValueError("PIT_SEALED_FINANCE_COVERAGE_IDS_DIFFER")
        special_ids = {
            coverage_id for coverage_id, code in by_coverage_id.items()
            if members[code]["coverage_status"] != "COMPLETE" or members[code].get("legal_no_data_binding")
        }
        coverage_rows = read_payloads(
            connection, table=pit.SOURCE_COVERAGE_TABLE, id_column="coverage_id",
            keys=special_ids, stage="finance-sealed-special-coverage",
        )
        ordinary_ids = sorted(by_coverage_id.keys() - special_ids)
        for offset in range(0, len(ordinary_ids), 15):
            ids = ordinary_ids[offset:offset + 15]
            rows = read_rows(
                connection,
                "coverage_id,scope_hash,stock_code,fact_kind,source,coverage_status,result_count,"
                "known_at,received_at,covered_through_at,source_response_hash,fact_set_hash,"
                "watermark_hash,revision_no,supersedes_coverage_id,coverage_fingerprint_hash,"
                "JSON_EXTRACT(payload_json, '$.fact_bindings') AS bindings",
                pit.SOURCE_COVERAGE_TABLE,
                "coverage_id IN :keys AND known_at<=:cutoff AND received_at<=:cutoff",
                {"keys": ids, "cutoff": fact_cutoff},
                stage=f"finance-sealed-coverage-bindings:{offset}", index="PRIMARY",
            )
            if len(rows) != len(ids) or {row["coverage_id"] for row in rows} != set(ids):
                raise ValueError("PIT_SEALED_FINANCE_COVERAGE_MEMBERSHIP_DIFFERS")
            coverage_rows.update({row["coverage_id"]: row for row in rows})
        predecessor_ids = {
            str(row["supersedes_coverage_id"]) for row in coverage_rows.values()
            if row["supersedes_coverage_id"]
        }
        predecessors = read_rows(
            connection,
            "coverage_id,scope_hash,revision_no,known_at,received_at,covered_through_at,"
            "coverage_status,coverage_fingerprint_hash",
            pit.SOURCE_COVERAGE_TABLE, "coverage_id IN :keys",
            {"keys": sorted(predecessor_ids)}, stage="finance-sealed-coverage-links", index="PRIMARY",
        ) if predecessor_ids else []
        predecessor_by_id = {row["coverage_id"]: row for row in predecessors}
        bound_reports: dict[str, tuple[str, str]] = {}
        for coverage_id, row in coverage_rows.items():
            code = by_coverage_id[coverage_id]
            member = members[code]
            if row["fact_kind"] != "finance" or row["stock_code"] != code or any(
                row[field] != member[field] for field in (
                    "scope_hash", "coverage_status", "source", "source_response_hash", "fact_set_hash", "watermark_hash",
                )
            ) or any(
                pit._dt_text(pit._row_datetime(row[field])) != member[field]
                for field in ("known_at", "covered_through_at")
            ):
                raise ValueError(f"PIT_SEALED_FINANCE_COVERAGE_DIFFERS:{code}")

            previous_id = row["supersedes_coverage_id"]
            parent = predecessor_by_id.get(previous_id)
            revision_no = int(row["revision_no"])
            known = pit._row_datetime(row["known_at"])
            if (
                revision_no < 1
                or (revision_no == 1 and previous_id is not None)
                or pit._row_datetime(row["received_at"]) > known
                or pit._row_datetime(row["covered_through_at"]) > known
                or not pit._HASH_RE.fullmatch(str(row["coverage_fingerprint_hash"]))
                or coverage_id != pit.canonical_hash({
                    "schema": "probiga.pit-source-coverage-id.v1", "scope_hash": row["scope_hash"],
                    "coverage_fingerprint_hash": row["coverage_fingerprint_hash"],
                })
            ):
                raise ValueError(f"PIT_SEALED_FINANCE_COVERAGE_LINK_DIFFERS:{code}")
            if revision_no > 1 and (
                not parent
                or parent["scope_hash"] != row["scope_hash"]
                or int(parent["revision_no"]) != revision_no - 1
                or parent["coverage_status"] not in {"COMPLETE", pit.FINANCE_EXPECTED_UNAVAILABLE_STATUS}
                or not pit._HASH_RE.fullmatch(str(parent["coverage_fingerprint_hash"]))
                or pit._row_datetime(parent["known_at"]) > known
                or pit._row_datetime(parent["received_at"]) > pit._row_datetime(parent["known_at"])
                or pit._row_datetime(parent["covered_through_at"]) > pit._row_datetime(parent["known_at"])
                or previous_id != pit.canonical_hash({
                    "schema": "probiga.pit-source-coverage-id.v1", "scope_hash": parent["scope_hash"],
                    "coverage_fingerprint_hash": parent["coverage_fingerprint_hash"],
                })
            ):
                raise ValueError(f"PIT_SEALED_FINANCE_COVERAGE_LINK_DIFFERS:{code}")
            statuses[code] = pit.PIT_AVAILABLE
            if coverage_id not in special_ids:
                if row["source"] not in pit.AUTHORITATIVE_FINANCE_SOURCES:
                    raise ValueError(f"PIT_SEALED_FINANCE_COVERAGE_DISPOSITION_DIFFERS:{code}")
                bindings = json.loads(row["bindings"] or "null")
                if (
                    not isinstance(bindings, list)
                    or not bindings
                    or len(bindings) != int(row["result_count"])
                    or any(not isinstance(item, dict) or set(item) != {"revision_id", "content_hash"}
                           or any(not isinstance(value, str) or not pit._HASH_RE.fullmatch(value)
                                  for value in item.values()) for item in bindings)
                    or pit.canonical_hash({"schema": "probiga.pit-finance-fact-set.v1", "bindings": bindings})
                    != member["fact_set_hash"]
                ):
                    raise ValueError(f"PIT_SEALED_FINANCE_FACT_SET_DIFFERS:{code}")
                for binding in bindings:
                    revision_id = binding["revision_id"]
                    if revision_id in bound_reports:
                        raise ValueError(f"PIT_SEALED_FINANCE_REVISION_BINDING_DIFFERS:{code}")
                    bound_reports[revision_id] = (code, binding["content_hash"])
                continue
            pit._validate_coverage_chain(
                [row], first_revision=revision_no,
                previous_id=parent["coverage_id"] if parent else None,
                previous_known=pit._row_datetime(parent["known_at"]) if parent else None,
            )
            payload = pit._parse_payload(row)
            receipt = pit._empty_coverage_receipt(row, fact_cutoff_at=fact_cutoff, decision_at=decision)
            if row["coverage_status"] == pit.FINANCE_EXPECTED_UNAVAILABLE_STATUS:
                evidence = payload["official_evidence"]
                if (
                    payload["expected_report_date"] != pit.finance_disclosure_gate(target).minimum_report_date.isoformat()
                    or not date.fromisoformat(evidence["valid_from"]) <= decision.date() <= date.fromisoformat(evidence["valid_until"])
                ):
                    raise ValueError(f"PIT_SEALED_FINANCE_NONFILING_EXPIRED:{code}")
                coverage[code] = {
                    **receipt,
                    **{key: evidence[key] for key in (
                        "valid_from", "valid_until", "next_retry_date", "announcement_id",
                        "announcement_url", "announcement_document_sha256",
                    )},
                    "coverage_status": row["coverage_status"], "reason_code": payload["reason_code"],
                    "source": row["source"], "known_at": receipt["coverage_known_at"],
                    "disposition_id": coverage_id, "source_response_hash": row["source_response_hash"],
                    "watermark_hash": row["watermark_hash"],
                }
                reasons[code] = "PIT_FINANCE_EXPECTED_UNAVAILABLE"
                continue
            source_rows = payload["source_rows"]
            if not source_rows and member.get("legal_no_data_binding"):
                coverage[code] = {**receipt, **member["legal_no_data_binding"]}
                reasons[code] = "PIT_FINANCE_AUTHORITATIVE_EMPTY"
                continue
            raise ValueError(f"PIT_SEALED_FINANCE_COVERAGE_DISPOSITION_DIFFERS:{code}")

        if not bound_reports:
            return
        report_dates = {}
        codes_by_report = defaultdict(list)
        for code in sorted({code for code, _ in bound_reports.values()}):
            latest = pit._date_value(members[code]["strategy_prefix_binding"].get("latest_report_date"), required=True)
            if latest > target:
                raise ValueError(f"PIT_SEALED_FINANCE_REPORT_SCOPE_DIFFERS:{code}")
            report_dates[code] = latest
            codes_by_report[latest].append(code)
        # This index starts with the exact issuer/report pair. The same narrow
        # inventory supplies predecessor IDs for each chosen report identity.
        chain_inventory = []
        metadata_limit = pit.FINANCE_REVISION_MYSQL_CODE_LIMIT
        for report_date, report_codes in sorted(codes_by_report.items()):
            for offset in range(0, len(report_codes), metadata_limit):
                codes_for_query = report_codes[offset:offset + metadata_limit]
                metadata = read_rows(
                    connection, "revision_id,identity_hash,stock_code,report_date,report_type,content_hash,revision_no",
                    pit.FINANCE_REVISION_TABLE,
                    "stock_code IN :keys AND report_date=:report AND known_at<=:cutoff AND received_at<=:cutoff",
                    {"keys": codes_for_query, "report": report_date, "cutoff": fact_cutoff},
                    stage=f"finance-sealed-report-metadata:{report_date}:{offset}", index="idx_pit_finance_decision",
                )
                if any(
                    row["stock_code"] not in codes_for_query
                    or pit._date_value(row["report_date"], required=True) != report_date
                    for row in metadata
                ):
                    raise ValueError("PIT_SEALED_FINANCE_REPORT_MEMBERSHIP_DIFFERS")
                chain_inventory.extend(metadata)
        if len(chain_inventory) != len({row["revision_id"] for row in chain_inventory}):
            raise ValueError("PIT_SEALED_FINANCE_REPORT_MEMBERSHIP_DIFFERS")
        selected_bindings: dict[str, tuple[str, str, str]] = {}
        bound_revision_numbers = {}
        for row in chain_inventory:
            revision_id = row["revision_id"]
            if revision_id not in bound_reports:
                continue
            code, content = bound_reports[revision_id]
            report_date = pit._date_value(row["report_date"], required=True)
            identity = pit.canonical_hash({
                "schema": "probiga.pit-finance-identity.v1", "stock_code": code,
                "report_date": report_date.isoformat(), "report_type": row["report_type"],
            })
            if (
                (row["stock_code"], row["content_hash"], row["identity_hash"]) != (code, content, identity)
                or report_date != report_dates[code]
                or identity in selected_bindings
            ):
                raise ValueError(f"PIT_SEALED_FINANCE_REPORT_BINDING_DIFFERS:{code}")
            selected_bindings[identity] = (code, revision_id, content)
            bound_revision_numbers[identity] = int(row["revision_no"])
        if {code for code, _, _ in selected_bindings.values()} != report_dates.keys():
            raise ValueError("PIT_SEALED_FINANCE_BOUND_REVISION_MISSING")
        revision_ids = {
            row["revision_id"] for row in chain_inventory
            if row["identity_hash"] in bound_revision_numbers
            and int(row["revision_no"]) <= bound_revision_numbers[row["identity_hash"]]
        }
        revision_rows = read_payloads(
            connection, table=pit.FINANCE_REVISION_TABLE, id_column="revision_id",
            keys=revision_ids, stage="finance-sealed-revision",
        )
        chains = defaultdict(list)
        for row in revision_rows.values():
            chains[row["identity_hash"]].append(row)
        candidates = defaultdict(list)
        date_only_reports = {}
        for identity, (code, revision_id, content) in selected_bindings.items():
            chain = sorted(chains[identity], key=lambda row: int(row["revision_no"]))
            selected, reason = pit._latest_revision_for_fact_cutoff(
                chain, fact_kind="finance", fact_cutoff_at=fact_cutoff, decision_at=decision,
            )
            bound = chain[-1]
            if bound["stock_code"] != code or bound["revision_id"] != revision_id or bound["content_hash"] != content:
                raise ValueError(f"PIT_SEALED_FINANCE_REVISION_BINDING_DIFFERS:{code}")
            if reason == "PIT_FINANCE_PUBLICATION_DATE_MISSING" and (
                bound["published_at"] is None and not bound["source_published_text"]
            ):
                # The immutable payload, validated by the revision chain above,
                # retains the vendor date when nullable enrichment masked it.
                raw = pit._parse_payload(bound)
                notice_date = pit._date_value(raw.get("notice_date"), required=False)
                update_date = pit._date_value(raw.get("source_update_date"), required=False)
                if (
                    raw.get("published_at") in (None, "")
                    and notice_date is not None and notice_date <= target
                    and datetime.combine(notice_date, datetime.max.time()) <= fact_cutoff
                    and (update_date is None or update_date <= target)
                    and pit._row_datetime(bound["known_at"]) <= fact_cutoff
                ):
                    selected = bound
                    date_only_reports[revision_id] = notice_date.isoformat()
            if not selected or selected["revision_id"] != revision_id:
                raise ValueError(f"PIT_SEALED_FINANCE_REVISION_DIFFERS:{code}:{reason}")
            candidates[code].append(selected)

        late_date_only_ids = {
            members[code]["coverage_id"] for code, rows in candidates.items()
            if pit._row_datetime(members[code]["known_at"]).date() > target
            and any(row["revision_id"] in date_only_reports for row in rows)
        }
        late_ids = sorted(late_date_only_ids)
        for offset in range(0, len(late_ids), 15):
            ids = late_ids[offset:offset + 15]
            rows = read_rows(
                connection, "coverage_id,JSON_EXTRACT(payload_json, '$.watermark') AS watermark",
                pit.SOURCE_COVERAGE_TABLE, "coverage_id IN :keys",
                {"keys": ids}, stage="finance-sealed-date-only-watermarks", index="PRIMARY",
            )
            if len(rows) != len(ids) or {row["coverage_id"] for row in rows} != set(ids):
                raise ValueError("PIT_SEALED_FINANCE_WATERMARK_MEMBERSHIP_DIFFERS")
            for row in rows:
                code = by_coverage_id[row["coverage_id"]]
                watermark = json.loads(row["watermark"] or "null")
                if not isinstance(watermark, dict) or pit.canonical_hash(watermark) != members[code]["watermark_hash"]:
                    raise ValueError(f"PIT_SEALED_FINANCE_WATERMARK_DIFFERS:{code}")
                guard = watermark["evidence"].get("source_timestamp_guard")
                if not isinstance(guard, dict) or guard.get("status") != "PASS" or guard.get("as_of_date") != target.isoformat():
                    raise ValueError(f"PIT_SEALED_FINANCE_DATE_ONLY_TARGET_GUARD_DIFFERS:{code}")

        for code, rows in candidates.items():
            selected = max(rows, key=lambda row: (
                pit._row_datetime(row["published_at"] or row["known_at"]),
                pit._row_datetime(row["known_at"]), int(row["revision_no"]), row["identity_hash"],
            ))
            fact = pit._finance_feature_fact(selected, fact_cutoff=fact_cutoff, decision=decision)
            source_date = date_only_reports.get(selected["revision_id"])
            if source_date:
                fact.update(finance_published_at=source_date, finance_notice_date=source_date)
            facts[code] = fact
            reasons[code] = ""

    for code in requested:
        if code in selection.exclusions:
            exclusion = dict(selection.exclusions[code])
            statuses[code] = pit.PIT_DATA_BLOCKED
            reasons[code] = "PIT_FINANCE_DATA_EXCLUDED:" + exclusion["reason_code"]
            coverage[code] = exclusion

    if any(code not in statuses for code in requested):
        with engine.connect() as connection:
            with _live_announcement_reply_budget(connection, deadline=deadline, stage="finance-sealed-isolation"):
                if connection.dialect.name == "mysql" and str(connection.get_isolation_level()).upper() not in {
                    "REPEATABLE READ", "SERIALIZABLE",
                }:
                    raise ValueError("PIT_SEALED_FINANCE_REQUIRES_REPEATABLE_READ")
            batch_size = pit.FINANCE_ATOMIC_BATCH_QUERY_CODE_LIMIT
            with connection.begin():
                for offset in range(0, len(requested), batch_size):
                    chunk = requested[offset:offset + batch_size]
                    load_chunk(connection, chunk)
                    if read_progress is not None:
                        read_progress(offset + len(chunk), len(requested))

    manifest = {
        "schema": "probiga.pit-finance-selection.v1", "fact_cutoff_at": pit._dt_text(fact_cutoff),
        "decision_at": pit._dt_text(decision), "as_of_date": target.isoformat(),
        "seal_coverage_id": selection.seal_id, "batch_root_sha256": selection.batch_root_sha256,
        "facts": {code: {"status": statuses[code], "reason": reasons[code], "coverage": coverage.get(code),
                         "revision_id": facts.get(code, {}).get("finance_revision_id"),
                         "content_hash": facts.get(code, {}).get("finance_content_hash")} for code in requested},
    }
    return pit.PITFactBatch(
        facts=facts, coverage_by_code=coverage, status_by_code=statuses, reason_by_code=reasons,
        manifest_hash=pit.canonical_hash(manifest), decision_at=pit._dt_text(decision),
        fact_cutoff_at=pit._dt_text(fact_cutoff), table_name=pit.FINANCE_REVISION_TABLE,
        global_status=pit.PIT_AVAILABLE,
    )
