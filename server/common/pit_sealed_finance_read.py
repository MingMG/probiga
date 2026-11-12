"""Materialize formal scoring facts from an already verified finance seal."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
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
        coverage_rows = read_payloads(
            connection, table=pit.SOURCE_COVERAGE_TABLE, id_column="coverage_id",
            keys=set(by_coverage_id), stage="finance-sealed-coverage",
        )
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
        bindings: dict[str, tuple[str, str, str]] = {}
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
            if int(row["revision_no"]) > 1 and (
                not parent
                or parent["scope_hash"] != row["scope_hash"]
                or int(parent["revision_no"]) != int(row["revision_no"]) - 1
                or parent["coverage_status"] not in {"COMPLETE", pit.FINANCE_EXPECTED_UNAVAILABLE_STATUS}
                or not pit._HASH_RE.fullmatch(str(parent["coverage_fingerprint_hash"]))
                or pit._row_datetime(parent["received_at"]) > pit._row_datetime(parent["known_at"])
                or pit._row_datetime(parent["covered_through_at"]) > pit._row_datetime(parent["known_at"])
                or previous_id != pit.canonical_hash({
                    "schema": "probiga.pit-source-coverage-id.v1", "scope_hash": parent["scope_hash"],
                    "coverage_fingerprint_hash": parent["coverage_fingerprint_hash"],
                })
            ):
                raise ValueError(f"PIT_SEALED_FINANCE_COVERAGE_LINK_DIFFERS:{code}")
            pit._validate_coverage_chain(
                [row], first_revision=int(row["revision_no"]),
                previous_id=parent["coverage_id"] if parent else None,
                previous_known=pit._row_datetime(parent["known_at"]) if parent else None,
            )
            payload = pit._parse_payload(row)
            receipt = pit._empty_coverage_receipt(row, fact_cutoff_at=fact_cutoff, decision_at=decision)
            statuses[code] = pit.PIT_AVAILABLE
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
            if row["source"] not in pit.AUTHORITATIVE_FINANCE_SOURCES or not source_rows:
                raise ValueError(f"PIT_SEALED_FINANCE_COVERAGE_DISPOSITION_DIFFERS:{code}")
            _, prefix = pit._finance_strategy_prefix_binding(source_rows)
            if prefix != member["strategy_prefix_binding"]:
                raise ValueError(f"PIT_SEALED_FINANCE_PREFIX_DIFFERS:{code}")
            reports = [
                (pit._date_value(source["report_date"], required=True), source)
                for source in source_rows
            ]
            eligible = [(report, source) for report, source in reports if report <= target]
            if not eligible or any(str(source["stock_code"]).zfill(6) != code for source in source_rows):
                raise ValueError(f"PIT_SEALED_FINANCE_REPORT_SCOPE_DIFFERS:{code}")
            latest = max(report for report, _ in eligible)
            binding_by_content = defaultdict(list)
            for binding in payload["fact_bindings"]:
                binding_by_content[binding["content_hash"]].append(binding["revision_id"])
            for report, source in eligible:
                if report != latest:
                    continue
                content = pit.canonical_hash({"schema": "probiga.pit-finance-payload.v1", "payload": source})
                revision_ids = binding_by_content[content]
                identity = pit.canonical_hash({"schema": "probiga.pit-finance-identity.v1", "stock_code": code,
                    "report_date": report.isoformat(), "report_type": str(source.get("report_type") or "").strip()[:64]})
                if len(revision_ids) != 1 or identity in bindings:
                    raise ValueError(f"PIT_SEALED_FINANCE_REVISION_BINDING_DIFFERS:{code}")
                bindings[identity] = (code, revision_ids[0], content)

        if not bindings:
            return
        chain_inventory = read_rows(
            connection, "revision_id,identity_hash,revision_no", pit.FINANCE_REVISION_TABLE,
            "identity_hash IN :keys AND known_at<=:cutoff AND received_at<=:cutoff",
            {"keys": sorted(bindings), "cutoff": fact_cutoff}, stage="finance-sealed-revision-links",
        )
        bound_revision_numbers = {
            row["identity_hash"]: int(row["revision_no"]) for row in chain_inventory
            if row["revision_id"] == bindings[row["identity_hash"]][1]
        }
        if bound_revision_numbers.keys() != bindings.keys():
            raise ValueError("PIT_SEALED_FINANCE_BOUND_REVISION_MISSING")
        revision_ids = {
            row["revision_id"] for row in chain_inventory
            if int(row["revision_no"]) <= bound_revision_numbers[row["identity_hash"]]
        }
        revision_rows = read_payloads(
            connection, table=pit.FINANCE_REVISION_TABLE, id_column="revision_id",
            keys=revision_ids, stage="finance-sealed-revision",
        )
        chains = defaultdict(list)
        for row in revision_rows.values():
            chains[row["identity_hash"]].append(row)
        candidates = defaultdict(list)
        for identity, (code, revision_id, content) in bindings.items():
            chain = sorted(chains[identity], key=lambda row: int(row["revision_no"]))
            selected, reason = pit._latest_revision_for_fact_cutoff(
                chain, fact_kind="finance", fact_cutoff_at=fact_cutoff, decision_at=decision,
            )
            if not selected or (
                selected["stock_code"] != code or selected["revision_id"] != revision_id
                or selected["content_hash"] != content
            ):
                raise ValueError(f"PIT_SEALED_FINANCE_REVISION_DIFFERS:{code}:{reason}")
            candidates[code].append(selected)
        for code, rows in candidates.items():
            selected = max(rows, key=lambda row: (
                pit._row_datetime(row["published_at"] or row["known_at"]),
                pit._row_datetime(row["known_at"]), int(row["revision_no"]), row["identity_hash"],
            ))
            facts[code] = pit._finance_feature_fact(selected, fact_cutoff=fact_cutoff, decision=decision)
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
