"""Frozen DDL payload for immutable V2 accounting-outcome evidence.

The payload is registered as forward-only migration 015 by
``server.db.migrations_v2``.  Importing this module still cannot execute DDL;
the explicit migration command, named lock, and execution-evidence opt-in gate
remain the only write boundary.
"""

from __future__ import annotations


ACCOUNTING_EVIDENCE_DDL_IS_REGISTERED = True
ACCOUNTING_EVIDENCE_MIGRATION_VERSION_PROPOSAL = (
    "20260803_015_v2_accounting_outcome_evidence"
)


ACCOUNTING_EVIDENCE_TABLE_DDL_PROPOSAL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS st_fill_accounting_outcome_v2 (
        accounting_outcome_id CHAR(64) PRIMARY KEY,
        fill_id VARCHAR(64) NOT NULL,
        fill_execution_evidence_id CHAR(64) NOT NULL,
        fill_execution_evidence_hash CHAR(64) NOT NULL,
        cash_binding_id CHAR(64) NOT NULL,
        cash_binding_hash CHAR(64) NOT NULL,
        cash_event_id VARCHAR(64) NOT NULL,
        order_transition_id CHAR(64) NOT NULL,
        order_transition_hash CHAR(64) NOT NULL,
        order_id VARCHAR(64) NOT NULL,
        account_id VARCHAR(64) NOT NULL,
        stock_code VARCHAR(16) NOT NULL,
        side VARCHAR(8) NOT NULL,
        account_cash_before DECIMAL(20,2) NOT NULL,
        account_cash_after DECIMAL(20,2) NOT NULL,
        lot_effect_root_hash CHAR(64) NOT NULL,
        lot_effects_hash CHAR(64) NOT NULL,
        lot_effect_count BIGINT NOT NULL,
        total_effect_quantity BIGINT NOT NULL,
        history_origin VARCHAR(40) NOT NULL,
        history_origin_id VARCHAR(128) NOT NULL,
        history_origin_at DATETIME NOT NULL,
        authority_status VARCHAR(40) NOT NULL,
        authority_receipt_hash CHAR(64) DEFAULT NULL,
        provenance_hash CHAR(64) NOT NULL,
        recorded_at DATETIME NOT NULL,
        outcome_hash CHAR(64) NOT NULL,
        created_at DATETIME NOT NULL,
        UNIQUE KEY uk_fill_accounting_outcome_v2_fill (fill_id),
        UNIQUE KEY uk_fill_accounting_outcome_v2_hash (outcome_hash),
        UNIQUE KEY uk_fill_accounting_outcome_v2_binding
            (accounting_outcome_id, fill_id),
        UNIQUE KEY uk_fill_accounting_outcome_v2_finalization_binding
            (accounting_outcome_id, fill_id, outcome_hash),
        KEY idx_fill_accounting_outcome_v2_account
            (account_id, recorded_at),
        KEY idx_fill_accounting_outcome_v2_root
            (lot_effect_root_hash),
        CONSTRAINT fk_fill_accounting_outcome_v2_fill
            FOREIGN KEY (fill_id) REFERENCES st_fill_v2 (fill_id),
        CONSTRAINT fk_fill_accounting_outcome_v2_order
            FOREIGN KEY (order_id) REFERENCES st_order_v2 (order_id),
        CONSTRAINT fk_fill_accounting_outcome_v2_account
            FOREIGN KEY (account_id)
            REFERENCES st_trade_account_v2 (account_id),
        CONSTRAINT fk_fill_accounting_outcome_v2_fill_evidence
            FOREIGN KEY (
                fill_execution_evidence_id, fill_id,
                fill_execution_evidence_hash
            ) REFERENCES st_fill_execution_evidence_v2 (
                fill_execution_evidence_id, fill_id, evidence_hash
            ),
        CONSTRAINT fk_fill_accounting_outcome_v2_cash_binding
            FOREIGN KEY (
                cash_binding_id, cash_event_id, cash_binding_hash
            ) REFERENCES st_cash_event_binding_v2 (
                cash_binding_id, cash_event_id, binding_hash
            ),
        CONSTRAINT fk_fill_accounting_outcome_v2_order_transition
            FOREIGN KEY (order_transition_id, order_transition_hash)
            REFERENCES st_order_transition_v2 (
                transition_id, transition_hash
            ),
        CHECK (accounting_outcome_id = outcome_hash),
        CHECK (side IN ('BUY', 'SELL')),
        CHECK (account_cash_before >= 0),
        CHECK (account_cash_after >= 0),
        CHECK (lot_effect_count >= 1),
        CHECK (total_effect_quantity >= 1),
        CHECK (history_origin IN
            ('START_AFTER_UNKNOWN', 'COMPLETE_FROM_DECLARED_ORIGIN')),
        CHECK (history_origin_id <> ''),
        CHECK (history_origin_at <= recorded_at),
        CHECK (authority_status = 'CONTENT_HASH_ONLY'),
        CHECK (authority_receipt_hash IS NULL),
        CHECK (recorded_at <= created_at)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS st_lot_transition_evidence_v2 (
        lot_transition_evidence_id CHAR(64) PRIMARY KEY,
        accounting_outcome_id CHAR(64) NOT NULL,
        fill_id VARCHAR(64) NOT NULL,
        fill_execution_evidence_id CHAR(64) NOT NULL,
        fill_execution_evidence_hash CHAR(64) NOT NULL,
        effect_sequence BIGINT NOT NULL,
        lot_transition_sequence BIGINT NOT NULL,
        effect_kind VARCHAR(40) NOT NULL,
        lot_effect_root_hash CHAR(64) NOT NULL,
        previous_effect_id CHAR(64) DEFAULT NULL,
        previous_effect_hash CHAR(64) DEFAULT NULL,
        previous_lot_transition_id CHAR(64) DEFAULT NULL,
        previous_lot_transition_hash CHAR(64) DEFAULT NULL,
        lot_id VARCHAR(64) NOT NULL,
        consumed_quantity BIGINT NOT NULL,
        before_lot_json LONGTEXT DEFAULT NULL,
        before_lot_hash CHAR(64) DEFAULT NULL,
        after_lot_json LONGTEXT NOT NULL,
        after_lot_hash CHAR(64) NOT NULL,
        occurred_at DATETIME NOT NULL,
        bound_at DATETIME NOT NULL,
        history_origin VARCHAR(40) NOT NULL,
        history_origin_id VARCHAR(128) NOT NULL,
        history_origin_at DATETIME NOT NULL,
        authority_status VARCHAR(40) NOT NULL,
        authority_receipt_hash CHAR(64) DEFAULT NULL,
        provenance_hash CHAR(64) NOT NULL,
        effect_hash CHAR(64) NOT NULL,
        created_at DATETIME NOT NULL,
        UNIQUE KEY uk_lot_transition_evidence_v2_hash (effect_hash),
        UNIQUE KEY uk_lot_transition_evidence_v2_binding
            (lot_transition_evidence_id, effect_hash),
        UNIQUE KEY uk_lot_transition_evidence_v2_outcome_sequence
            (accounting_outcome_id, effect_sequence),
        UNIQUE KEY uk_lot_transition_evidence_v2_fill_lot
            (fill_id, lot_id),
        UNIQUE KEY uk_lot_transition_evidence_v2_lot_sequence
            (lot_id, lot_transition_sequence),
        KEY idx_lot_transition_evidence_v2_fill
            (fill_execution_evidence_id, effect_sequence),
        KEY idx_lot_transition_evidence_v2_root
            (lot_effect_root_hash, effect_sequence),
        CONSTRAINT fk_lot_transition_evidence_v2_outcome
            FOREIGN KEY (accounting_outcome_id, fill_id)
            REFERENCES st_fill_accounting_outcome_v2 (
                accounting_outcome_id, fill_id
            ),
        CONSTRAINT fk_lot_transition_evidence_v2_fill_evidence
            FOREIGN KEY (
                fill_execution_evidence_id, fill_id,
                fill_execution_evidence_hash
            ) REFERENCES st_fill_execution_evidence_v2 (
                fill_execution_evidence_id, fill_id, evidence_hash
            ),
        CONSTRAINT fk_lot_transition_evidence_v2_lot
            FOREIGN KEY (lot_id) REFERENCES st_position_lot_v2 (lot_id),
        CONSTRAINT fk_lot_transition_evidence_v2_previous_effect
            FOREIGN KEY (previous_effect_id, previous_effect_hash)
            REFERENCES st_lot_transition_evidence_v2 (
                lot_transition_evidence_id, effect_hash
            ),
        CONSTRAINT fk_lot_transition_evidence_v2_previous_lot
            FOREIGN KEY (
                previous_lot_transition_id,
                previous_lot_transition_hash
            ) REFERENCES st_lot_transition_evidence_v2 (
                lot_transition_evidence_id, effect_hash
            ),
        CHECK (lot_transition_evidence_id = effect_hash),
        CHECK (effect_sequence >= 0),
        CHECK (lot_transition_sequence >= 0),
        CHECK (effect_kind IN ('BUY_CREATE', 'SELL_FIFO_CONSUME')),
        CHECK ((previous_effect_id IS NULL) =
            (previous_effect_hash IS NULL)),
        CHECK ((effect_sequence = 0
                AND previous_effect_id IS NULL)
            OR (effect_sequence > 0
                AND previous_effect_id IS NOT NULL)),
        CHECK ((previous_lot_transition_id IS NULL) =
            (previous_lot_transition_hash IS NULL)),
        CHECK ((lot_transition_sequence = 0
                AND previous_lot_transition_id IS NULL)
            OR (lot_transition_sequence > 0
                AND previous_lot_transition_id IS NOT NULL)),
        CHECK ((before_lot_json IS NULL) = (before_lot_hash IS NULL)),
        CHECK ((effect_kind = 'BUY_CREATE'
                AND consumed_quantity = 0
                AND before_lot_json IS NULL
                AND lot_transition_sequence = 0)
            OR (effect_kind = 'SELL_FIFO_CONSUME'
                AND consumed_quantity >= 1
                AND before_lot_json IS NOT NULL)),
        CHECK (history_origin <> 'COMPLETE_FROM_DECLARED_ORIGIN'
            OR effect_kind <> 'SELL_FIFO_CONSUME'
            OR previous_lot_transition_id IS NOT NULL),
        CHECK (before_lot_json IS NULL OR JSON_VALID(before_lot_json)),
        CHECK (JSON_VALID(after_lot_json)),
        CHECK (history_origin IN
            ('START_AFTER_UNKNOWN', 'COMPLETE_FROM_DECLARED_ORIGIN')),
        CHECK (history_origin_id <> ''),
        CHECK (history_origin_at <= occurred_at),
        CHECK (authority_status = 'CONTENT_HASH_ONLY'),
        CHECK (authority_receipt_hash IS NULL),
        CHECK (occurred_at <= bound_at),
        CHECK (bound_at <= created_at)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS st_fill_accounting_outcome_finalization_v2 (
        finalization_id CHAR(64) PRIMARY KEY,
        accounting_outcome_id CHAR(64) NOT NULL,
        fill_id VARCHAR(64) NOT NULL,
        outcome_hash CHAR(64) NOT NULL,
        fill_execution_evidence_id CHAR(64) NOT NULL,
        fill_execution_evidence_hash CHAR(64) NOT NULL,
        lot_effect_root_hash CHAR(64) NOT NULL,
        lot_effects_hash CHAR(64) NOT NULL,
        effect_hashes_json LONGTEXT NOT NULL,
        lot_effect_count BIGINT NOT NULL,
        total_effect_quantity BIGINT NOT NULL,
        finalization_status VARCHAR(16) NOT NULL,
        history_origin VARCHAR(40) NOT NULL,
        history_origin_id VARCHAR(128) NOT NULL,
        history_origin_at DATETIME NOT NULL,
        authority_status VARCHAR(40) NOT NULL,
        authority_receipt_hash CHAR(64) DEFAULT NULL,
        provenance_hash CHAR(64) NOT NULL,
        finalized_at DATETIME NOT NULL,
        finalization_hash CHAR(64) NOT NULL,
        created_at DATETIME NOT NULL,
        UNIQUE KEY uk_fill_accounting_finalization_v2_outcome
            (accounting_outcome_id),
        UNIQUE KEY uk_fill_accounting_finalization_v2_fill (fill_id),
        UNIQUE KEY uk_fill_accounting_finalization_v2_hash
            (finalization_hash),
        UNIQUE KEY uk_fill_accounting_finalization_v2_binding
            (finalization_id, finalization_hash),
        KEY idx_fill_accounting_finalization_v2_status
            (finalization_status, finalized_at),
        CONSTRAINT fk_fill_accounting_finalization_v2_outcome
            FOREIGN KEY (accounting_outcome_id, fill_id, outcome_hash)
            REFERENCES st_fill_accounting_outcome_v2 (
                accounting_outcome_id, fill_id, outcome_hash
            ),
        CONSTRAINT fk_fill_accounting_finalization_v2_fill_evidence
            FOREIGN KEY (
                fill_execution_evidence_id, fill_id,
                fill_execution_evidence_hash
            ) REFERENCES st_fill_execution_evidence_v2 (
                fill_execution_evidence_id, fill_id, evidence_hash
            ),
        CHECK (finalization_id = finalization_hash),
        CHECK (finalization_status = 'FINAL'),
        CHECK (lot_effect_count >= 1),
        CHECK (total_effect_quantity >= 1),
        CHECK (JSON_VALID(effect_hashes_json)),
        CHECK (history_origin IN
            ('START_AFTER_UNKNOWN', 'COMPLETE_FROM_DECLARED_ORIGIN')),
        CHECK (history_origin_id <> ''),
        CHECK (history_origin_at <= finalized_at),
        CHECK (authority_status = 'CONTENT_HASH_ONLY'),
        CHECK (authority_receipt_hash IS NULL),
        CHECK (finalized_at <= created_at)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4
    """,
)


ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL: tuple[str, ...] = (
    "DROP TRIGGER IF EXISTS trg_fill_accounting_outcome_v2_guard_bi",
    """
    CREATE TRIGGER trg_fill_accounting_outcome_v2_guard_bi
    BEFORE INSERT ON st_fill_accounting_outcome_v2
    FOR EACH ROW
    BEGIN
        DECLARE nested_count INT DEFAULT 0;

        IF NEW.side NOT IN ('BUY', 'SELL')
           OR NEW.account_cash_before < 0
           OR NEW.account_cash_after < 0
           OR NEW.lot_effect_count < 1
           OR NEW.total_effect_quantity < 1
           OR (NEW.side = 'BUY' AND NEW.lot_effect_count <> 1)
           OR NEW.history_origin NOT IN
                ('START_AFTER_UNKNOWN', 'COMPLETE_FROM_DECLARED_ORIGIN')
           OR NEW.history_origin_id IS NULL
           OR NEW.history_origin_id = ''
           OR NEW.history_origin_at IS NULL
           OR NEW.history_origin_at > NEW.recorded_at
           OR NEW.recorded_at > NEW.created_at
           OR NEW.authority_status <> 'CONTENT_HASH_ONLY'
           OR NEW.authority_receipt_hash IS NOT NULL THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid fill accounting outcome core fields';
        END IF;

        IF CHAR_LENGTH(NEW.accounting_outcome_id) <> 64
           OR BINARY NEW.accounting_outcome_id
                <> BINARY LOWER(NEW.accounting_outcome_id)
           OR NEW.accounting_outcome_id REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.fill_execution_evidence_id) <> 64
           OR BINARY NEW.fill_execution_evidence_id
                <> BINARY LOWER(NEW.fill_execution_evidence_id)
           OR NEW.fill_execution_evidence_id REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.fill_execution_evidence_hash) <> 64
           OR BINARY NEW.fill_execution_evidence_hash
                <> BINARY LOWER(NEW.fill_execution_evidence_hash)
           OR NEW.fill_execution_evidence_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.cash_binding_id) <> 64
           OR BINARY NEW.cash_binding_id
                <> BINARY LOWER(NEW.cash_binding_id)
           OR NEW.cash_binding_id REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.cash_binding_hash) <> 64
           OR BINARY NEW.cash_binding_hash
                <> BINARY LOWER(NEW.cash_binding_hash)
           OR NEW.cash_binding_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.order_transition_id) <> 64
           OR BINARY NEW.order_transition_id
                <> BINARY LOWER(NEW.order_transition_id)
           OR NEW.order_transition_id REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.order_transition_hash) <> 64
           OR BINARY NEW.order_transition_hash
                <> BINARY LOWER(NEW.order_transition_hash)
           OR NEW.order_transition_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.lot_effect_root_hash) <> 64
           OR BINARY NEW.lot_effect_root_hash
                <> BINARY LOWER(NEW.lot_effect_root_hash)
           OR NEW.lot_effect_root_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.lot_effects_hash) <> 64
           OR BINARY NEW.lot_effects_hash
                <> BINARY LOWER(NEW.lot_effects_hash)
           OR NEW.lot_effects_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.provenance_hash) <> 64
           OR BINARY NEW.provenance_hash
                <> BINARY LOWER(NEW.provenance_hash)
           OR NEW.provenance_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.outcome_hash) <> 64
           OR BINARY NEW.outcome_hash <> BINARY LOWER(NEW.outcome_hash)
           OR NEW.outcome_hash REGEXP '[^0-9a-f]' THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid fill accounting outcome SHA256';
        END IF;

        IF BINARY NEW.accounting_outcome_id <> BINARY NEW.outcome_hash THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'fill accounting outcome identity mismatch';
        END IF;

        SELECT COUNT(*) INTO nested_count
        FROM st_fill_execution_evidence_v2 fe
        INNER JOIN st_fill_v2 f
            ON BINARY f.fill_id = BINARY fe.fill_id
        INNER JOIN st_cash_event_binding_v2 cb
            ON BINARY cb.cash_binding_id = BINARY NEW.cash_binding_id
           AND BINARY cb.cash_event_id = BINARY NEW.cash_event_id
           AND BINARY cb.binding_hash = BINARY NEW.cash_binding_hash
        INNER JOIN st_cash_ledger_v2 cl
            ON BINARY cl.cash_event_id = BINARY cb.cash_event_id
        INNER JOIN st_order_transition_v2 ot
            ON BINARY ot.transition_id = BINARY NEW.order_transition_id
           AND BINARY ot.transition_hash = BINARY NEW.order_transition_hash
        INNER JOIN st_order_v2 o
            ON BINARY o.order_id = BINARY NEW.order_id
        INNER JOIN st_trade_account_v2 a
            ON BINARY a.account_id = BINARY NEW.account_id
        WHERE BINARY fe.fill_execution_evidence_id
                = BINARY NEW.fill_execution_evidence_id
          AND BINARY fe.fill_id = BINARY NEW.fill_id
          AND BINARY fe.evidence_hash
                = BINARY NEW.fill_execution_evidence_hash
          AND BINARY fe.order_id = BINARY NEW.order_id
          AND BINARY fe.account_id = BINARY NEW.account_id
          AND BINARY fe.stock_code = BINARY NEW.stock_code
          AND BINARY f.order_id = BINARY NEW.order_id
          AND BINARY f.account_id = BINARY NEW.account_id
          AND BINARY f.stock_code = BINARY NEW.stock_code
          AND BINARY f.side = BINARY NEW.side
          AND f.quantity = NEW.total_effect_quantity
          AND f.net_cash_amount = cl.amount
          AND f.filled_at = fe.executed_at
          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                fe.fill_payload_json, '$.side')) = BINARY NEW.side
          AND JSON_EXTRACT(fe.fill_payload_json, '$.quantity')
                = NEW.total_effect_quantity
          AND BINARY cb.account_id = BINARY NEW.account_id
          AND BINARY cb.related_order_id = BINARY NEW.order_id
          AND BINARY cb.related_fill_id = BINARY NEW.fill_id
          AND BINARY cb.fill_execution_evidence_id
                = BINARY NEW.fill_execution_evidence_id
          AND BINARY cb.fill_execution_evidence_hash
                = BINARY NEW.fill_execution_evidence_hash
          AND BINARY cb.cash_event_type
                = BINARY CONCAT(NEW.side, '_FILL')
          AND BINARY cl.account_id = BINARY NEW.account_id
          AND BINARY cl.related_order_id = BINARY NEW.order_id
          AND BINARY cl.related_fill_id = BINARY NEW.fill_id
          AND BINARY cl.event_type = BINARY CONCAT(NEW.side, '_FILL')
          AND cl.balance_after = NEW.account_cash_after
          AND NEW.account_cash_before + cl.amount
                = NEW.account_cash_after
          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                cb.cash_event_payload_json, '$.amount')) AS DECIMAL(20,2))
                = cl.amount
          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                cb.cash_event_payload_json, '$.balance_after'))
                AS DECIMAL(20,2)) = NEW.account_cash_after
          AND BINARY ot.order_id = BINARY NEW.order_id
          AND BINARY ot.account_id = BINARY NEW.account_id
          AND BINARY ot.transition_kind = BINARY 'FILL_APPLIED'
          AND BINARY ot.related_fill_id = BINARY NEW.fill_id
          AND BINARY ot.fill_execution_evidence_id
                = BINARY NEW.fill_execution_evidence_id
          AND BINARY ot.fill_execution_evidence_hash
                = BINARY NEW.fill_execution_evidence_hash
          AND ot.next_filled_quantity - ot.previous_filled_quantity
                = f.quantity
          AND BINARY o.account_id = BINARY NEW.account_id
          AND BINARY o.stock_code = BINARY NEW.stock_code
          AND BINARY o.side = BINARY NEW.side
          AND o.filled_quantity = ot.next_filled_quantity
          AND BINARY o.status = BINARY ot.to_status
          AND a.cash_balance = NEW.account_cash_after
          AND fe.bound_at <= NEW.recorded_at
          AND cb.bound_at <= NEW.recorded_at
          AND ot.recorded_at <= NEW.recorded_at
          AND fe.history_origin = NEW.history_origin
          AND fe.history_origin_id <=> NEW.history_origin_id
          AND fe.history_origin_at <=> NEW.history_origin_at
          AND cb.history_origin = NEW.history_origin
          AND cb.history_origin_id <=> NEW.history_origin_id
          AND cb.history_origin_at <=> NEW.history_origin_at
          AND ot.history_origin = NEW.history_origin
          AND ot.history_origin_id <=> NEW.history_origin_id
          AND ot.history_origin_at <=> NEW.history_origin_at
          AND fe.authority_status = NEW.authority_status
          AND cb.authority_status = NEW.authority_status
          AND ot.authority_status = NEW.authority_status;

        IF nested_count <> 1 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'accounting outcome nested evidence mismatch';
        END IF;
    END
    """,
    "DROP TRIGGER IF EXISTS trg_lot_transition_evidence_v2_guard_bi",
    """
    CREATE TRIGGER trg_lot_transition_evidence_v2_guard_bi
    BEFORE INSERT ON st_lot_transition_evidence_v2
    FOR EACH ROW
    BEGIN
        DECLARE parent_count INT DEFAULT 0;
        DECLARE predecessor_count INT DEFAULT 0;
        DECLARE prior_consumed BIGINT DEFAULT 0;
        DECLARE expected_total BIGINT DEFAULT 0;
        DECLARE expected_effect_count BIGINT DEFAULT 0;

        IF NEW.effect_sequence < 0
           OR NEW.lot_transition_sequence < 0
           OR NEW.consumed_quantity < 0
           OR NEW.effect_kind NOT IN ('BUY_CREATE', 'SELL_FIFO_CONSUME')
           OR NEW.history_origin NOT IN
                ('START_AFTER_UNKNOWN', 'COMPLETE_FROM_DECLARED_ORIGIN')
           OR NEW.history_origin_id IS NULL
           OR NEW.history_origin_id = ''
           OR NEW.history_origin_at IS NULL
           OR NEW.history_origin_at > NEW.occurred_at
           OR NEW.occurred_at > NEW.bound_at
           OR NEW.bound_at > NEW.created_at
           OR NEW.authority_status <> 'CONTENT_HASH_ONLY'
           OR NEW.authority_receipt_hash IS NOT NULL THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid lot transition evidence core fields';
        END IF;

        IF (NEW.previous_effect_id IS NULL)
                <> (NEW.previous_effect_hash IS NULL)
           OR (NEW.previous_lot_transition_id IS NULL)
                <> (NEW.previous_lot_transition_hash IS NULL)
           OR (NEW.before_lot_json IS NULL)
                <> (NEW.before_lot_hash IS NULL)
           OR (NEW.effect_sequence = 0
                AND NEW.previous_effect_id IS NOT NULL)
           OR (NEW.effect_sequence > 0
                AND NEW.previous_effect_id IS NULL)
           OR (NEW.lot_transition_sequence = 0
                AND NEW.previous_lot_transition_id IS NOT NULL)
           OR (NEW.lot_transition_sequence > 0
                AND NEW.previous_lot_transition_id IS NULL) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid lot transition predecessor shape';
        END IF;

        IF CHAR_LENGTH(NEW.lot_transition_evidence_id) <> 64
           OR BINARY NEW.lot_transition_evidence_id
                <> BINARY LOWER(NEW.lot_transition_evidence_id)
           OR NEW.lot_transition_evidence_id REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.accounting_outcome_id) <> 64
           OR BINARY NEW.accounting_outcome_id
                <> BINARY LOWER(NEW.accounting_outcome_id)
           OR NEW.accounting_outcome_id REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.fill_execution_evidence_id) <> 64
           OR BINARY NEW.fill_execution_evidence_id
                <> BINARY LOWER(NEW.fill_execution_evidence_id)
           OR NEW.fill_execution_evidence_id REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.fill_execution_evidence_hash) <> 64
           OR BINARY NEW.fill_execution_evidence_hash
                <> BINARY LOWER(NEW.fill_execution_evidence_hash)
           OR NEW.fill_execution_evidence_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.lot_effect_root_hash) <> 64
           OR BINARY NEW.lot_effect_root_hash
                <> BINARY LOWER(NEW.lot_effect_root_hash)
           OR NEW.lot_effect_root_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.after_lot_hash) <> 64
           OR BINARY NEW.after_lot_hash
                <> BINARY LOWER(NEW.after_lot_hash)
           OR NEW.after_lot_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.provenance_hash) <> 64
           OR BINARY NEW.provenance_hash
                <> BINARY LOWER(NEW.provenance_hash)
           OR NEW.provenance_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.effect_hash) <> 64
           OR BINARY NEW.effect_hash <> BINARY LOWER(NEW.effect_hash)
           OR NEW.effect_hash REGEXP '[^0-9a-f]'
           OR (NEW.before_lot_hash IS NOT NULL AND (
                CHAR_LENGTH(NEW.before_lot_hash) <> 64
                OR BINARY NEW.before_lot_hash
                    <> BINARY LOWER(NEW.before_lot_hash)
                OR NEW.before_lot_hash REGEXP '[^0-9a-f]'))
           OR (NEW.previous_effect_id IS NOT NULL AND (
                CHAR_LENGTH(NEW.previous_effect_id) <> 64
                OR BINARY NEW.previous_effect_id
                    <> BINARY LOWER(NEW.previous_effect_id)
                OR NEW.previous_effect_id REGEXP '[^0-9a-f]'
                OR CHAR_LENGTH(NEW.previous_effect_hash) <> 64
                OR BINARY NEW.previous_effect_hash
                    <> BINARY LOWER(NEW.previous_effect_hash)
                OR NEW.previous_effect_hash REGEXP '[^0-9a-f]'))
           OR (NEW.previous_lot_transition_id IS NOT NULL AND (
                CHAR_LENGTH(NEW.previous_lot_transition_id) <> 64
                OR BINARY NEW.previous_lot_transition_id
                    <> BINARY LOWER(NEW.previous_lot_transition_id)
                OR NEW.previous_lot_transition_id REGEXP '[^0-9a-f]'
                OR CHAR_LENGTH(NEW.previous_lot_transition_hash) <> 64
                OR BINARY NEW.previous_lot_transition_hash
                    <> BINARY LOWER(NEW.previous_lot_transition_hash)
                OR NEW.previous_lot_transition_hash REGEXP '[^0-9a-f]')) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid lot transition evidence SHA256';
        END IF;

        IF BINARY NEW.lot_transition_evidence_id <> BINARY NEW.effect_hash THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'lot transition evidence identity mismatch';
        END IF;

        IF JSON_VALID(NEW.after_lot_json) <> 1
           OR (NEW.before_lot_json IS NOT NULL
                AND JSON_VALID(NEW.before_lot_json) <> 1) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid canonical lot snapshot JSON';
        END IF;

        IF JSON_TYPE(JSON_EXTRACT(NEW.after_lot_json, '$')) <> 'OBJECT'
           OR JSON_LENGTH(NEW.after_lot_json) <> 21
           OR JSON_CONTAINS_PATH(
                NEW.after_lot_json, 'all',
                '$.lot_id', '$.account_id', '$.stock_code',
                '$.theme_code', '$.strategy_version', '$.opened_fill_id',
                '$.opened_trade_date', '$.settlement_date',
                '$.original_quantity', '$.remaining_quantity',
                '$.cost_price', '$.allocated_buy_fee', '$.position_state',
                '$.approved_target_quantity', '$.add_count',
                '$.initial_stop', '$.protective_stop',
                '$.invalidation_condition', '$.version',
                '$.created_at', '$.closed_at') <> 1
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.lot_id')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.account_id')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.stock_code')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.theme_code')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.strategy_version')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.opened_fill_id')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.opened_trade_date')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.settlement_date')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.original_quantity')) <> 'INTEGER'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.remaining_quantity')) <> 'INTEGER'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.cost_price')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.allocated_buy_fee')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.position_state')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json,
                '$.approved_target_quantity')) <> 'INTEGER'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.add_count')) <> 'INTEGER'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.initial_stop')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.protective_stop')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json,
                '$.invalidation_condition')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.version')) <> 'INTEGER'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.created_at')) <> 'STRING'
           OR JSON_TYPE(JSON_EXTRACT(
                NEW.after_lot_json, '$.closed_at'))
                NOT IN ('NULL', 'STRING')
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.cost_price'))
                NOT REGEXP '^-?[0-9]+[.][0-9]{6}$'
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.allocated_buy_fee'))
                NOT REGEXP '^-?[0-9]+[.][0-9]{2}$'
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.initial_stop'))
                NOT REGEXP '^-?[0-9]+[.][0-9]{6}$'
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.protective_stop'))
                NOT REGEXP '^-?[0-9]+[.][0-9]{6}$'
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.opened_trade_date'))
                NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.settlement_date'))
                NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.created_at'))
                NOT REGEXP
                '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}[+]00:00$'
           OR (JSON_TYPE(JSON_EXTRACT(
                    NEW.after_lot_json, '$.closed_at')) = 'STRING'
                AND JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.after_lot_json, '$.closed_at'))
                    NOT REGEXP
                    '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}[+]00:00$')
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.lot_id')) = ''
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.account_id')) = ''
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.stock_code')) = ''
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.strategy_version')) = ''
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.opened_fill_id')) = ''
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.invalidation_condition')) = ''
           OR JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.position_state')) NOT IN
                ('OPENING', 'VALID_STRONG', 'VALID', 'WEAKENED',
                 'BROKEN', 'RISK_EXIT', 'EXIT_PENDING_T1',
                 'EXIT_PENDING_LIQUIDITY', 'CLOSED')
           OR JSON_EXTRACT(
                NEW.after_lot_json, '$.original_quantity') < 1
           OR JSON_EXTRACT(
                NEW.after_lot_json, '$.remaining_quantity') < 0
           OR JSON_EXTRACT(NEW.after_lot_json, '$.remaining_quantity')
                > JSON_EXTRACT(
                    NEW.after_lot_json, '$.original_quantity')
           OR JSON_EXTRACT(
                NEW.after_lot_json, '$.approved_target_quantity') < 1
           OR JSON_EXTRACT(NEW.after_lot_json, '$.add_count') < 0
           OR JSON_EXTRACT(NEW.after_lot_json, '$.version') < 1 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'noncanonical after lot snapshot';
        END IF;

        SELECT COUNT(*) INTO parent_count
        FROM st_fill_accounting_outcome_v2 ao
        INNER JOIN st_fill_execution_evidence_v2 fe
            ON BINARY fe.fill_execution_evidence_id
                = BINARY NEW.fill_execution_evidence_id
           AND BINARY fe.fill_id = BINARY NEW.fill_id
           AND BINARY fe.evidence_hash
                = BINARY NEW.fill_execution_evidence_hash
        INNER JOIN st_fill_v2 f
            ON BINARY f.fill_id = BINARY NEW.fill_id
        INNER JOIN st_position_lot_v2 lot
            ON BINARY lot.lot_id = BINARY NEW.lot_id
        WHERE BINARY ao.accounting_outcome_id
                = BINARY NEW.accounting_outcome_id
          AND BINARY ao.fill_id = BINARY NEW.fill_id
          AND BINARY ao.fill_execution_evidence_id
                = BINARY NEW.fill_execution_evidence_id
          AND BINARY ao.fill_execution_evidence_hash
                = BINARY NEW.fill_execution_evidence_hash
          AND BINARY ao.lot_effect_root_hash
                = BINARY NEW.lot_effect_root_hash
          AND NEW.effect_sequence < ao.lot_effect_count
          AND ao.recorded_at >= NEW.bound_at
          AND ao.history_origin = NEW.history_origin
          AND ao.history_origin_id <=> NEW.history_origin_id
          AND ao.history_origin_at <=> NEW.history_origin_at
          AND ao.authority_status = NEW.authority_status
          AND BINARY ao.provenance_hash = BINARY NEW.provenance_hash
          AND ((ao.side = 'BUY' AND NEW.effect_kind = 'BUY_CREATE')
            OR (ao.side = 'SELL'
                AND NEW.effect_kind = 'SELL_FIFO_CONSUME'))
          AND BINARY fe.order_id = BINARY ao.order_id
          AND BINARY fe.account_id = BINARY ao.account_id
          AND BINARY fe.stock_code = BINARY ao.stock_code
          AND fe.executed_at = NEW.occurred_at
          AND fe.bound_at <= NEW.bound_at
          AND BINARY f.order_id = BINARY ao.order_id
          AND BINARY f.account_id = BINARY ao.account_id
          AND BINARY f.stock_code = BINARY ao.stock_code
          AND BINARY f.side = BINARY ao.side
          AND f.quantity = ao.total_effect_quantity
          AND BINARY lot.account_id = BINARY ao.account_id
          AND BINARY lot.stock_code = BINARY ao.stock_code
          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.lot_id')) = BINARY lot.lot_id
          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.account_id')) = BINARY lot.account_id
          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.stock_code')) = BINARY lot.stock_code
          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.theme_code')) = BINARY lot.theme_code
          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.strategy_version'))
                = BINARY lot.strategy_version
          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.opened_fill_id'))
                = BINARY lot.opened_fill_id
          AND JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.opened_trade_date'))
                = DATE_FORMAT(lot.opened_trade_date, '%Y-%m-%d')
          AND JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.settlement_date'))
                = DATE_FORMAT(lot.settlement_date, '%Y-%m-%d')
          AND JSON_EXTRACT(NEW.after_lot_json, '$.original_quantity')
                = lot.original_quantity
          AND JSON_EXTRACT(NEW.after_lot_json, '$.remaining_quantity')
                = lot.remaining_quantity
          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.cost_price')) AS DECIMAL(20,6))
                = lot.cost_price
          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.allocated_buy_fee'))
                AS DECIMAL(20,2)) = lot.allocated_buy_fee
          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.position_state'))
                = BINARY lot.position_state
          AND JSON_EXTRACT(
                NEW.after_lot_json, '$.approved_target_quantity')
                = lot.approved_target_quantity
          AND JSON_EXTRACT(NEW.after_lot_json, '$.add_count') = lot.add_count
          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.initial_stop')) AS DECIMAL(20,6))
                = lot.initial_stop
          AND CAST(JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.protective_stop')) AS DECIMAL(20,6))
                = lot.protective_stop
          AND BINARY JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.invalidation_condition'))
                = BINARY lot.invalidation_condition
          AND JSON_EXTRACT(NEW.after_lot_json, '$.version') = lot.version
          AND JSON_UNQUOTE(JSON_EXTRACT(
                NEW.after_lot_json, '$.created_at'))
                = DATE_FORMAT(
                    CONVERT_TZ(lot.created_at, '+08:00', '+00:00'),
                    '%Y-%m-%dT%H:%i:%s.%f+00:00')
          AND ((lot.closed_at IS NULL
                AND JSON_TYPE(JSON_EXTRACT(
                    NEW.after_lot_json, '$.closed_at')) = 'NULL')
            OR (lot.closed_at IS NOT NULL
                AND JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.after_lot_json, '$.closed_at'))
                    = DATE_FORMAT(
                        CONVERT_TZ(lot.closed_at, '+08:00', '+00:00'),
                        '%Y-%m-%dT%H:%i:%s.%f+00:00')));

        IF parent_count <> 1 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'lot effect parent or canonical after row mismatch';
        END IF;

        IF NEW.effect_sequence > 0 THEN
            SELECT COUNT(*) INTO predecessor_count
            FROM st_lot_transition_evidence_v2 previous_effect
            WHERE BINARY previous_effect.lot_transition_evidence_id
                    = BINARY NEW.previous_effect_id
              AND BINARY previous_effect.effect_hash
                    = BINARY NEW.previous_effect_hash
              AND BINARY previous_effect.accounting_outcome_id
                    = BINARY NEW.accounting_outcome_id
              AND BINARY previous_effect.fill_id = BINARY NEW.fill_id
              AND previous_effect.effect_sequence + 1
                    = NEW.effect_sequence
              AND BINARY previous_effect.lot_effect_root_hash
                    = BINARY NEW.lot_effect_root_hash
              AND previous_effect.bound_at <= NEW.bound_at
              AND previous_effect.history_origin = NEW.history_origin
              AND previous_effect.history_origin_id <=> NEW.history_origin_id
              AND previous_effect.history_origin_at <=> NEW.history_origin_at
              AND BINARY previous_effect.provenance_hash
                    = BINARY NEW.provenance_hash;
            IF predecessor_count <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'lot effect predecessor mismatch';
            END IF;
        END IF;

        IF NEW.lot_transition_sequence > 0 THEN
            SELECT COUNT(*) INTO predecessor_count
            FROM st_lot_transition_evidence_v2 previous_lot
            WHERE BINARY previous_lot.lot_transition_evidence_id
                    = BINARY NEW.previous_lot_transition_id
              AND BINARY previous_lot.effect_hash
                    = BINARY NEW.previous_lot_transition_hash
              AND BINARY previous_lot.lot_id = BINARY NEW.lot_id
              AND previous_lot.lot_transition_sequence + 1
                    = NEW.lot_transition_sequence
              AND BINARY previous_lot.after_lot_hash
                    = BINARY NEW.before_lot_hash
              AND previous_lot.bound_at <= NEW.occurred_at
              AND previous_lot.history_origin = NEW.history_origin
              AND previous_lot.history_origin_id <=> NEW.history_origin_id
              AND previous_lot.history_origin_at <=> NEW.history_origin_at;
            IF predecessor_count <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'per-lot transition predecessor mismatch';
            END IF;
        END IF;

        IF NEW.effect_kind = 'BUY_CREATE' THEN
            IF NEW.effect_sequence <> 0
               OR NEW.lot_transition_sequence <> 0
               OR NEW.previous_effect_id IS NOT NULL
               OR NEW.previous_lot_transition_id IS NOT NULL
               OR NEW.before_lot_json IS NOT NULL
               OR NEW.consumed_quantity <> 0
               OR BINARY NEW.lot_id <> BINARY CONCAT('LOT:', NEW.fill_id) THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'invalid BUY lot creation shape';
            END IF;

            SELECT COUNT(*) INTO parent_count
            FROM st_fill_accounting_outcome_v2 ao
            INNER JOIN st_fill_v2 f
                ON BINARY f.fill_id = BINARY NEW.fill_id
            INNER JOIN st_fill_execution_evidence_v2 fe
                ON BINARY fe.fill_execution_evidence_id
                    = BINARY NEW.fill_execution_evidence_id
            INNER JOIN st_position_lot_v2 lot
                ON BINARY lot.lot_id = BINARY NEW.lot_id
            WHERE BINARY ao.accounting_outcome_id
                    = BINARY NEW.accounting_outcome_id
              AND ao.side = 'BUY'
              AND ao.lot_effect_count = 1
              AND ao.total_effect_quantity = f.quantity
              AND BINARY lot.opened_fill_id = BINARY NEW.fill_id
              AND lot.original_quantity = f.quantity
              AND lot.remaining_quantity = f.quantity
              AND lot.cost_price = f.price
              AND lot.allocated_buy_fee = f.fee_amount
              AND lot.position_state = 'OPENING'
              AND lot.version = 1
              AND lot.closed_at IS NULL
              AND lot.created_at >= NEW.occurred_at
              AND lot.created_at <= NEW.bound_at
              AND DATE_FORMAT(lot.settlement_date, '%Y-%m-%d')
                    = JSON_UNQUOTE(JSON_EXTRACT(
                        fe.settlement_evidence_json, '$.settlement_date'));
            IF parent_count <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'BUY lot differs from fill outcome';
            END IF;
        ELSE
            IF NEW.before_lot_json IS NULL
               OR NEW.consumed_quantity < 1
               OR (NEW.history_origin = 'COMPLETE_FROM_DECLARED_ORIGIN'
                    AND NEW.previous_lot_transition_id IS NULL)
               OR JSON_VALID(NEW.before_lot_json) <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'SELL requires a canonical before snapshot';
            END IF;

            IF JSON_TYPE(JSON_EXTRACT(NEW.before_lot_json, '$')) <> 'OBJECT'
               OR JSON_LENGTH(NEW.before_lot_json) <> 21
               OR JSON_CONTAINS_PATH(
                    NEW.before_lot_json, 'all',
                    '$.lot_id', '$.account_id', '$.stock_code',
                    '$.theme_code', '$.strategy_version', '$.opened_fill_id',
                    '$.opened_trade_date', '$.settlement_date',
                    '$.original_quantity', '$.remaining_quantity',
                    '$.cost_price', '$.allocated_buy_fee',
                    '$.position_state', '$.approved_target_quantity',
                    '$.add_count', '$.initial_stop', '$.protective_stop',
                    '$.invalidation_condition', '$.version',
                    '$.created_at', '$.closed_at') <> 1
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.lot_id')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.account_id')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.stock_code')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.theme_code')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.strategy_version')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.opened_fill_id')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.opened_trade_date')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.settlement_date')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json,
                    '$.original_quantity')) <> 'INTEGER'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json,
                    '$.remaining_quantity')) <> 'INTEGER'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.cost_price')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json,
                    '$.allocated_buy_fee')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.position_state')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json,
                    '$.approved_target_quantity')) <> 'INTEGER'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.add_count')) <> 'INTEGER'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.initial_stop')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.protective_stop')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json,
                    '$.invalidation_condition')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.version')) <> 'INTEGER'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.created_at')) <> 'STRING'
               OR JSON_TYPE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.closed_at')) <> 'NULL'
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.cost_price'))
                    NOT REGEXP '^-?[0-9]+[.][0-9]{6}$'
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.allocated_buy_fee'))
                    NOT REGEXP '^-?[0-9]+[.][0-9]{2}$'
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.initial_stop'))
                    NOT REGEXP '^-?[0-9]+[.][0-9]{6}$'
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.protective_stop'))
                    NOT REGEXP '^-?[0-9]+[.][0-9]{6}$'
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.opened_trade_date'))
                    NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.settlement_date'))
                    NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.created_at'))
                    NOT REGEXP
                    '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}[+]00:00$'
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.lot_id')) = ''
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.account_id')) = ''
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.stock_code')) = ''
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.strategy_version')) = ''
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.opened_fill_id')) = ''
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.invalidation_condition')) = ''
               OR JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.position_state')) NOT IN
                    ('OPENING', 'VALID_STRONG', 'VALID', 'WEAKENED',
                     'BROKEN', 'RISK_EXIT', 'EXIT_PENDING_T1',
                     'EXIT_PENDING_LIQUIDITY')
               OR JSON_EXTRACT(
                    NEW.before_lot_json, '$.original_quantity') < 1
               OR JSON_EXTRACT(
                    NEW.before_lot_json, '$.remaining_quantity') < 1
               OR JSON_EXTRACT(
                    NEW.before_lot_json, '$.approved_target_quantity') < 1
               OR JSON_EXTRACT(NEW.before_lot_json, '$.add_count') < 0
               OR JSON_EXTRACT(NEW.before_lot_json, '$.version') < 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'noncanonical before lot snapshot';
            END IF;

            IF BINARY JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.lot_id')) <> BINARY NEW.lot_id
               OR JSON_EXTRACT(
                    NEW.before_lot_json, '$.remaining_quantity')
                    < NEW.consumed_quantity
               OR JSON_EXTRACT(
                    NEW.before_lot_json, '$.remaining_quantity') < 0
               OR JSON_EXTRACT(
                    NEW.before_lot_json, '$.remaining_quantity')
                    > JSON_EXTRACT(
                        NEW.before_lot_json, '$.original_quantity')
               OR JSON_EXTRACT(NEW.after_lot_json, '$.remaining_quantity')
                    <> JSON_EXTRACT(
                        NEW.before_lot_json, '$.remaining_quantity')
                       - NEW.consumed_quantity
               OR JSON_EXTRACT(NEW.after_lot_json, '$.version')
                    <> JSON_EXTRACT(NEW.before_lot_json, '$.version') + 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'invalid SELL lot consumption shape';
            END IF;

            IF NOT (JSON_EXTRACT(NEW.before_lot_json, '$.account_id')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.account_id'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.stock_code')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.stock_code'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.theme_code')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.theme_code'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.strategy_version')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.strategy_version'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.opened_fill_id')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.opened_fill_id'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.opened_trade_date')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.opened_trade_date'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.settlement_date')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.settlement_date'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.original_quantity')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.original_quantity'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.cost_price')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.cost_price'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.allocated_buy_fee')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.allocated_buy_fee'))
               OR NOT (JSON_EXTRACT(
                    NEW.before_lot_json, '$.approved_target_quantity')
                    <=> JSON_EXTRACT(
                        NEW.after_lot_json, '$.approved_target_quantity'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.add_count')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.add_count'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.initial_stop')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.initial_stop'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.protective_stop')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.protective_stop'))
               OR NOT (JSON_EXTRACT(
                    NEW.before_lot_json, '$.invalidation_condition')
                    <=> JSON_EXTRACT(
                        NEW.after_lot_json, '$.invalidation_condition'))
               OR NOT (JSON_EXTRACT(NEW.before_lot_json, '$.created_at')
                    <=> JSON_EXTRACT(NEW.after_lot_json, '$.created_at')) THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'SELL lot immutable fields changed';
            END IF;

            IF (JSON_EXTRACT(NEW.after_lot_json, '$.remaining_quantity') = 0
                AND (JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.after_lot_json, '$.position_state')) <> 'CLOSED'
                    OR JSON_TYPE(JSON_EXTRACT(
                        NEW.after_lot_json, '$.closed_at')) <> 'STRING'
                    OR (SELECT lot.closed_at
                        FROM st_position_lot_v2 lot
                        WHERE BINARY lot.lot_id = BINARY NEW.lot_id)
                        <> NEW.occurred_at))
               OR (JSON_EXTRACT(
                        NEW.after_lot_json, '$.remaining_quantity') > 0
                    AND (NOT (JSON_EXTRACT(
                            NEW.before_lot_json, '$.position_state')
                            <=> JSON_EXTRACT(
                                NEW.after_lot_json, '$.position_state'))
                        OR NOT (JSON_EXTRACT(
                            NEW.before_lot_json, '$.closed_at')
                            <=> JSON_EXTRACT(
                                NEW.after_lot_json, '$.closed_at')))) THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'invalid SELL lot close state';
            END IF;

            SELECT COUNT(*) INTO parent_count
            FROM st_fill_execution_evidence_v2 fe
            INNER JOIN st_market_calendar_evidence_v2 cal
                ON BINARY cal.calendar_evidence_id
                    = BINARY fe.calendar_evidence_id
               AND BINARY cal.evidence_hash
                    = BINARY fe.calendar_evidence_hash
            WHERE BINARY fe.fill_execution_evidence_id
                    = BINARY NEW.fill_execution_evidence_id
              AND cal.trade_date >= STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.before_lot_json, '$.settlement_date')), '%Y-%m-%d');
            IF parent_count <> 1 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'SELL consumed an unsettled lot';
            END IF;

            SELECT COUNT(*) INTO predecessor_count
            FROM st_position_lot_v2 earlier_lot
            INNER JOIN st_fill_accounting_outcome_v2 ao
                ON BINARY ao.accounting_outcome_id
                    = BINARY NEW.accounting_outcome_id
            INNER JOIN st_fill_execution_evidence_v2 fe
                ON BINARY fe.fill_execution_evidence_id
                    = BINARY NEW.fill_execution_evidence_id
            INNER JOIN st_market_calendar_evidence_v2 cal
                ON BINARY cal.calendar_evidence_id
                    = BINARY fe.calendar_evidence_id
               AND BINARY cal.evidence_hash
                    = BINARY fe.calendar_evidence_hash
            WHERE BINARY earlier_lot.account_id = BINARY ao.account_id
              AND BINARY earlier_lot.stock_code = BINARY ao.stock_code
              AND earlier_lot.settlement_date <= cal.trade_date
              AND (earlier_lot.remaining_quantity > 0
                OR (earlier_lot.remaining_quantity = 0
                    AND earlier_lot.closed_at = NEW.occurred_at
                    AND NOT EXISTS (
                        SELECT 1
                        FROM st_lot_transition_evidence_v2 recorded_effect
                        WHERE BINARY recorded_effect.accounting_outcome_id
                                = BINARY NEW.accounting_outcome_id
                          AND BINARY recorded_effect.lot_id
                                = BINARY earlier_lot.lot_id
                          AND recorded_effect.effect_sequence
                                < NEW.effect_sequence)))
              AND (earlier_lot.opened_trade_date
                    < STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(
                        NEW.before_lot_json,
                        '$.opened_trade_date')), '%Y-%m-%d')
                OR (earlier_lot.opened_trade_date
                        = STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(
                            NEW.before_lot_json,
                            '$.opened_trade_date')), '%Y-%m-%d')
                    AND BINARY earlier_lot.lot_id < BINARY NEW.lot_id));
            IF predecessor_count <> 0 THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'SELL skipped an earlier eligible FIFO lot';
            END IF;

            IF NEW.effect_sequence > 0 THEN
                SELECT COUNT(*) INTO predecessor_count
                FROM st_lot_transition_evidence_v2 previous_effect
                WHERE BINARY previous_effect.lot_transition_evidence_id
                        = BINARY NEW.previous_effect_id
                  AND previous_effect.effect_kind = 'SELL_FIFO_CONSUME'
                  AND JSON_EXTRACT(
                        previous_effect.before_lot_json,
                        '$.remaining_quantity')
                        = previous_effect.consumed_quantity
                  AND (
                    JSON_UNQUOTE(JSON_EXTRACT(
                        previous_effect.before_lot_json,
                        '$.opened_trade_date'))
                        < JSON_UNQUOTE(JSON_EXTRACT(
                            NEW.before_lot_json, '$.opened_trade_date'))
                    OR (
                        JSON_UNQUOTE(JSON_EXTRACT(
                            previous_effect.before_lot_json,
                            '$.opened_trade_date'))
                            = JSON_UNQUOTE(JSON_EXTRACT(
                                NEW.before_lot_json,
                                '$.opened_trade_date'))
                        AND BINARY previous_effect.lot_id
                            < BINARY NEW.lot_id));
                IF predecessor_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'SELL lot effects violate FIFO order';
                END IF;
            END IF;

            SELECT ao.total_effect_quantity, ao.lot_effect_count
                INTO expected_total, expected_effect_count
            FROM st_fill_accounting_outcome_v2 ao
            WHERE BINARY ao.accounting_outcome_id
                    = BINARY NEW.accounting_outcome_id;
            SELECT COALESCE(SUM(consumed_quantity), 0)
                INTO prior_consumed
            FROM st_lot_transition_evidence_v2 existing_effect
            WHERE BINARY existing_effect.accounting_outcome_id
                    = BINARY NEW.accounting_outcome_id;
            IF prior_consumed + NEW.consumed_quantity > expected_total
               OR (NEW.effect_sequence = expected_effect_count - 1
                    AND prior_consumed + NEW.consumed_quantity
                        <> expected_total)
               OR (NEW.effect_sequence < expected_effect_count - 1
                    AND prior_consumed + NEW.consumed_quantity
                        >= expected_total) THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'SELL lot effects do not reconcile to fill';
            END IF;
        END IF;
    END
    """,
    "DROP TRIGGER IF EXISTS trg_fill_accounting_finalization_v2_guard_bi",
    """
    CREATE TRIGGER trg_fill_accounting_finalization_v2_guard_bi
    BEFORE INSERT ON st_fill_accounting_outcome_finalization_v2
    FOR EACH ROW
    BEGIN
        DECLARE parent_count INT DEFAULT 0;
        DECLARE total_child_count BIGINT DEFAULT 0;
        DECLARE matching_child_count BIGINT DEFAULT 0;
        DECLARE child_sequence BIGINT DEFAULT 0;
        DECLARE child_id CHAR(64) DEFAULT NULL;
        DECLARE child_hash CHAR(64) DEFAULT NULL;
        DECLARE child_previous_id CHAR(64) DEFAULT NULL;
        DECLARE child_previous_hash CHAR(64) DEFAULT NULL;
        DECLARE child_lot_sequence BIGINT DEFAULT 0;
        DECLARE child_previous_lot_id CHAR(64) DEFAULT NULL;
        DECLARE child_previous_lot_hash CHAR(64) DEFAULT NULL;
        DECLARE child_lot_id VARCHAR(64) DEFAULT NULL;
        DECLARE child_kind VARCHAR(40) DEFAULT NULL;
        DECLARE child_consumed BIGINT DEFAULT 0;
        DECLARE child_before_json LONGTEXT DEFAULT NULL;
        DECLARE child_before_hash CHAR(64) DEFAULT NULL;
        DECLARE child_after_json LONGTEXT DEFAULT NULL;
        DECLARE child_occurred_at DATETIME DEFAULT NULL;
        DECLARE child_bound_at DATETIME DEFAULT NULL;
        DECLARE previous_child_id CHAR(64) DEFAULT NULL;
        DECLARE previous_child_hash CHAR(64) DEFAULT NULL;
        DECLARE previous_fifo_date VARCHAR(10) DEFAULT NULL;
        DECLARE previous_fifo_lot_id VARCHAR(64) DEFAULT NULL;
        DECLARE current_fifo_date VARCHAR(10) DEFAULT NULL;
        DECLARE predecessor_count INT DEFAULT 0;
        DECLARE skipped_count INT DEFAULT 0;
        DECLARE observed_sell_quantity BIGINT DEFAULT 0;
        DECLARE outcome_side VARCHAR(8) DEFAULT NULL;
        DECLARE canonical_effect_hashes LONGTEXT DEFAULT '[';
        DECLARE canonical_preimage LONGTEXT DEFAULT NULL;
        DECLARE recomputed_effects_hash CHAR(64) DEFAULT NULL;
        DECLARE canonical_finalized_at VARCHAR(32) DEFAULT NULL;
        DECLARE canonical_finalization_preimage LONGTEXT DEFAULT NULL;
        DECLARE recomputed_finalization_hash CHAR(64) DEFAULT NULL;

        IF NEW.finalization_status <> 'FINAL'
           OR NEW.lot_effect_count < 1
           OR NEW.total_effect_quantity < 1
           OR JSON_VALID(NEW.effect_hashes_json) <> 1
           OR NEW.history_origin NOT IN
                ('START_AFTER_UNKNOWN', 'COMPLETE_FROM_DECLARED_ORIGIN')
           OR NEW.history_origin_id IS NULL
           OR NEW.history_origin_id = ''
           OR NEW.history_origin_at IS NULL
           OR NEW.history_origin_at > NEW.finalized_at
           OR NEW.finalized_at > NEW.created_at
           OR NEW.authority_status <> 'CONTENT_HASH_ONLY'
           OR NEW.authority_receipt_hash IS NOT NULL THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid accounting finalization core fields';
        END IF;

        IF JSON_TYPE(JSON_EXTRACT(NEW.effect_hashes_json, '$')) <> 'ARRAY'
           OR JSON_LENGTH(NEW.effect_hashes_json) <> NEW.lot_effect_count THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'finalization effect hash array shape mismatch';
        END IF;

        IF CHAR_LENGTH(NEW.finalization_id) <> 64
           OR BINARY NEW.finalization_id
                <> BINARY LOWER(NEW.finalization_id)
           OR NEW.finalization_id REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.accounting_outcome_id) <> 64
           OR BINARY NEW.accounting_outcome_id
                <> BINARY LOWER(NEW.accounting_outcome_id)
           OR NEW.accounting_outcome_id REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.outcome_hash) <> 64
           OR BINARY NEW.outcome_hash <> BINARY LOWER(NEW.outcome_hash)
           OR NEW.outcome_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.fill_execution_evidence_id) <> 64
           OR BINARY NEW.fill_execution_evidence_id
                <> BINARY LOWER(NEW.fill_execution_evidence_id)
           OR NEW.fill_execution_evidence_id REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.fill_execution_evidence_hash) <> 64
           OR BINARY NEW.fill_execution_evidence_hash
                <> BINARY LOWER(NEW.fill_execution_evidence_hash)
           OR NEW.fill_execution_evidence_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.lot_effect_root_hash) <> 64
           OR BINARY NEW.lot_effect_root_hash
                <> BINARY LOWER(NEW.lot_effect_root_hash)
           OR NEW.lot_effect_root_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.lot_effects_hash) <> 64
           OR BINARY NEW.lot_effects_hash
                <> BINARY LOWER(NEW.lot_effects_hash)
           OR NEW.lot_effects_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.provenance_hash) <> 64
           OR BINARY NEW.provenance_hash
                <> BINARY LOWER(NEW.provenance_hash)
           OR NEW.provenance_hash REGEXP '[^0-9a-f]'
           OR CHAR_LENGTH(NEW.finalization_hash) <> 64
           OR BINARY NEW.finalization_hash
                <> BINARY LOWER(NEW.finalization_hash)
           OR NEW.finalization_hash REGEXP '[^0-9a-f]' THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid accounting finalization SHA256';
        END IF;

        IF BINARY NEW.finalization_id <> BINARY NEW.finalization_hash THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'accounting finalization identity mismatch';
        END IF;

        SELECT COUNT(*) INTO parent_count
        FROM st_fill_accounting_outcome_v2 ao
        INNER JOIN st_fill_execution_evidence_v2 fe
            ON BINARY fe.fill_execution_evidence_id
                = BINARY NEW.fill_execution_evidence_id
           AND BINARY fe.fill_id = BINARY NEW.fill_id
           AND BINARY fe.evidence_hash
                = BINARY NEW.fill_execution_evidence_hash
        WHERE BINARY ao.accounting_outcome_id
                = BINARY NEW.accounting_outcome_id
          AND BINARY ao.fill_id = BINARY NEW.fill_id
          AND BINARY ao.outcome_hash = BINARY NEW.outcome_hash
          AND BINARY ao.fill_execution_evidence_id
                = BINARY NEW.fill_execution_evidence_id
          AND BINARY ao.fill_execution_evidence_hash
                = BINARY NEW.fill_execution_evidence_hash
          AND BINARY ao.lot_effect_root_hash
                = BINARY NEW.lot_effect_root_hash
          AND BINARY ao.lot_effects_hash = BINARY NEW.lot_effects_hash
          AND ao.lot_effect_count = NEW.lot_effect_count
          AND ao.total_effect_quantity = NEW.total_effect_quantity
          AND ao.history_origin = NEW.history_origin
          AND ao.history_origin_id <=> NEW.history_origin_id
          AND ao.history_origin_at <=> NEW.history_origin_at
          AND ao.authority_status = NEW.authority_status
          AND BINARY ao.provenance_hash = BINARY NEW.provenance_hash
          AND ao.recorded_at <= NEW.finalized_at
          AND BINARY fe.order_id = BINARY ao.order_id
          AND BINARY fe.account_id = BINARY ao.account_id
          AND BINARY fe.stock_code = BINARY ao.stock_code
          AND fe.history_origin = NEW.history_origin
          AND fe.history_origin_id <=> NEW.history_origin_id
          AND fe.history_origin_at <=> NEW.history_origin_at
          AND fe.authority_status = NEW.authority_status;
        IF parent_count <> 1 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'accounting finalization parent mismatch';
        END IF;

        SELECT ao.side INTO outcome_side
        FROM st_fill_accounting_outcome_v2 ao
        WHERE BINARY ao.accounting_outcome_id
                = BINARY NEW.accounting_outcome_id;
        IF (outcome_side = 'BUY' AND NEW.lot_effect_count <> 1)
           OR outcome_side NOT IN ('BUY', 'SELL') THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'invalid finalized accounting side shape';
        END IF;

        SELECT COUNT(*) INTO total_child_count
        FROM st_lot_transition_evidence_v2 effect
        WHERE BINARY effect.accounting_outcome_id
                = BINARY NEW.accounting_outcome_id;
        SELECT COUNT(*) INTO matching_child_count
        FROM st_lot_transition_evidence_v2 effect
        WHERE BINARY effect.accounting_outcome_id
                = BINARY NEW.accounting_outcome_id
          AND BINARY effect.fill_id = BINARY NEW.fill_id
          AND BINARY effect.fill_execution_evidence_id
                = BINARY NEW.fill_execution_evidence_id
          AND BINARY effect.fill_execution_evidence_hash
                = BINARY NEW.fill_execution_evidence_hash
          AND BINARY effect.lot_effect_root_hash
                = BINARY NEW.lot_effect_root_hash
          AND effect.effect_sequence >= 0
          AND effect.effect_sequence < NEW.lot_effect_count
          AND effect.history_origin = NEW.history_origin
          AND effect.history_origin_id <=> NEW.history_origin_id
          AND effect.history_origin_at <=> NEW.history_origin_at
          AND effect.authority_status = NEW.authority_status
          AND BINARY effect.provenance_hash = BINARY NEW.provenance_hash
          AND effect.bound_at <= NEW.finalized_at;
        IF total_child_count <> NEW.lot_effect_count
           OR matching_child_count <> NEW.lot_effect_count THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'accounting finalization child set mismatch';
        END IF;

        WHILE child_sequence < NEW.lot_effect_count DO
            IF JSON_TYPE(JSON_EXTRACT(
                    NEW.effect_hashes_json,
                    CONCAT('$[', child_sequence, ']'))) <> 'STRING' THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'finalization effect hash must be JSON string';
            END IF;

            SELECT effect.lot_transition_evidence_id,
                   effect.effect_hash,
                   effect.previous_effect_id,
                   effect.previous_effect_hash,
                   effect.lot_transition_sequence,
                   effect.previous_lot_transition_id,
                   effect.previous_lot_transition_hash,
                   effect.lot_id,
                   effect.effect_kind,
                   effect.consumed_quantity,
                   effect.before_lot_json,
                   effect.before_lot_hash,
                   effect.after_lot_json,
                   effect.occurred_at,
                   effect.bound_at
                INTO child_id, child_hash,
                     child_previous_id, child_previous_hash,
                     child_lot_sequence,
                     child_previous_lot_id, child_previous_lot_hash,
                     child_lot_id, child_kind, child_consumed,
                     child_before_json, child_before_hash,
                     child_after_json, child_occurred_at, child_bound_at
            FROM st_lot_transition_evidence_v2 effect
            WHERE BINARY effect.accounting_outcome_id
                    = BINARY NEW.accounting_outcome_id
              AND effect.effect_sequence = child_sequence;

            IF BINARY JSON_UNQUOTE(JSON_EXTRACT(
                    NEW.effect_hashes_json,
                    CONCAT('$[', child_sequence, ']')))
                    <> BINARY child_hash
               OR CHAR_LENGTH(child_hash) <> 64
               OR BINARY child_hash <> BINARY LOWER(child_hash)
               OR child_hash REGEXP '[^0-9a-f]'
               OR BINARY child_id <> BINARY child_hash THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'finalization effect hash item mismatch';
            END IF;

            SET canonical_effect_hashes = CONCAT(
                canonical_effect_hashes,
                IF(child_sequence = 0, '', ','),
                '"', child_hash, '"');

            IF child_sequence = 0 THEN
                IF child_previous_id IS NOT NULL
                   OR child_previous_hash IS NOT NULL THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'finalized effect genesis mismatch';
                END IF;
            ELSEIF child_previous_id IS NULL
               OR child_previous_hash IS NULL
               OR BINARY child_previous_id <> BINARY previous_child_id
               OR BINARY child_previous_hash <> BINARY previous_child_hash THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'finalized per-fill effect chain mismatch';
            END IF;

            IF child_lot_sequence = 0 THEN
                IF child_previous_lot_id IS NOT NULL
                   OR child_previous_lot_hash IS NOT NULL
                   OR (NEW.history_origin = 'COMPLETE_FROM_DECLARED_ORIGIN'
                       AND child_kind = 'SELL_FIFO_CONSUME') THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'finalized per-lot genesis mismatch';
                END IF;
            ELSE
                SELECT COUNT(*) INTO predecessor_count
                FROM st_lot_transition_evidence_v2 previous_lot
                WHERE BINARY previous_lot.lot_transition_evidence_id
                        = BINARY child_previous_lot_id
                  AND BINARY previous_lot.effect_hash
                        = BINARY child_previous_lot_hash
                  AND BINARY previous_lot.lot_id = BINARY child_lot_id
                  AND previous_lot.lot_transition_sequence + 1
                        = child_lot_sequence
                  AND BINARY previous_lot.after_lot_hash
                        = BINARY child_before_hash
                  AND previous_lot.bound_at <= child_occurred_at
                  AND previous_lot.history_origin = NEW.history_origin
                  AND previous_lot.history_origin_id <=> NEW.history_origin_id
                  AND previous_lot.history_origin_at <=> NEW.history_origin_at;
                IF predecessor_count <> 1 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'finalized per-lot chain mismatch';
                END IF;
            END IF;

            IF outcome_side = 'BUY' THEN
                IF child_sequence <> 0
                   OR child_kind <> 'BUY_CREATE'
                   OR child_consumed <> 0
                   OR child_before_json IS NOT NULL
                   OR JSON_EXTRACT(
                        child_after_json, '$.original_quantity')
                        <> NEW.total_effect_quantity
                   OR JSON_EXTRACT(
                        child_after_json, '$.remaining_quantity')
                        <> NEW.total_effect_quantity THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'finalized BUY child mismatch';
                END IF;
            ELSE
                IF child_kind <> 'SELL_FIFO_CONSUME'
                   OR child_before_json IS NULL
                   OR child_consumed < 1
                   OR child_consumed > JSON_EXTRACT(
                        child_before_json, '$.remaining_quantity')
                   OR (child_sequence < NEW.lot_effect_count - 1
                       AND child_consumed <> JSON_EXTRACT(
                            child_before_json, '$.remaining_quantity')) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'finalized SELL child quantity mismatch';
                END IF;

                SET current_fifo_date = JSON_UNQUOTE(JSON_EXTRACT(
                    child_before_json, '$.opened_trade_date'));
                IF child_sequence > 0 AND (
                    previous_fifo_date > current_fifo_date
                    OR (previous_fifo_date = current_fifo_date
                        AND BINARY previous_fifo_lot_id
                            >= BINARY child_lot_id)) THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'finalized SELL FIFO order mismatch';
                END IF;

                SELECT COUNT(*) INTO skipped_count
                FROM st_position_lot_v2 earlier_lot
                INNER JOIN st_fill_accounting_outcome_v2 ao
                    ON BINARY ao.accounting_outcome_id
                        = BINARY NEW.accounting_outcome_id
                INNER JOIN st_fill_execution_evidence_v2 fe
                    ON BINARY fe.fill_execution_evidence_id
                        = BINARY NEW.fill_execution_evidence_id
                INNER JOIN st_market_calendar_evidence_v2 cal
                    ON BINARY cal.calendar_evidence_id
                        = BINARY fe.calendar_evidence_id
                   AND BINARY cal.evidence_hash
                        = BINARY fe.calendar_evidence_hash
                WHERE BINARY earlier_lot.account_id = BINARY ao.account_id
                  AND BINARY earlier_lot.stock_code = BINARY ao.stock_code
                  AND earlier_lot.settlement_date <= cal.trade_date
                  AND (earlier_lot.remaining_quantity > 0
                    OR (earlier_lot.remaining_quantity = 0
                        AND earlier_lot.closed_at = child_occurred_at
                        AND NOT EXISTS (
                            SELECT 1
                            FROM st_lot_transition_evidence_v2 recorded_effect
                            WHERE BINARY recorded_effect.accounting_outcome_id
                                    = BINARY NEW.accounting_outcome_id
                              AND BINARY recorded_effect.lot_id
                                    = BINARY earlier_lot.lot_id
                              AND recorded_effect.effect_sequence
                                    < child_sequence)))
                  AND (earlier_lot.opened_trade_date
                        < STR_TO_DATE(current_fifo_date, '%Y-%m-%d')
                    OR (earlier_lot.opened_trade_date
                            = STR_TO_DATE(current_fifo_date, '%Y-%m-%d')
                        AND BINARY earlier_lot.lot_id
                            < BINARY child_lot_id));
                IF skipped_count <> 0 THEN
                    SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'finalized SELL skipped FIFO lot';
                END IF;

                SET observed_sell_quantity =
                    observed_sell_quantity + child_consumed;
                SET previous_fifo_date = current_fifo_date;
                SET previous_fifo_lot_id = child_lot_id;
            END IF;

            SET previous_child_id = child_id;
            SET previous_child_hash = child_hash;
            SET child_sequence = child_sequence + 1;
        END WHILE;

        SET canonical_effect_hashes = CONCAT(canonical_effect_hashes, ']');
        IF BINARY NEW.effect_hashes_json
                <> BINARY canonical_effect_hashes THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'effect hash array is not canonical';
        END IF;

        SET canonical_preimage = CONCAT(
            '{"namespace":"trading-v2.lot-accounting-effect-list.v1",',
            '"payload":{"effect_hashes":', canonical_effect_hashes,
            ',"root_hash":"', NEW.lot_effect_root_hash, '"}}');
        SET recomputed_effects_hash = LOWER(SHA2(canonical_preimage, 256));
        IF BINARY recomputed_effects_hash <> BINARY NEW.lot_effects_hash THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'finalized lot effect list hash mismatch';
        END IF;

        IF outcome_side = 'SELL'
           AND observed_sell_quantity <> NEW.total_effect_quantity THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'finalized SELL quantity mismatch';
        END IF;

        SET canonical_finalized_at = DATE_FORMAT(
            CONVERT_TZ(NEW.finalized_at, '+08:00', '+00:00'),
            '%Y-%m-%dT%H:%i:%s.%f+00:00');
        SET canonical_finalization_preimage = CONCAT(
            '{"namespace":"trading-v2.fill-accounting-finalization.v1",',
            '"payload":{"accounting_outcome_id":',
            JSON_QUOTE(NEW.accounting_outcome_id),
            ',"effect_hashes":', canonical_effect_hashes,
            ',"fill_execution_evidence_hash":',
            JSON_QUOTE(NEW.fill_execution_evidence_hash),
            ',"fill_execution_evidence_id":',
            JSON_QUOTE(NEW.fill_execution_evidence_id),
            ',"fill_id":', JSON_QUOTE(NEW.fill_id),
            ',"finalization_status":',
            JSON_QUOTE(NEW.finalization_status),
            ',"finalized_at":', JSON_QUOTE(canonical_finalized_at),
            ',"lot_effect_count":',
            CAST(NEW.lot_effect_count AS CHAR),
            ',"lot_effect_root_hash":',
            JSON_QUOTE(NEW.lot_effect_root_hash),
            ',"lot_effects_hash":', JSON_QUOTE(NEW.lot_effects_hash),
            ',"outcome_hash":', JSON_QUOTE(NEW.outcome_hash),
            ',"provenance_hash":', JSON_QUOTE(NEW.provenance_hash),
            ',"total_effect_quantity":',
            CAST(NEW.total_effect_quantity AS CHAR), '}}');
        SET recomputed_finalization_hash = LOWER(
            SHA2(canonical_finalization_preimage, 256));
        IF BINARY recomputed_finalization_hash
                <> BINARY NEW.finalization_hash
           OR BINARY recomputed_finalization_hash
                <> BINARY NEW.finalization_id THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'accounting finalization content hash mismatch';
        END IF;
    END
    """,
)


ACCOUNTING_EVIDENCE_APPEND_ONLY_GUARD_PROPOSAL: tuple[str, ...] = (
    "DROP TRIGGER IF EXISTS trg_fill_accounting_outcome_v2_guard_bu",
    """
    CREATE TRIGGER trg_fill_accounting_outcome_v2_guard_bu
    BEFORE UPDATE ON st_fill_accounting_outcome_v2
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'fill accounting outcome is append only';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_fill_accounting_outcome_v2_guard_bd",
    """
    CREATE TRIGGER trg_fill_accounting_outcome_v2_guard_bd
    BEFORE DELETE ON st_fill_accounting_outcome_v2
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'fill accounting outcome cannot be deleted';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_lot_transition_evidence_v2_guard_bu",
    """
    CREATE TRIGGER trg_lot_transition_evidence_v2_guard_bu
    BEFORE UPDATE ON st_lot_transition_evidence_v2
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'lot transition evidence is append only';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_lot_transition_evidence_v2_guard_bd",
    """
    CREATE TRIGGER trg_lot_transition_evidence_v2_guard_bd
    BEFORE DELETE ON st_lot_transition_evidence_v2
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'lot transition evidence cannot be deleted';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_fill_accounting_finalization_v2_guard_bu",
    """
    CREATE TRIGGER trg_fill_accounting_finalization_v2_guard_bu
    BEFORE UPDATE ON st_fill_accounting_outcome_finalization_v2
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'accounting finalization is append only';
    END
    """,
    "DROP TRIGGER IF EXISTS trg_fill_accounting_finalization_v2_guard_bd",
    """
    CREATE TRIGGER trg_fill_accounting_finalization_v2_guard_bd
    BEFORE DELETE ON st_fill_accounting_outcome_finalization_v2
    FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'accounting finalization cannot be deleted';
    END
    """,
)


FINALIZED_ACCOUNTING_OUTCOME_READ_SQL = """
SELECT outcome.*
FROM st_fill_accounting_outcome_v2 outcome
INNER JOIN st_fill_accounting_outcome_finalization_v2 finalization
    ON BINARY finalization.accounting_outcome_id
        = BINARY outcome.accounting_outcome_id
   AND BINARY finalization.fill_id = BINARY outcome.fill_id
   AND BINARY finalization.outcome_hash = BINARY outcome.outcome_hash
   AND BINARY finalization.fill_execution_evidence_id
        = BINARY outcome.fill_execution_evidence_id
   AND BINARY finalization.fill_execution_evidence_hash
        = BINARY outcome.fill_execution_evidence_hash
   AND BINARY finalization.lot_effect_root_hash
        = BINARY outcome.lot_effect_root_hash
   AND BINARY finalization.lot_effects_hash
        = BINARY outcome.lot_effects_hash
   AND finalization.lot_effect_count = outcome.lot_effect_count
   AND finalization.total_effect_quantity = outcome.total_effect_quantity
   AND BINARY finalization.provenance_hash = BINARY outcome.provenance_hash
WHERE BINARY finalization.finalization_status = BINARY 'FINAL'
"""


ACCOUNTING_EVIDENCE_ALL_DDL_PROPOSAL = (
    ACCOUNTING_EVIDENCE_TABLE_DDL_PROPOSAL
    + ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL
    + ACCOUNTING_EVIDENCE_APPEND_ONLY_GUARD_PROPOSAL
)


__all__ = [
    "ACCOUNTING_EVIDENCE_ALL_DDL_PROPOSAL",
    "ACCOUNTING_EVIDENCE_APPEND_ONLY_GUARD_PROPOSAL",
    "ACCOUNTING_EVIDENCE_DDL_IS_REGISTERED",
    "ACCOUNTING_EVIDENCE_INSERT_GUARD_PROPOSAL",
    "ACCOUNTING_EVIDENCE_MIGRATION_VERSION_PROPOSAL",
    "ACCOUNTING_EVIDENCE_TABLE_DDL_PROPOSAL",
    "FINALIZED_ACCOUNTING_OUTCOME_READ_SQL",
]
