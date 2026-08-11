from __future__ import annotations

import pytest

from tools.mysql84_bulk_transform import BulkTransformError, transform_dump_lines


def _run(payload: bytes, requested: tuple[str, ...] = ("probiga.t",)) -> tuple[bytes, object]:
    stream, stats = transform_dump_lines(payload.splitlines(keepends=True), requested)
    return b"".join(stream), stats


def test_defer_secondary_indexes_rewrites_create_and_adds_exact_definitions():
    payload = b"""USE `probiga`;
CREATE TABLE `t` (
  `id` bigint NOT NULL,
  `code` varchar(16) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_code` (`code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
LOCK TABLES `t` WRITE;
ALTER TABLE `t` DISABLE KEYS;
INSERT INTO `t` VALUES (1,'A'),(2,'B');
ALTER TABLE `t` ENABLE KEYS;
UNLOCK TABLES;
"""

    transformed, stats = _run(payload)

    create_part = transformed.split(b"LOCK TABLES", 1)[0]
    assert b"KEY `idx_code`" not in create_part
    assert b"PRIMARY KEY (`id`)" in transformed
    assert (
        b"ALTER TABLE `probiga`.`t` ADD KEY `idx_code` (`code`) USING BTREE;\n"
        in transformed
    )
    assert stats.matched_names == {"probiga.t"}
    assert stats.added_index_statements == 1


def test_unqualified_request_matches_current_use_schema():
    payload = b"""USE `probiga`;
CREATE TABLE `t` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_id` (`id`)
) ENGINE=InnoDB;
UNLOCK TABLES;
"""
    transformed, stats = _run(payload, ("t",))
    assert b"ADD KEY `idx_id`" in transformed
    assert stats.matched_names == {"probiga.t"}


def test_requested_table_without_secondary_index_fails_closed():
    payload = b"""USE `probiga`;
CREATE TABLE `t` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;
"""
    stream, _stats = transform_dump_lines(payload.splitlines(keepends=True), ("probiga.t",))
    with pytest.raises(BulkTransformError, match="no removable secondary indexes"):
        b"".join(stream)


def test_incomplete_target_data_fails_closed():
    payload = b"""USE `probiga`;
CREATE TABLE `t` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_id` (`id`)
) ENGINE=InnoDB;
LOCK TABLES `t` WRITE;
"""
    stream, _stats = transform_dump_lines(payload.splitlines(keepends=True), ("probiga.t",))
    with pytest.raises(BulkTransformError, match="before deferred table data"):
        b"".join(stream)


def test_all_sentinel_defers_every_table_with_secondary_indexes():
    payload = b"""USE `probiga`;
CREATE TABLE `one` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_one` (`id`)
) ENGINE=InnoDB;
UNLOCK TABLES;
CREATE TABLE `two` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_two` (`id`)
) ENGINE=InnoDB;
UNLOCK TABLES;
"""
    transformed, stats = _run(payload, ("all",))
    assert b"ADD KEY `idx_one`" in transformed
    assert b"ADD UNIQUE KEY `uk_two`" in transformed
    assert stats.defer_all is True
    assert stats.matched_names == {"probiga.one", "probiga.two"}


def test_constraints_after_secondary_indexes_are_preserved():
    payload = b"""USE `probiga`;
CREATE TABLE `child` (
  `id` int NOT NULL,
  `parent_id` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_parent` (`parent_id`),
  CONSTRAINT `fk_parent` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`)
) ENGINE=InnoDB;
UNLOCK TABLES;
"""
    transformed, stats = _run(payload, ("probiga.child",))
    create_part = transformed.split(b"UNLOCK TABLES", 1)[0]
    assert b"CONSTRAINT `fk_parent`" in create_part
    assert b"ADD KEY `ix_parent`" in transformed
    assert stats.matched_names == {"probiga.child"}
