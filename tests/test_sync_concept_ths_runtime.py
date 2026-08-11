import importlib
import os
import sys
from unittest.mock import patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, text


def test_sync_concept_ths_uses_shared_tls_capable_engine_factory():
    sys.modules.pop("tools.sync_concept_ths", None)
    with patch.dict(
        os.environ,
        {"MYSQL_URL": "mysql+pymysql://runtime:secret@127.0.0.1:13306/probiga"},
    ), patch("server.common.batch_db.create_batch_engine") as factory:
        module = importlib.import_module("tools.sync_concept_ths")

    factory.assert_called_once_with(module.mysql_url)


def _load_module():
    sys.modules.pop("tools.sync_concept_ths", None)
    with patch.dict(
        os.environ,
        {"MYSQL_URL": "mysql+pymysql://runtime:secret@127.0.0.1:13306/probiga"},
    ), patch("server.common.batch_db.create_batch_engine"):
        return importlib.import_module("tools.sync_concept_ths")


def test_concept_refresh_rolls_back_delete_when_insert_fails():
    module = _load_module()
    test_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with test_engine.begin() as conn:
        conn.execute(text("CREATE TABLE si_concept_code_ths (name TEXT)"))
        conn.execute(text("INSERT INTO si_concept_code_ths (name) VALUES ('old')"))

    with pytest.raises(Exception):
        module.replace_table_transactionally(
            test_engine,
            pd.DataFrame([{"unexpected_column": "new"}]),
            "si_concept_code_ths",
        )

    with test_engine.connect() as conn:
        assert conn.execute(text("SELECT name FROM si_concept_code_ths")).scalar() == "old"


def test_concept_refresh_refuses_to_replace_with_empty_dataset():
    module = _load_module()
    test_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with pytest.raises(ValueError, match="empty dataset"):
        module.replace_table_transactionally(
            test_engine,
            pd.DataFrame(),
            "si_concept_constituent_ths",
        )
