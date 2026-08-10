import importlib
import os
import sys
from unittest.mock import patch


def test_sync_concept_ths_uses_shared_tls_capable_engine_factory():
    sys.modules.pop("tools.sync_concept_ths", None)
    with patch.dict(
        os.environ,
        {"MYSQL_URL": "mysql+pymysql://runtime:secret@127.0.0.1:13306/probiga"},
    ), patch("server.common.batch_db.create_batch_engine") as factory:
        module = importlib.import_module("tools.sync_concept_ths")

    factory.assert_called_once_with(module.mysql_url)
