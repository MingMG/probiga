# -*- coding: utf-8 -*-
import os
import unittest
from unittest.mock import patch

from tools import jq_config


class JQConfigTest(unittest.TestCase):
    def setUp(self):
        jq_config._authed = False
        jq_config._jq_module = None

    def test_missing_credentials_raise_clear_error(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                jq_config._jq_credentials()
        self.assertIn("JQ_PHONE", str(ctx.exception))

    def test_missing_package_can_be_queried_as_optional(self):
        with patch("tools.jq_config.importlib.import_module", side_effect=ModuleNotFoundError):
            self.assertIsNone(jq_config.get_jq_client(required=False))


if __name__ == "__main__":
    unittest.main()
