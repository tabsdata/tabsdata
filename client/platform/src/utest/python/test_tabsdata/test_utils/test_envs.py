#
# Copyright 2026 Tabs Data Inc.
#

import pytest

from tabsdata._utils.envs import is_env_enabled

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


class TestIsEnvEnabled:
    def test_true_values(self, monkeypatch):
        for val in ["1", "true", "True", "TRUE", "yes", "Yes", "y", "Y", "on", "ON"]:
            monkeypatch.setenv("TEST_ENV", val)
            assert is_env_enabled("TEST_ENV") is True, f"Failed for {val}"

    def test_false_values(self, monkeypatch):
        for val in ["0", "false", "False", "no", "off", "", "random"]:
            monkeypatch.setenv("TEST_ENV", val)
            assert is_env_enabled("TEST_ENV") is False, f"Failed for {val}"

    def test_unset_returns_false(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT_ENV_VAR_12345", raising=False)
        assert is_env_enabled("NONEXISTENT_ENV_VAR_12345") is False
