#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging

import pytest

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.fixture(autouse=True)
def _neutralize_ai_agent_env(monkeypatch):
    # The CLI refuses to run inside an AI agent (tabsdatak.cli.td.cli); clear the
    # agent-marker env vars so the unit suite is hermetic even when it is itself
    # run from within an agent. Tests that exercise the guard set them explicitly.
    for var in ("GEMINI_CLI", "CLAUDECODE", "CODEX_THREAD_ID", "TDK_IN_AGENT"):
        monkeypatch.delenv(var, raising=False)

from test_tabsdatak.bootest import enrich_sys_path
from test_tabsdatak.cli.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

# noinspection PyUnusedImports
from test_tabsdata.conftest import (
    clean_everything,
    clean_python_virtual_environments,
    pytest_addoption,
    pytest_generate_tests,
    setup_temp_folder,
    setup_temp_folder_node,
)
