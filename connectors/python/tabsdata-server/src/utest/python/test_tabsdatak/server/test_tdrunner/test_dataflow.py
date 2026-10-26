#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._commons.run.dataflow import (
    CONTAINER_ROOT,
    POLARS_OOC_MEMORY_BUDGET_FRACTION,
    POLARS_OOC_SPILL_DIR,
    POLARS_TEMP_DIR,
    TD_DATAFLOW__SPILL__MEMORY_THRESHOLD_PERCENT,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

PROBE = """
import json
import os
import sys

import tabsdatak.server._tdrunner

polars_loaded_first = "polars" in sys.modules

import tabsdatak.server._tdrunner.function.commands.run_function

names = {names}
print(json.dumps({{
    "polars_loaded_first": polars_loaded_first,
    "polars_loaded": "polars" in sys.modules,
    "environment": {{name: os.environ.get(name) for name in names}},
}}))
"""


def probe(environment: dict[str, str], names: list[str]) -> dict:
    completed = subprocess.run(
        [sys.executable, "-c", PROBE.format(names=names)],
        env={**os.environ, **environment},
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_a_runner_command_starts_with_the_dataflow_settings_applied(tmp_path: Path):
    names = [POLARS_TEMP_DIR, POLARS_OOC_SPILL_DIR, POLARS_OOC_MEMORY_BUDGET_FRACTION]

    outcome = probe(
        {
            CONTAINER_ROOT: str(tmp_path),
            TD_DATAFLOW__SPILL__MEMORY_THRESHOLD_PERCENT: "30",
        },
        names,
    )

    assert not outcome["polars_loaded_first"]
    assert outcome["polars_loaded"]
    assert outcome["environment"] == {
        POLARS_TEMP_DIR: str(tmp_path / "tabsdata/dataflow/temporary"),
        POLARS_OOC_SPILL_DIR: str(tmp_path / "tabsdata/dataflow/spill"),
        POLARS_OOC_MEMORY_BUDGET_FRACTION: "0.3",
    }
