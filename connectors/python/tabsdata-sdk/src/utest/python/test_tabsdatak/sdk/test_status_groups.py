#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import tabsdatak.sdk as sdk
from tabsdatak.sdk.status_utils.data_version import DATA_VERSION_STATUS_MAPPING
from tabsdatak.sdk.status_utils.execution import EXECUTION_STATUS_MAPPING
from tabsdatak.sdk.status_utils.function_run import FUNCTION_RUN_STATUS_MAPPING
from tabsdatak.sdk.status_utils.groups import (
    FINAL,
    STATUS_GROUPS,
    Entity,
    Health,
    annotate,
    health,
    is_final,
    status_groups_document,
    status_name,
)
from tabsdatak.sdk.status_utils.transaction import TRANSACTION_STATUS_MAPPING
from tabsdatak.sdk.status_utils.worker import WORKER_STATUS_MAPPING

_ASSET = Path(sdk.__file__).parent / "resources" / "status_groups.v1.json"

_SERVER_CODES = {
    Entity.FUNCTION_RUN: {"S", "RR", "RS", "R", "D", "E", "F", "H", "C", "X", "Y"},
    Entity.DATA_VERSION: {"S", "RR", "RS", "R", "D", "E", "F", "H", "C", "X", "Y"},
    Entity.EXECUTION: {"S", "R", "L", "F", "U"},
    Entity.TRANSACTION: {"S", "R", "L", "C", "X", "Y", "U"},
    Entity.WORKER: {"S", "RR", "R", "D", "SPS", "SPR", "SP", "E", "F", "X"},
}

_CANONICAL = [
    (Entity.FUNCTION_RUN, "S", Health.RUNNING, False),
    (Entity.FUNCTION_RUN, "RR", Health.RUNNING, False),
    (Entity.FUNCTION_RUN, "RS", Health.RUNNING, False),
    (Entity.FUNCTION_RUN, "R", Health.RUNNING, False),
    (Entity.FUNCTION_RUN, "D", Health.RUNNING, False),
    (Entity.FUNCTION_RUN, "E", Health.WARNING, False),
    (Entity.FUNCTION_RUN, "F", Health.ERROR, True),
    (Entity.FUNCTION_RUN, "H", Health.WARNING, True),
    (Entity.FUNCTION_RUN, "Y", Health.WARNING, True),
    (Entity.FUNCTION_RUN, "C", Health.OK, True),
    (Entity.FUNCTION_RUN, "X", Health.OK, True),
    (Entity.EXECUTION, "S", Health.RUNNING, False),
    (Entity.EXECUTION, "R", Health.RUNNING, False),
    (Entity.EXECUTION, "F", Health.OK, True),
    (Entity.EXECUTION, "L", Health.ERROR, True),
    (Entity.EXECUTION, "U", Health.ERROR, True),
    (Entity.TRANSACTION, "S", Health.RUNNING, False),
    (Entity.TRANSACTION, "R", Health.RUNNING, False),
    (Entity.TRANSACTION, "C", Health.OK, True),
    (Entity.TRANSACTION, "X", Health.OK, True),
    (Entity.TRANSACTION, "Y", Health.WARNING, True),
    (Entity.TRANSACTION, "L", Health.ERROR, True),
    (Entity.TRANSACTION, "U", Health.ERROR, True),
    (Entity.WORKER, "S", Health.RUNNING, False),
    (Entity.WORKER, "RR", Health.RUNNING, False),
    (Entity.WORKER, "R", Health.RUNNING, False),
    (Entity.WORKER, "SPS", Health.RUNNING, False),
    (Entity.WORKER, "SPR", Health.RUNNING, False),
    (Entity.WORKER, "D", Health.OK, True),
    (Entity.WORKER, "SP", Health.OK, True),
    (Entity.WORKER, "X", Health.OK, True),
    (Entity.WORKER, "E", Health.WARNING, False),
    (Entity.WORKER, "F", Health.ERROR, True),
]


@pytest.mark.parametrize("entity, code, expected_health, final", _CANONICAL)
def test_canonical_table(entity, code, expected_health, final):
    assert health(entity, code) is expected_health
    assert is_final(entity, code) is final


@pytest.mark.parametrize("entity", list(Entity))
def test_every_server_code_is_covered(entity):
    grouped = set().union(*STATUS_GROUPS[entity].values())
    assert grouped == _SERVER_CODES[entity]
    assert FINAL[entity] <= grouped


@pytest.mark.parametrize(
    "entity, mapping",
    [
        (Entity.FUNCTION_RUN, FUNCTION_RUN_STATUS_MAPPING),
        (Entity.DATA_VERSION, DATA_VERSION_STATUS_MAPPING),
        (Entity.EXECUTION, EXECUTION_STATUS_MAPPING),
        (Entity.TRANSACTION, TRANSACTION_STATUS_MAPPING),
        (Entity.WORKER, WORKER_STATUS_MAPPING),
    ],
)
def test_status_mapping_matches_server_codes(entity, mapping):
    assert set(mapping) == _SERVER_CODES[entity]


def test_names_and_codes_are_both_accepted():
    assert health("function_run", "Failed") is Health.ERROR
    assert health("execution", "Finished") is Health.OK
    assert status_name(Entity.EXECUTION, "F") == "Finished"
    assert status_name(Entity.FUNCTION_RUN, "F") == "Failed"
    assert is_final(Entity.WORKER, "Done")


def test_unknown_status():
    assert health(Entity.EXECUTION, "Q") is None
    assert health(Entity.EXECUTION, None) is None
    assert not is_final(Entity.EXECUTION, None)


def test_annotate_does_not_mutate():
    row = {"id": "1", "status": "L"}
    annotated = annotate(Entity.EXECUTION, row)
    assert row == {"id": "1", "status": "L"}
    assert annotated == {
        "id": "1",
        "status": "L",
        "status_name": "Stalled",
        "health": "error",
        "final": True,
    }


def test_annotate_names_the_run_counts_of_a_plan():
    row = {"status": "R", "function_run_status_count": {"H": 1, "S": 3, "R": 0}}
    assert annotate(Entity.EXECUTION, row)["function_runs_by_status"] == {
        "On Hold": 1,
        "Scheduled": 3,
    }
    assert "function_runs_by_status" not in annotate(Entity.FUNCTION_RUN, {})


def test_ui_asset_is_current():
    document = status_groups_document()
    if os.environ.get("TD_SNAPSHOT_UPDATE") == "1":
        _ASSET.write_text(json.dumps(document, indent=2) + "\n")
    assert json.loads(_ASSET.read_text()) == document, (
        "Regenerate with TD_SNAPSHOT_UPDATE=1."
    )
