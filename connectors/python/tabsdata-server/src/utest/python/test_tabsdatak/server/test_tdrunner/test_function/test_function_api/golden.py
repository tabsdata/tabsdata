#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import os
from datetime import date, datetime, time

import polars as pl
import pytest

from tabsdatak.extensions.variant.edition import EDITION
from test_te_variant import function_api as edition

from .driver import Run

UPDATE_VARIABLE = "TD_GOLDEN_UPDATE"

RANDOM = "<random>"
POSITION = "<position>"
MASKED = "<masked>"
ROOT = "<root>"

MASKED_KEYS = {"start", "end", "data_bytes", "error_message"}


def check_edition() -> None:
    assert edition.EDITION == EDITION, (
        f"The installed edition is {EDITION}, but the function API goldens on "
        f"the path are {edition.EDITION}; run the tests from the checkout of "
        f"the installed edition"
    )


def plain(value: object, roots: tuple[str, ...]) -> object:
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    if isinstance(value, str):
        for root in roots:
            value = value.replace(root, ROOT)
        return value.replace(os.sep, "/") if ROOT in value else value
    if isinstance(value, list):
        return [plain(item, roots) for item in value]
    if isinstance(value, dict):
        return {key: plain(item, roots) for key, item in value.items()}
    return value


def table(frame: pl.DataFrame, roots: tuple[str, ...]) -> dict:
    rows = []
    for row in frame.to_dicts():
        for column in edition.RANDOM_COLUMNS:
            if column in row:
                row[column] = RANDOM
        for column in edition.POSITION_COLUMNS:
            if column in row:
                row[column] = POSITION
        rows.append(plain(row, roots))
    return {
        "columns": [[name, str(data_type)] for name, data_type in frame.schema.items()],
        "rows": sorted(rows, key=lambda row: json.dumps(row, sort_keys=True)),
    }


def masked(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: MASKED if key in MASKED_KEYS else masked(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [masked(item) for item in value]
    return value


def snapshot(run: Run) -> dict:
    roots = (str(run.root.resolve()), str(run.root))
    result = {
        "outputs": {
            name: table(frame, roots) for name, frame in sorted(run.outputs.items())
        },
        "response": masked(plain(run.response, roots)),
        "callbacks": masked(plain(run.callbacks, roots)),
    }
    if run.delivered is not None:
        result["delivered"] = table(run.delivered, roots)
    return result


def check_system_columns(run: Run) -> None:
    for name, frame in run.outputs.items():
        for column in edition.RANDOM_COLUMNS:
            if column not in frame.columns:
                continue
            assert frame[column].null_count() == 0, (
                f"Table {name} has null values in {column}"
            )
            assert frame[column].n_unique() == frame.height, (
                f"Table {name} repeats values in {column}"
            )
        for column in edition.POSITION_COLUMNS:
            if column not in frame.columns:
                continue
            assert sorted(frame[column].to_list()) == list(range(frame.height)), (
                f"Table {name} does not number its rows 0 to n-1 in {column}"
            )


def compare(name: str, result: dict) -> None:
    check_edition()
    path = edition.GOLDENS / f"{name}.json"
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if os.environ.get(UPDATE_VARIABLE) == "1":
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered)
        return
    if not path.exists():
        pytest.fail(
            f"No {EDITION} golden for {name} ({path}); generate it in an "
            f"{EDITION} environment with {UPDATE_VARIABLE}=1 and review the file"
        )
    assert json.loads(path.read_text()) == json.loads(rendered), (
        f"The {name} run no longer matches its {EDITION} golden ({path}); if "
        f"the change is intended, regenerate it with {UPDATE_VARIABLE}=1 and "
        f"review the diff"
    )
