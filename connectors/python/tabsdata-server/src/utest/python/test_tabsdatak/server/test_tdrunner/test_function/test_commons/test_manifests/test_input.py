#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest
import yaml

from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputEntry,
    InputManifest,
    InputRecord,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

TRIGGERED_ON = 1780000000100

VERSIONED = InputRecord(
    file="/a.parquet",
    input_idx=0,
    version_pos=0,
    execution_id="E",
    transaction_id="T",
    physical_table_data_version_id="V",
    triggered_on=TRIGGERED_ON,
)


def test_write_produces_the_inputs_document(tmp_path):
    path = tmp_path / "inputs.yaml"
    InputManifest(
        inputs=(
            InputEntry(
                name="a",
                table_pos=0,
                tables=(
                    InputRecord(file="/a.parquet", metadata={"k": "v"}, input_idx=3),
                    InputRecord(),
                ),
            ),
            InputEntry(name="b", table_pos=1, is_empty=True),
        )
    ).write(path)
    assert yaml.safe_load(path.read_text()) == {
        "inputs": [
            {
                "name": "a",
                "table_pos": 0,
                "is_empty": False,
                "tables": [
                    {
                        "file": "/a.parquet",
                        "metadata": {"k": "v"},
                        "input_idx": 3,
                        "version_pos": -1,
                        "execution_id": None,
                        "transaction_id": None,
                        "physical_table_data_version_id": None,
                        "triggered_on": None,
                    },
                    {
                        "file": None,
                        "metadata": None,
                        "input_idx": None,
                        "version_pos": -1,
                        "execution_id": None,
                        "transaction_id": None,
                        "physical_table_data_version_id": None,
                        "triggered_on": None,
                    },
                ],
            },
            {"name": "b", "table_pos": 1, "is_empty": True, "tables": []},
        ]
    }


def test_load_keeps_several_records_in_order(tmp_path):
    manifest = InputManifest(
        inputs=(
            InputEntry(
                name="a",
                table_pos=0,
                tables=(
                    InputRecord(file="/b.parquet", version_pos=0),
                    InputRecord(file="/c.parquet", version_pos=1),
                ),
            ),
        )
    )
    path = tmp_path / "inputs.yaml"
    manifest.write(path)
    assert InputManifest.load(path) == manifest


def test_load_gives_optional_keys_their_defaults(tmp_path):
    path = tmp_path / "inputs.yaml"
    path.write_text("inputs: [{name: a, table_pos: 0, tables: [{file: /a.parquet}]}]\n")
    assert InputManifest.load(path).inputs == (
        InputEntry(name="a", table_pos=0, tables=(InputRecord(file="/a.parquet"),)),
    )


def test_version_of_a_versioned_record():
    version = VERSIONED.version()
    assert (version.execution, version.transaction, version.version) == (
        "E",
        "T",
        "V",
    )
    assert version.timestamp == datetime.fromtimestamp(
        TRIGGERED_ON / 1000, tz=timezone.utc
    )


@pytest.mark.parametrize(
    "field",
    [
        "execution_id",
        "transaction_id",
        "physical_table_data_version_id",
        "triggered_on",
    ],
)
def test_version_needs_every_version_field(field):
    assert replace(VERSIONED, **{field: None}).version() is None
