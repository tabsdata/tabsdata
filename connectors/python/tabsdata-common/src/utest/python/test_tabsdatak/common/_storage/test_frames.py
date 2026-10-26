#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from tabsdatak.common._storage import frames, mounts
from tabsdatak.common._storage.mounts import (
    AnonymousMountDeclaration,
    NamedMountDeclaration,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

PLACEHOLDER = "${{secret}}"
LOCATION = "s3://tables/collection/table/v1.t"


def option_variable(mount: str, key: str) -> str:
    return mounts.secret_variable("storage", "mounts", mount, "options", key)


def test_an_anonymous_location_is_read_as_it_is():
    target = frames.target(LOCATION, AnonymousMountDeclaration())

    assert target == frames.FrameTarget(uri=LOCATION, storage_options={})


def test_a_named_location_carries_its_mount_options(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(option_variable("ROOT", "secret_access_key"), "secret")
    declaration = NamedMountDeclaration(
        id="ROOT",
        uri="s3://tables",
        options={
            "access_key_id": "identity",
            "secret_access_key": PLACEHOLDER,
            "endpoint": "http://127.0.0.1:9000",
            "allow_http": "true",
        },
    )

    target = frames.target(LOCATION, declaration)

    assert target == frames.FrameTarget(
        uri=LOCATION,
        storage_options={
            "aws_access_key_id": "identity",
            "aws_secret_access_key": "secret",
            "aws_endpoint": "http://127.0.0.1:9000",
            "aws_allow_http": "true",
        },
    )


def test_options_are_resolved_once_when_the_target_is_built(
    monkeypatch: pytest.MonkeyPatch,
):
    variable = option_variable("ROOT", "secret_access_key")
    monkeypatch.setenv(variable, "secret")
    declaration = NamedMountDeclaration(
        id="ROOT",
        uri="s3://tables",
        options={
            "access_key_id": "identity",
            "secret_access_key": PLACEHOLDER,
            "region": "eu-west-1",
        },
    )

    target = frames.target(LOCATION, declaration)
    monkeypatch.delenv(variable)

    assert target.storage_options["aws_secret_access_key"] == "secret"


@pytest.mark.parametrize(
    "declaration",
    [
        AnonymousMountDeclaration(),
        NamedMountDeclaration(id="ROOT", uri="file:///unused"),
    ],
    ids=["anonymous", "named"],
)
def test_a_frame_round_trips_through_a_file_uri(tmp_path: Path, declaration):
    frame = pl.LazyFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    target = frames.target((tmp_path / "table.parquet").as_uri(), declaration)

    frames.sink_parquet(frame, target)

    assert_frame_equal(frames.scan_parquet(target).collect(), frame.collect())
