#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import uuid
from pathlib import Path

import obstore
import polars as pl
import pytest
from polars.testing import assert_frame_equal
from tabsdatax.itest.fixture.backends.azurebloblocal import (
    AzureBlobLocalDomain,
    azurebloblocal_domain,
)
from tabsdatax.itest.fixture.backends.gcslocal import GcsLocalDomain, gcslocal_domain
from tabsdatax.itest.fixture.backends.s3local import S3LocalDomain, s3local_domain

from tabsdatak.common._storage import frames, transfer
from testi_tabsdatak.common._storage import mounted

pytestmark = [pytest.mark.integration, pytest.mark.tabsdatak]

ROWS = 100_000
TAIL = 10


def table() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "id": pl.int_range(ROWS, eager=True),
            "name": pl.int_range(ROWS, eager=True).cast(pl.String),
        },
    )


def round_trip(domain, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    declaration = mounted.declaration(domain, tmp_path, monkeypatch)
    root = declaration.resolved_uri().rstrip("/")
    uri = f"{root}/frames-{uuid.uuid4().hex}/table.parquet"
    target = frames.target(uri, declaration)
    try:
        frames.sink_parquet(table(), target)
        assert_frame_equal(frames.scan_parquet(target).collect(), table().collect())
        tail = frames.scan_parquet(target).filter(pl.col("id") >= ROWS - TAIL)
        assert tail.collect().height == TAIL
    finally:
        store_root, name = transfer.split(uri)
        obstore.delete(transfer.store(store_root, target.storage_options), name)


@s3local_domain("s3local")
def test_s3local_frame_round_trip(
    domain_s3local: S3LocalDomain,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    round_trip(domain_s3local, tmp_path, monkeypatch)


@gcslocal_domain("gcslocal")
def test_gcslocal_frame_round_trip(
    domain_gcslocal: GcsLocalDomain,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    round_trip(domain_gcslocal, tmp_path, monkeypatch)


@azurebloblocal_domain("azurebloblocal")
def test_azurebloblocal_frame_round_trip(
    domain_azurebloblocal: AzureBlobLocalDomain,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    round_trip(domain_azurebloblocal, tmp_path, monkeypatch)
