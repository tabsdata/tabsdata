#
# Copyright 2026 Tabsdata Inc.
#

"""Plumbing every SQL connector's plugin-level integration test repeats: a work
dir for the plugin to write into, the frame behind a ``read_in`` output slot, and
the provisioned data a test drives the plugin with (a SQL domain's row count, an
fs domain's parquet file).

Connector-agnostic on purpose -- what varies per connector (the store's identifier
delimiters, the URI shape, the ``Conn`` classes) belongs in that connector's own
``backend`` module.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain
from tabsdatax.itest.fixture.functions.files import first_provisioned_file
from tabsdatax.itest.fixture.functions.sql import read_parquet_slot

PARQUET = "parquet"


def work_dir(tmp_path: Path, name: str = "work") -> Path:
    """A fresh directory to hand a plugin as its ``work_dir``."""
    work = tmp_path / name
    work.mkdir()
    return work


def slot_frame(out: list[list], index: int = 0) -> pl.DataFrame:
    """The frame a ``read_in`` output slot holds, chunks concatenated. A query that
    matched no rows writes no chunk at all, so its slot is empty -- assert on
    ``out[index] == []`` rather than calling this."""
    return pl.concat([read_parquet_slot(p) for p in out[index]])


def entity_rows(domain, entity: str) -> int:
    """The row count the descriptor records for `entity`. SQL domains list entities
    as bare table names. A domain missing the entity failed provisioning, so this
    fails rather than skipping -- the model's tables are part of the contract a
    resolved domain fulfils."""
    rows = (domain.entities or {}).get(entity)
    if rows is None:
        pytest.fail(
            f"domain {domain.name!r} (model={domain.model!r}) has no {entity!r}; "
            f"(re)provision the domain with data",
            pytrace=False,
        )
    return rows


def provisioned_parquet(domain: FsDomain, entity: str, fmt: str = PARQUET) -> Path:
    """`entity`'s provisioned file in an fs domain, parquet by default -- the seed
    lays each format out under its own subdir. Parquet keeps the column types the
    text formats flatten, so it is what a destination test feeds the plugin."""
    return first_provisioned_file(domain.base_path / fmt, f"{entity}*.{fmt}")
