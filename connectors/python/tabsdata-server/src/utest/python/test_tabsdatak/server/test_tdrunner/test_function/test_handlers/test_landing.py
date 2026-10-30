#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import polars as pl
import pyarrow.parquet as pq
import pytest

from tabsdatak.server._tdrunner.function._commons.document.request import Location
from tabsdatak.server._tdrunner.function._handlers.landing import (
    TABSDATA_LANDING_FACTS_KEY,
    Landing,
    Origin,
    SchemaMismatchError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def landing(root: Path) -> Landing:
    return Landing(Location(uri=(root / "landing").as_uri(), env_prefix="ROOT"))


def local(location: Location) -> Path:
    return Path(url2pathname(urlparse(location.uri).path))


def facts(location: Location) -> dict:
    metadata = pq.read_metadata(local(location)).metadata
    return json.loads(metadata[TABSDATA_LANDING_FACTS_KEY.encode()])


def test_typed_data_lands_with_its_own_types(tmp_path):
    frame = pl.LazyFrame(
        {
            "id": [1, 2, 3],
            "day": [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)],
        },
    )
    landed = landing(tmp_path).land(
        0,
        0,
        frame,
        metadata={"@query": "select 1"},
    )
    assert landed.origin is Origin.DECLARED
    assert landed.rows == 3
    assert pl.read_parquet(local(landed.data)).equals(frame.collect())
    described = pl.read_parquet(local(landed.schema))
    assert described.height == 0
    assert described.schema == frame.collect_schema()
    assert facts(landed.schema) == {
        "rows": 3,
        "origin": "declared",
        "metadata": {"@query": "select 1"},
    }


def test_untyped_data_lands_as_strings_with_the_schema_polars_inferred(tmp_path):
    source = tmp_path / "orders.csv"
    source.write_text("id,paid,day\n00123,true,2026-01-31\n7,False,2026-02-01\n")
    schema = pl.scan_csv(
        source,
        infer_schema_length=None,
        try_parse_dates=True,
    ).collect_schema()
    strings = pl.scan_csv(source, infer_schema=False)
    landed = landing(tmp_path).land(
        1,
        2,
        strings,
        schema=schema,
    )
    assert landed.origin is Origin.INFERRED
    assert landed.data.uri.endswith("/landing/1.2.t")
    assert landed.schema.uri.endswith("/landing/1.2.s")
    data = pl.read_parquet(local(landed.data))
    assert data.to_dict(as_series=False) == {
        "id": ["00123", "7"],
        "paid": ["true", "False"],
        "day": ["2026-01-31", "2026-02-01"],
    }
    assert pl.read_parquet(local(landed.schema)).schema == pl.Schema(
        {"id": pl.Int64, "paid": pl.Boolean, "day": pl.Date},
    )
    assert facts(landed.schema) == {"rows": 2, "origin": "inferred", "metadata": None}


def test_an_empty_frame_lands_with_no_rows(tmp_path):
    frame = pl.LazyFrame(schema={"id": pl.Int64})
    landed = landing(tmp_path).land(0, 0, frame)
    assert landed.rows == 0
    assert pl.read_parquet(local(landed.data)).schema == frame.collect_schema()


def test_a_schema_must_name_the_data_columns(tmp_path):
    frame = pl.LazyFrame({"id": ["1"]})
    with pytest.raises(SchemaMismatchError, match=r"names columns \['code'\]"):
        landing(tmp_path).land(
            0,
            0,
            frame,
            schema=pl.Schema({"code": pl.Int64}),
        )


def test_a_schema_is_only_for_untyped_data(tmp_path):
    frame = pl.LazyFrame({"id": [1]})
    with pytest.raises(SchemaMismatchError, match=r"\['id'\] are typed"):
        landing(tmp_path).land(
            0,
            0,
            frame,
            schema=pl.Schema({"id": pl.Int64}),
        )
