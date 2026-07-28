#
# Copyright 2026 Tabsdata Inc.
#

"""AzureBlob source-plugin integration tests using tabsdata"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain, azureblob_domain
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
    table_frame,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.azureblob.backend import src_conn, store
from testi_tabsdatak.conn.common.sql.function_project import function_module

from tabsdatak.sdk.tabsdata_server import FunctionInfo

pytestmark = pytest.mark.azureblob

# The functions these tests register live in `functions/`, checked in as a user
# project would have them; that directory is the project_root they are bundled
# from.
FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")


def _entity_key(domain: AzureblobDomain, fmt: str) -> str:
    """The provisioned blob a publisher's glob matches. The descriptor lists
    entities as ``<format>/<entity>_<stamp>.<format>``, relative to base_path."""
    prefix = f"{fmt}/{publisher.ENTITY}_"
    matches = sorted(key for key in (domain.entities or {}) if key.startswith(prefix))
    assert matches, f"no {prefix!r} entity in {sorted(domain.entities or {})}"
    return f"{str(domain.base_path).strip('/')}/{matches[0]}"


def _reference(
    domain: AzureblobDomain, source_format: str, tmp_path: Path
) -> pl.DataFrame:
    """What the published table must hold. The staged model is identical across
    formats, so the csv copy is the reference for every one of them -- except that
    the transporter reads a quoted empty csv cell as null, where polars' own read
    keeps it as an empty string."""
    local = tmp_path / "reference.csv"
    store(domain).download(_entity_key(domain, "csv"), local)
    frame = pl.read_csv(local)
    if source_format == "csv":
        frame = frame.with_columns(pl.col(pl.String).replace("", None))
    return frame


@pytest.mark.parametrize(
    "source_format, function",
    [
        ("csv", publisher.pub_publisher_reads_csv_from_azureblob_domain),
        ("ndjson", publisher.pub_publisher_reads_ndjson_from_azureblob_domain),
        ("parquet", publisher.pub_publisher_reads_parquet_from_azureblob_domain),
    ],
)
@tabsdata_instance(consumer="tdtest")
@azureblob_domain("azureblob_world_xs")
def test_publisher_reads_format_from_azureblob_domain(
    source_format: str,
    function: FunctionInfo,
    instance_tdtest: Instance,
    domain_azureblob_world_xs: AzureblobDomain,
    collection_factory: CollectionFactory,
    tmp_path: Path,
) -> None:
    domain = domain_azureblob_world_xs
    expected = _reference(domain, source_format, tmp_path)
    collection = collection_factory(
        "sources", src_conn(domain, f"{domain.base_path}/{source_format}")
    )
    run_function(collection, function, FUNCTIONS)

    assert_frames_match(table_frame(collection, publisher.PUBLISHED_TABLE), expected)
