#
# Copyright 2026 Tabsdata Inc.
#

"""AzureBlob destination-plugin integration tests using tabsdata"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain, azureblob_domain
from tabsdatax.itest.fixture.functions.files import download_object
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.azureblob.backend import dest_conn, run_prefix, store
from testi_tabsdatak.conn.common.sql.function_project import function_module

from tabsdatak.conn.dev import DataGenSrcConn
from tabsdatak.sdk.tabsdata_server import FunctionInfo

pytestmark = pytest.mark.azureblob

# The functions these tests register live in `functions/`, checked in as a user
# project would have them; that directory is the project_root they are bundled
# from.
FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")
subscriber = function_module(FUNCTIONS, "subscriber")


@pytest.mark.parametrize(
    "dest_format, reader, function",
    [
        ("csv", pl.read_csv, subscriber.sub_subscriber_writes_csv_to_azureblob_domain),
        (
            "json",
            pl.read_ndjson,
            subscriber.sub_subscriber_writes_json_to_azureblob_domain,
        ),
        (
            "parquet",
            pl.read_parquet,
            subscriber.sub_subscriber_writes_parquet_to_azureblob_domain,
        ),
    ],
)
@tabsdata_instance(consumer="tdtest")
@azureblob_domain("azureblob")
def test_subscriber_writes_format_to_azureblob_domain(
    dest_format: str,
    reader: Callable[[Path], pl.DataFrame],
    function: FunctionInfo,
    instance_tdtest: Instance,
    domain_azureblob: AzureblobDomain,
    collection_factory: CollectionFactory,
    tmp_path: Path,
) -> None:
    # The subscriber reads a tabsdata table, so the input table is filled by the
    # dev data generator -- its rows live in the publisher module, so no other
    # connector and no external system feed this test.
    # OS/kminus does not auto-trigger subscribers, so trigger it directly.
    prefix = run_prefix(domain_azureblob.base_path, "subscribed")
    source = collection_factory(
        "sources", DataGenSrcConn(), publisher.SOURCE_COLLECTION
    )
    run_function(
        source, publisher.pub_subscriber_writes_table_to_azureblob_domain, FUNCTIONS
    )

    destination = collection_factory(
        "destinations", dest_conn(domain_azureblob, prefix)
    )
    run_function(destination, function, FUNCTIONS)

    name = f"{subscriber.SUBSCRIBED_STEM}.{dest_format}"
    fetched = download_object(
        store(domain_azureblob), f"{prefix}/{name}", tmp_path / name
    )
    assert_frames_match(reader(fetched), publisher.GENERATED_FRAME)
