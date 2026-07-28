#
# Copyright 2026 Tabsdata Inc.
#

"""LocalFile destination-plugin integration tests using tabsdata"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.common.sql.function_project import function_module
from testi_tabsdatak.conn.localfile.backend import dest_conn, scratch

from tabsdatak.conn.dev import DataGenSrcConn
from tabsdatak.sdk.tabsdata_server import FunctionInfo

pytestmark = pytest.mark.localfile

# The functions these tests register live in `functions/`, checked in as a user
# project would have them; that directory is the project_root they are bundled
# from.
FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")
subscriber = function_module(FUNCTIONS, "subscriber")


@pytest.mark.parametrize(
    "dest_format, reader, function",
    [
        ("csv", pl.read_csv, subscriber.sub_subscriber_writes_csv_to_fs_domain),
        ("json", pl.read_ndjson, subscriber.sub_subscriber_writes_json_to_fs_domain),
        (
            "parquet",
            pl.read_parquet,
            subscriber.sub_subscriber_writes_parquet_to_fs_domain,
        ),
    ],
)
@tabsdata_instance(consumer="tdtest")
@fs_domain("fs")
def test_subscriber_writes_format_to_fs_domain(
    dest_format: str,
    reader: Callable[[Path], pl.DataFrame],
    function: FunctionInfo,
    instance_tdtest: Instance,
    domain_fs: FsDomain,
    collection_factory: CollectionFactory,
    request: pytest.FixtureRequest,
) -> None:
    # The subscriber reads a tabsdata table, so the input table is filled by the
    # dev data generator -- its rows live in the publisher module, so no other
    # connector and no external system feed this test.
    # OS/kminus does not auto-trigger subscribers, so trigger it directly.
    _, base, _ = scratch(domain_fs, request)
    source = collection_factory(
        "sources", DataGenSrcConn(), publisher.SOURCE_COLLECTION
    )
    run_function(source, publisher.pub_subscriber_writes_table_to_fs_domain, FUNCTIONS)

    destination = collection_factory("destinations", dest_conn(base))
    run_function(destination, function, FUNCTIONS)

    out_file = base / f"{subscriber.SUBSCRIBED_STEM}.{dest_format}"
    assert out_file.exists(), f"subscriber did not write {out_file}"
    assert_frames_match(reader(out_file), publisher.GENERATED_FRAME)
