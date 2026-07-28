#
# Copyright 2026 Tabsdata Inc.
#

"""LocalFile source-plugin integration tests using tabsdata"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.files import first_provisioned_file
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
    table_frame,
    wait_for_completion,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.common.sql.function_project import function_module
from testi_tabsdatak.conn.localfile.backend import scratch, src_conn, stage_parquet

from tabsdatak.sdk.status_utils.execution import EXECUTION_SUCCESSFUL_FINAL_STATUSES
from tabsdatak.sdk.tabsdata_server import FunctionInfo

pytestmark = pytest.mark.localfile

# The functions these tests register live in `functions/`, checked in as a user
# project would have them; that directory is the project_root they are bundled
# from.
FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")


def _entity_file(domain: FsDomain, fmt: str) -> Path:
    """The provisioned file a publisher's glob matches, in ``fmt``'s directory."""
    return first_provisioned_file(domain.base_path / fmt, f"{publisher.ENTITY}_*.{fmt}")


def _reference(domain: FsDomain, source_format: str) -> pl.DataFrame:
    """What the published table must hold. The staged model is identical across
    formats, so the csv copy is the reference for every one of them -- except that
    the transporter reads a quoted empty csv cell as null, where polars' own read
    keeps it as an empty string."""
    frame = pl.read_csv(_entity_file(domain, "csv"))
    if source_format == "csv":
        frame = frame.with_columns(pl.col(pl.String).replace("", None))
    return frame


@pytest.mark.parametrize(
    "source_format, function",
    [
        ("csv", publisher.pub_publisher_reads_csv_from_fs_domain),
        ("ndjson", publisher.pub_publisher_reads_ndjson_from_fs_domain),
        ("parquet", publisher.pub_publisher_reads_parquet_from_fs_domain),
    ],
)
@tabsdata_instance(consumer="tdtest")
@fs_domain("fs_world_xs")
def test_publisher_reads_format_from_fs_domain(
    source_format: str,
    function: FunctionInfo,
    instance_tdtest: Instance,
    domain_fs_world_xs: FsDomain,
    collection_factory: CollectionFactory,
) -> None:
    domain = domain_fs_world_xs
    expected = _reference(domain, source_format)
    collection = collection_factory(
        "sources", src_conn(domain.base_path / source_format)
    )
    run_function(collection, function, FUNCTIONS)

    assert_frames_match(table_frame(collection, publisher.PUBLISHED_TABLE), expected)


@tabsdata_instance(consumer="tdtest")
@fs_domain("fs")
def test_publisher_imports_only_new_files_across_runs(
    instance_tdtest: Instance,
    domain_fs: FsDomain,
    collection_factory: CollectionFactory,
    request: pytest.FixtureRequest,
) -> None:
    # The per-slot ``file-lastmod`` cursor the plugin stashes on the trx context
    # has to survive the run for the second trigger to see only the file that
    # appeared after the first one -- what the plugin-level tests can only assert
    # against a fabricated context.
    _, base, _ = scratch(domain_fs, request)
    stage_parquet(base, "first.parquet", publisher.INCREMENTAL_CUTOFF)

    collection = collection_factory("sources", src_conn(base))
    function = collection.register_function(
        info=publisher.pub_publisher_imports_only_new_files_across_runs,
        project_root=str(FUNCTIONS),
    )

    status = wait_for_completion(function.trigger())
    assert status in EXECUTION_SUCCESSFUL_FINAL_STATUSES
    assert_frames_match(
        table_frame(collection, publisher.PUBLISHED_TABLE),
        pl.DataFrame({"name": ["first.parquet"]}),
    )

    stage_parquet(
        base, "second.parquet", publisher.INCREMENTAL_CUTOFF + timedelta(hours=1)
    )
    status = wait_for_completion(function.trigger())
    assert status in EXECUTION_SUCCESSFUL_FINAL_STATUSES
    assert_frames_match(
        table_frame(collection, publisher.PUBLISHED_TABLE),
        pl.DataFrame({"name": ["second.parquet"]}),
    )
