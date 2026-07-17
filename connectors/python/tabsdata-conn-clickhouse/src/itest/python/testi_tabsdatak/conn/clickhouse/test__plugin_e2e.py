#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end ClickHouse connector tests: register real functions on a running
tabsdata instance, trigger them, and assert the outcome against a tdx-provisioned
``clickhouse`` domain -- unlike ``test__plugin_integration.py``, which drives the
plugin directly with a fabricated context.

ClickHouse is destination-only (no ``ClickHouseSrc``) and bulk-loads through cloud
staging, so the test declares ``@clickhouse_domain`` (target), ``@s3_domain``
(staging credentials), and the ``tdtest`` instance. The source table is seeded by
the ``dev`` connector's ``DataGenSrc`` (in-line rows). Verification is with a
``clickhouse_connect`` client. The integration suite covers the full staging
matrix (s3/azblob/gcs); this e2e exercises the through-the-instance path once with
S3 staging.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.clickhouse import (
    ClickhouseDomain,
    clickhouse_domain,
)
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.files import write_subscriber_module
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.functions.sql import sql_credentials, write_sql_publisher
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.conn.clickhouse import ClickHouseDestConn
from tabsdatak.conn.dev import DataGenSrcConn

pytestmark = pytest.mark.clickhouse

_TABLE = "e2e_clickhouse_tbl"
_DATA = {"id": [1, 2, 3], "name": ["alice", "bob", "carol"]}
_SRC_IMPORT = "import polars as pl\nfrom tabsdatak.conn.dev import DataGenSrc, Data"


def _seed_source(collection_factory: CollectionFactory, project_root: Path) -> object:
    """Publish in-line ``DataGenSrc`` rows into a fresh source collection
    (ClickHouse has no Src of its own); return the collection."""
    info = write_sql_publisher(
        project_root,
        source_import=_SRC_IMPORT,
        source_expr=f"DataGenSrc(data=[Data(pl.DataFrame({_DATA!r}))])",
        output_table=_TABLE,
    )
    collection = collection_factory("sources", DataGenSrcConn())
    run_function(collection, info, project_root)
    return collection


@tabsdata_instance(consumer="tdtest")
@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_subscriber_writes_table_to_clickhouse_domain(
    instance_tdtest: Instance,
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    collection_factory: CollectionFactory,
    clickhouse,
    staging,
    tmp_path: Path,
) -> None:
    # Seed a tabsdata table from DataGenSrc, then subscribe it out to a scratch
    # clickhouse table and assert the rows match. OS/kminus does not auto-trigger
    # subscribers, so trigger it directly.
    db = clickhouse(domain_clickhouse)
    expected = pl.DataFrame(_DATA)
    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    source_collection = _seed_source(collection_factory, publisher_root)

    out_table = f"e2e_out_{uuid.uuid4().hex[:8]}"
    base = str(domain_s3.base_path).strip("/")
    dest_conn = ClickHouseDestConn(
        host=domain_clickhouse.host,
        port=str(domain_clickhouse.port),
        credentials=sql_credentials(domain_clickhouse),
        staging=staging(domain_s3, f"/{base}/clickhouse-staging/{uuid.uuid4().hex}"),
        database=domain_clickhouse.database,
        secure="false",
    )
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = write_subscriber_module(
        subscriber_root,
        dest_import="from tabsdatak.conn.clickhouse import ClickHouseDest",
        dest_expr=(
            f"ClickHouseDest(tables=[{out_table!r}], if_table_exists='replace')"
        ),
        input_table=f"{source_collection.name}/{_TABLE}",
    )
    destination_collection = collection_factory("destinations", dest_conn)
    try:
        run_function(destination_collection, subscriber_info, subscriber_root)
        assert_frames_match(db.read_table(out_table), expected)
    finally:
        db.drop_table(out_table)
