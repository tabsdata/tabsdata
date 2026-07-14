#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end StarRocks connector tests: register real functions on a running
tabsdata instance, trigger them, and assert the outcome against a tdx-provisioned
``starrocks`` domain -- unlike ``test__plugin_integration.py``, which drives the
plugin directly with a fabricated context.

StarRocks is destination-only (no ``StarRocksSrc``) and bulk-loads through cloud
staging, so the test declares three backends: ``@starrocks_domain`` (target),
``@s3_domain`` (staging credentials), and the ``tdtest`` instance. The source
table is seeded by the ``dev`` connector's ``DataGenSrc`` (in-line rows). The
connector loads into existing tables only, so the target table is pre-created via
the ``create_table`` fixture before the subscriber runs. Verification is over
StarRocks' MySQL wire protocol (the ``sql`` fixture).
"""

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.backends.starrocks import StarrocksDomain, starrocks_domain
from tabsdatax.itest.fixture.functions.files import write_subscriber_module
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.functions.sql import sql_credentials, write_sql_publisher
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.conn.dev import DataGenSrcConn
from tabsdatak.conn.starrocks import StarRocksDestConn

pytestmark = pytest.mark.starrocks

_TABLE = "e2e_starrocks_tbl"
_DATA = {"id": [1, 2, 3], "name": ["alice", "bob", "carol"]}
_SRC_IMPORT = "import polars as pl\nfrom tabsdatak.conn.dev import DataGenSrc, Data"


def _seed_source(collection_factory: CollectionFactory, project_root: Path) -> object:
    """Publish in-line ``DataGenSrc`` rows into a fresh source collection
    (StarRocks has no Src of its own); return the collection."""
    info = write_sql_publisher(
        project_root,
        source_import=_SRC_IMPORT,
        source_expr=f"DataGenSrc(data=[Data(pl.DataFrame({_DATA!r}))])",
        output_table=_TABLE,
    )
    collection = collection_factory("sources", DataGenSrcConn())
    run_function(collection, info, project_root)
    return collection


@pytest.mark.xfail(
    strict=True,
    reason=(
        "The tdx StarRocks user is passwordless, so the connection carries an "
        "empty-valued password secret. Empty-valued secrets are not injected into "
        "the function pod (env var left unset), so the subscriber worker fails "
        "hydration with COMMON_6 and the run ends 'Stalled'. Platform bug tracked "
        "in TD-2802; the connector itself is fine (integration tests pass). Drop "
        "this xfail once empty secrets reach function pods."
    ),
)
@tabsdata_instance(consumer="tdtest")
@starrocks_domain("starrocks")
@s3_domain("s3")
def test_subscriber_writes_table_to_starrocks_domain(
    instance_tdtest: Instance,
    domain_starrocks: StarrocksDomain,
    domain_s3: S3Domain,
    collection_factory: CollectionFactory,
    sql,
    staging,
    create_table,
    tmp_path: Path,
) -> None:
    # Seed a tabsdata table from DataGenSrc, then subscribe it out to a scratch
    # starrocks table (pre-created, since the connector loads into existing tables
    # only) and assert the rows match. OS/kminus does not auto-trigger
    # subscribers, so trigger it directly.
    db = sql(domain_starrocks)
    expected = pl.DataFrame(_DATA)
    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    source_collection = _seed_source(collection_factory, publisher_root)

    out_table = f"e2e_out_{uuid.uuid4().hex[:8]}"
    create_table(domain_starrocks, out_table)
    base = str(domain_s3.base_path).strip("/")
    dest_conn = StarRocksDestConn(
        host=domain_starrocks.host,
        port=int(domain_starrocks.port),
        credentials=sql_credentials(domain_starrocks),
        staging=staging(domain_s3, f"/{base}/starrocks-staging/{uuid.uuid4().hex}"),
        database=domain_starrocks.database,
    )
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = write_subscriber_module(
        subscriber_root,
        dest_import="from tabsdatak.conn.starrocks import StarRocksDest",
        dest_expr=(f"StarRocksDest(tables=[{out_table!r}], if_table_exists='replace')"),
        input_table=f"{source_collection.name}/{_TABLE}",
    )
    destination_collection = collection_factory("destinations", dest_conn)
    try:
        run_function(destination_collection, subscriber_info, subscriber_root)
        assert_frames_match(db.read_table(out_table), expected)
    finally:
        db.drop_table(out_table)
