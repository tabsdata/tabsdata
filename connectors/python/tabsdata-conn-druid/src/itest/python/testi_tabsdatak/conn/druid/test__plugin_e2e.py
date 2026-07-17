#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end Druid connector tests: register a real function on a running
tabsdata instance, trigger it, and assert the outcome against a tdx-provisioned
``druid`` domain (a real Druid cluster) -- unlike ``test__plugin_integration.py``,
which drives the plugin directly.

Druid is destination-only (no ``DruidSrc``), so the source table is seeded by the
``dev`` connector's ``DataGenSrc`` (in-line rows). The subscriber writes it out to
a scratch Druid datasource (ingestion creates it), staged through S3, and verified
with Druid SQL. Druid is eventually consistent, so verification polls. Requires
the ``tdtest`` instance up plus the ``druid`` and ``s3`` domains."""

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.druid import DruidDomain, druid_domain
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.files import write_subscriber_module
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.functions.sql import write_sql_publisher
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.api import Secret
from tabsdatak.conn.dev import DataGenSrcConn
from tabsdatak.conn.druid import DruidDestConn, UserPassword

pytestmark = pytest.mark.druid

_TABLE = "e2e_druid_tbl"
_DATA = {"id": [1, 2, 3], "name": ["alice", "bob", "carol"]}
_SRC_IMPORT = "import polars as pl\nfrom tabsdatak.conn.dev import DataGenSrc, Data"


def _dest_conn(
    domain_druid: DruidDomain, domain_s3: S3Domain, staging
) -> DruidDestConn:
    base_path = f"/druid-staging/{uuid.uuid4().hex}"
    return DruidDestConn(
        overlord_url=domain_druid.overlord_url,
        staging=staging(domain_s3, base_path),
        credentials=UserPassword(
            user=Secret(domain_druid.user),
            password=Secret(domain_druid.password),
        ),
    )


def _seed_source(collection_factory: CollectionFactory, project_root: Path) -> object:
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
@druid_domain("druid")
@s3_domain("s3")
def test_subscriber_writes_table_to_druid_domain(
    instance_tdtest: Instance,
    domain_druid: DruidDomain,
    domain_s3: S3Domain,
    collection_factory: CollectionFactory,
    druid,
    staging,
    tmp_path: Path,
) -> None:
    # Seed a tabsdata table from DataGenSrc, then subscribe it out to a scratch
    # druid datasource and assert the rows match. OS/kminus does not auto-trigger
    # subscribers, so trigger it directly.
    store = druid(domain_druid)
    expected = pl.DataFrame(_DATA)
    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    source_collection = _seed_source(collection_factory, publisher_root)

    datasource = f"e2e_out_{uuid.uuid4().hex[:8]}"
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = write_subscriber_module(
        subscriber_root,
        dest_import="from tabsdatak.conn.druid import DruidDest",
        dest_expr=(f"DruidDest(tables=[{datasource!r}], if_table_exists='replace')"),
        input_table=f"{source_collection.name}/{_TABLE}",
    )
    destination_collection = collection_factory(
        "destinations", _dest_conn(domain_druid, domain_s3, staging)
    )
    try:
        run_function(destination_collection, subscriber_info, subscriber_root)
        assert_frames_match(store.read_table(datasource, expected_rows=3), expected)
    finally:
        store.drop_datasource(datasource)
