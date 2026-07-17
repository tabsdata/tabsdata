#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end BigQuery connector tests: register a real function on a running
tabsdata instance, trigger it, and assert the outcome against a tdx-provisioned
``bigquery`` domain (a real BigQuery dataset) -- unlike
``test__plugin_integration.py``, which drives the plugin directly.

BigQuery is destination-only (no ``BigQuerySrc``), so the source table is seeded by
the ``dev`` connector's ``DataGenSrc`` (in-line rows). The subscriber writes it out
to a scratch BigQuery table (the load job creates it), verified with a
``google-cloud-bigquery`` client. Requires the ``tdtest`` instance up and the
``bigquery`` domain provisioned with project/dataset/bucket/SA creds.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.bigquery import BigqueryDomain, bigquery_domain
from tabsdatax.itest.fixture.functions.files import write_subscriber_module
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.functions.sql import write_sql_publisher
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.api import Secret
from tabsdatak.conn.bigquery import BigQueryDestConn, GCPJson
from tabsdatak.conn.dev import DataGenSrcConn

pytestmark = pytest.mark.bigquery

_TABLE = "e2e_bigquery_tbl"
_DATA = {"id": [1, 2, 3], "name": ["alice", "bob", "carol"]}
_SRC_IMPORT = "import polars as pl\nfrom tabsdatak.conn.dev import DataGenSrc, Data"


def _dest_conn(domain: BigqueryDomain, staging) -> BigQueryDestConn:
    return BigQueryDestConn(
        staging=staging(domain),
        credentials=GCPJson(json=Secret(domain.service_account_key)),
        project=domain.project,
        dataset=domain.dataset,
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
@bigquery_domain("bigquery")
def test_subscriber_writes_table_to_bigquery_domain(
    instance_tdtest: Instance,
    domain_bigquery: BigqueryDomain,
    collection_factory: CollectionFactory,
    bigquery,
    staging,
    tmp_path: Path,
) -> None:
    # Seed a tabsdata table from DataGenSrc, then subscribe it out to a scratch
    # bigquery table and assert the rows match. OS/kminus does not auto-trigger
    # subscribers, so trigger it directly.
    db = bigquery(domain_bigquery)
    expected = pl.DataFrame(_DATA)
    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    source_collection = _seed_source(collection_factory, publisher_root)

    out_table = f"e2e_out_{uuid.uuid4().hex[:8]}"
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = write_subscriber_module(
        subscriber_root,
        dest_import="from tabsdatak.conn.bigquery import BigQueryDest",
        dest_expr=(
            f"BigQueryDest(tables=[{f'`{out_table}`'!r}], if_table_exists='replace')"
        ),
        input_table=f"{source_collection.name}/{_TABLE}",
    )
    destination_collection = collection_factory(
        "destinations", _dest_conn(domain_bigquery, staging)
    )
    try:
        run_function(destination_collection, subscriber_info, subscriber_root)
        assert_frames_match(db.read_table(out_table), expected)
    finally:
        db.drop_table(out_table)
