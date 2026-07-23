#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end Snowflake connector tests: register a real function on a running
tabsdata instance, trigger it, and assert the outcome against a tdx-provisioned
``snowflake`` domain (a real Snowflake account) -- unlike
``test__plugin_integration.py``, which drives the plugin directly.

Snowflake is destination-only (no ``SnowflakeSrc``), so the source table is seeded
by the ``dev`` connector's ``DataGenSrc`` (in-line rows). The subscriber writes it
out to a scratch Snowflake table (the connector creates it), verified with a
``snowflake.connector`` client. Requires the ``tdtest`` instance up and the
``snowflake`` domain provisioned with account/PAT creds.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.snowflake import SnowflakeDomain, snowflake_domain
from tabsdatax.itest.fixture.functions.files import write_subscriber_module
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
)
from tabsdatax.itest.fixture.functions.sql import write_sql_publisher
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.dev import DataGenSrcConn
from tabsdatak.conn.snowflake import SnowflakeDestConn

pytestmark = pytest.mark.snowflake

_TABLE = "e2e_snowflake_tbl"
_DATA = {"id": [1, 2, 3], "name": ["alice", "bob", "carol"]}
_SRC_IMPORT = "import polars as pl\nfrom tabsdatak.conn.dev import DataGenSrc, Data"


def _dest_conn(domain: SnowflakeDomain) -> SnowflakeDestConn:
    # Pass the schema bare: tdx provisions it unquoted (Snowflake upper-folds it),
    # so the connector resolves it to the stored, upper-folded identifier.
    return SnowflakeDestConn(
        account=domain.account,
        credentials=UserPassword(
            user=Secret(domain.username), password=Secret(domain.pat)
        ),
        database=domain.database,
        schema=domain.schema,
        warehouse=domain.warehouse,
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
@snowflake_domain("snowflake")
def test_subscriber_writes_table_to_snowflake_domain(
    instance_tdtest: Instance,
    domain_snowflake: SnowflakeDomain,
    collection_factory: CollectionFactory,
    snowflake,
    tmp_path: Path,
) -> None:
    # Seed a tabsdata table from DataGenSrc, then subscribe it out to a scratch
    # snowflake table and assert the rows match. OS/kminus does not auto-trigger
    # subscribers, so trigger it directly.
    db = snowflake(domain_snowflake)
    expected = pl.DataFrame(_DATA)
    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    source_collection = _seed_source(collection_factory, publisher_root)

    out_table = f"E2E_OUT_{uuid.uuid4().hex[:8].upper()}"
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = write_subscriber_module(
        subscriber_root,
        dest_import="from tabsdatak.conn.snowflake import SnowflakeDest",
        dest_expr=f"SnowflakeDest(tables=[{out_table!r}], if_table_exists='replace')",
        input_table=f"{source_collection.name}/{_TABLE}",
    )
    destination_collection = collection_factory(
        "destinations", _dest_conn(domain_snowflake)
    )
    try:
        run_function(destination_collection, subscriber_info, subscriber_root)
        got = db.read_table(out_table)
        got = got.rename({c: c.lower() for c in got.columns})
        assert_frames_match(got, expected)
    finally:
        db.drop_table(out_table)
