#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end MSSQL connector tests: register a real function on a running
tabsdata instance, trigger it, and assert the outcome against a tdx-provisioned
``mssqlserver`` domain -- unlike ``test__plugin_integration.py``, which drives the
plugin directly with a fabricated context.

The generic harness and the SQL helpers (entity resolution, credentials, the
scalar/watermark publisher generators) are shared under
:mod:`tabsdatax.itest.fixture.functions` (``harness`` / ``files`` / ``sql``);
only the mssql ``Conn`` construction is here. Requires the ``tdtest`` instance up
and the ``mssql`` domain provisioned with data.
"""

from __future__ import annotations

import urllib.parse
import uuid
from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.mssqlserver import (
    MssqlserverDomain,
    mssqlserver_domain,
)
from tabsdatax.itest.fixture.functions.files import write_subscriber_module
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
    wait_for_completion,
)
from tabsdatax.itest.fixture.functions.sql import (
    SqlStore,
    entity_name,
    sql_credentials,
    write_sql_publisher,
    write_watermark_publisher,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.conn.mssql import MSSQLDestConn, MSSQLSrcConn
from tabsdatak.sdk.status_utils.execution import EXECUTION_SUCCESSFUL_FINAL_STATUSES

pytestmark = pytest.mark.mssql

_SRC_IMPORT = "from tabsdatak.conn.mssql import MSSQLSrc"
_DEST_IMPORT = "from tabsdatak.conn.mssql import MSSQLDest"
_TABLE = "e2e_mssql_tbl"
_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


def _conn_uri(domain: MssqlserverDomain) -> str:
    driver = urllib.parse.quote_plus(_ODBC_DRIVER)
    return (
        f"mssql+pyodbc://{domain.host}:{domain.port}/{domain.db}"
        f"?driver={driver}&TrustServerCertificate=yes"
    )


def _src_conn(domain: MssqlserverDomain) -> MSSQLSrcConn:
    return MSSQLSrcConn(uri=_conn_uri(domain), credentials=sql_credentials(domain))


def _dest_conn(domain: MssqlserverDomain) -> MSSQLDestConn:
    return MSSQLDestConn(uri=_conn_uri(domain), credentials=sql_credentials(domain))


def _publish(
    collection_factory: CollectionFactory,
    domain: MssqlserverDomain,
    project_root: Path,
    entity: str,
    db: SqlStore,
) -> object:
    """Publish ``SELECT * FROM <entity>`` into a fresh source collection; return
    the collection."""
    query = f"SELECT * FROM {db.quote(entity)}"
    info = write_sql_publisher(
        project_root,
        source_import=_SRC_IMPORT,
        source_expr=f"MSSQLSrc(queries=[{query!r}])",
        output_table=_TABLE,
    )
    collection = collection_factory("sources", _src_conn(domain))
    run_function(collection, info, project_root)
    return collection


@tabsdata_instance(consumer="tdtest")
@mssqlserver_domain("mssql")
def test_publisher_reads_table_from_mssql_domain(
    instance_tdtest: Instance,
    domain_mssql: MssqlserverDomain,
    collection_factory: CollectionFactory,
    sql,
    tmp_path: Path,
) -> None:
    db = sql(domain_mssql)
    entity = entity_name(domain_mssql)
    expected = db.read_table(entity)
    project_root = tmp_path / "pub"
    project_root.mkdir()
    collection = _publish(collection_factory, domain_mssql, project_root, entity, db)

    published = collection.get_table(_TABLE)
    assert published is not None, f"table {_TABLE!r} not found after publish"
    assert_frames_match(published.sample(), expected)


@tabsdata_instance(consumer="tdtest")
@mssqlserver_domain("mssql")
def test_subscriber_writes_table_to_mssql_domain(
    instance_tdtest: Instance,
    domain_mssql: MssqlserverDomain,
    collection_factory: CollectionFactory,
    sql,
    tmp_path: Path,
) -> None:
    # Round-trip: publish a provisioned table into a tabsdata table, then
    # subscribe it back out to a scratch mssql table and assert it matches.
    # OS/kminus does not auto-trigger subscribers, so trigger it directly.
    db = sql(domain_mssql)
    entity = entity_name(domain_mssql)
    expected = db.read_table(entity)
    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    source_collection = _publish(
        collection_factory, domain_mssql, publisher_root, entity, db
    )

    out_table = f"e2e_out_{uuid.uuid4().hex[:8]}"
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = write_subscriber_module(
        subscriber_root,
        dest_import=_DEST_IMPORT,
        dest_expr=f"MSSQLDest(tables=[{out_table!r}], if_table_exists='replace')",
        input_table=f"{source_collection.name}/{_TABLE}",
    )
    destination_collection = collection_factory(
        "destinations", _dest_conn(domain_mssql)
    )
    try:
        run_function(destination_collection, subscriber_info, subscriber_root)
        assert_frames_match(db.read_table(out_table), expected)
    finally:
        db.drop_table(out_table)


@tabsdata_instance(consumer="tdtest")
@mssqlserver_domain("mssql")
def test_publisher_initial_values_advance_across_runs(
    instance_tdtest: Instance,
    domain_mssql: MssqlserverDomain,
    collection_factory: CollectionFactory,
    sql,
    tmp_path: Path,
) -> None:
    # Incremental publish: seed a watermark bind (wm=0), advance it to 3 via
    # ctx.set_attr, and trigger twice against a deterministic 5-row source. Run 1
    # reads all rows (id > 0), run 2 reads only rows above the persisted watermark
    # (id > 3) -- verifying the seed applies and the advanced value persists.
    db = sql(domain_mssql)
    src_table = f"td_iv_{uuid.uuid4().hex[:8]}"
    db.create_int_table(src_table, [{"id": i, "name": f"n{i}"} for i in range(1, 6)])
    query = f"SELECT id, name FROM {db.quote(src_table)} WHERE id > :wm ORDER BY id"
    project_root = tmp_path / "pub"
    project_root.mkdir()
    info = write_watermark_publisher(
        project_root,
        source_import=_SRC_IMPORT,
        source_expr=f'MSSQLSrc(queries=[{query!r}], initial_values={{"wm": 0}})',
        output_table=_TABLE,
        bind="wm",
        advance_to=3,
    )
    collection = collection_factory("sources", _src_conn(domain_mssql))
    try:
        function = collection.register_function(
            info=info, project_root=str(project_root)
        )
        status = wait_for_completion(function.trigger())
        assert status in EXECUTION_SUCCESSFUL_FINAL_STATUSES
        first = collection.get_table(_TABLE).sample()
        assert sorted(first["id"].to_list()) == [1, 2, 3, 4, 5]

        status = wait_for_completion(function.trigger())
        assert status in EXECUTION_SUCCESSFUL_FINAL_STATUSES
        second = collection.get_table(_TABLE).sample()
        assert sorted(second["id"].to_list()) == [4, 5]
    finally:
        db.drop_table(src_table)
