#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end MySQL connector tests: register a real function on a running
tabsdata instance, trigger it, and assert the outcome against a tdx-provisioned
``mysql`` domain -- unlike ``test__plugin_integration.py``, which drives the
plugin directly with a fabricated context.

The generic harness (``server`` / ``collection_factory`` fixtures, ``run_function``,
``assert_frames_match``) and the shared subscriber module writer live under
``tabsdatax.itest.fixture.functions`` (``harness`` / ``files``); only the MySQL
specifics are here. The publisher is written locally: a SQL ``Src`` maps one
query to one positional ``TableFrame`` (not the array-mode a file ``Src`` gets).
Requires the ``tdtest`` instance up and the ``mysql`` domain provisioned with
data (``tdx-domain create --sys mysql --domain mysql --model world``).
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.mysql import MysqlDomain, mysql_domain
from tabsdatax.itest.fixture.functions.files import write_subscriber_module
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
    wait_for_completion,
    write_module,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mysql import MySQLDestConn, MySQLSrcConn
from tabsdatak.sdk.status_utils.execution import EXECUTION_SUCCESSFUL_FINAL_STATUSES
from tabsdatak.sdk.tabsdata_server import FunctionInfo

pytestmark = pytest.mark.mysql

_SRC_IMPORT = "from tabsdatak.conn.mysql import MySQLSrc"
_DEST_IMPORT = "from tabsdatak.conn.mysql import MySQLDest"
_TABLE = "e2e_mysql_tbl"


def _conn_uri(domain: MysqlDomain) -> str:
    return f"mysql+pymysql://{domain.host}:{domain.port}/{domain.db}"


def _credentials(domain: MysqlDomain) -> UserPassword:
    return UserPassword(user=Secret(domain.user), password=Secret(domain.password))


def _src_conn(domain: MysqlDomain) -> MySQLSrcConn:
    return MySQLSrcConn(uri=_conn_uri(domain), credentials=_credentials(domain))


def _dest_conn(domain: MysqlDomain) -> MySQLDestConn:
    return MySQLDestConn(uri=_conn_uri(domain), credentials=_credentials(domain))


def _entity(domain: MysqlDomain) -> str:
    """A deterministic provisioned table name, or skip if the domain is unseeded."""
    if not domain.entities:
        pytest.skip(
            f"domain {domain.name!r} has no entities (model={domain.model!r}); "
            f"provision the domain with data before running this test",
        )
    return sorted(domain.entities)[0]


def _write_sql_publisher(
    project_root: Path, *, source_expr: str, output_table: str
) -> FunctionInfo:
    """A publisher reading one query into ``output_table``. Unlike a file ``Src``
    (array-mode: one path may glob to many files), a SQL ``Src`` maps each query
    to a single positional ``TableFrame``, so a one-query source yields one frame."""
    module = write_module(
        project_root,
        "e2e_publisher",
        "from tabsdatak.api import publisher, TableFrameSpec\n"
        f"{_SRC_IMPORT}\n"
        "\n"
        f"@publisher(source={source_expr}, output_tables=[{output_table!r}])\n"
        "def e2e_pub(frame: TableFrameSpec) -> TableFrameSpec:\n"
        "    return frame\n",
    )
    return module.e2e_pub


def _publish(
    collection_factory: CollectionFactory,
    domain_mysql: MysqlDomain,
    project_root: Path,
    entity: str,
) -> object:
    """Publish ``SELECT * FROM <entity>`` into a fresh source collection; return
    the collection."""
    query = f"SELECT * FROM `{entity}`"
    info = _write_sql_publisher(
        project_root,
        source_expr=f"MySQLSrc(queries=[{query!r}])",
        output_table=_TABLE,
    )
    collection = collection_factory("sources", _src_conn(domain_mysql))
    run_function(collection, info, project_root)
    return collection


@tabsdata_instance(consumer="tdtest")
@mysql_domain("mysql")
def test_publisher_reads_table_from_mysql_domain(
    instance_tdtest: Instance,
    domain_mysql: MysqlDomain,
    collection_factory: CollectionFactory,
    sql,
    tmp_path: Path,
) -> None:
    entity = _entity(domain_mysql)
    expected = sql(domain_mysql).read_table(entity)
    project_root = tmp_path / "pub"
    project_root.mkdir()
    collection = _publish(collection_factory, domain_mysql, project_root, entity)

    published = collection.get_table(_TABLE)
    assert published is not None, f"table {_TABLE!r} not found after publish"
    assert_frames_match(published.sample(), expected)


@tabsdata_instance(consumer="tdtest")
@mysql_domain("mysql")
def test_subscriber_writes_table_to_mysql_domain(
    instance_tdtest: Instance,
    domain_mysql: MysqlDomain,
    collection_factory: CollectionFactory,
    sql,
    tmp_path: Path,
) -> None:
    # Round-trip: publish a provisioned table into a tabsdata table, then
    # subscribe it back out to a scratch mysql table and assert it matches.
    # OS/kminus does not auto-trigger subscribers, so trigger it directly.
    db = sql(domain_mysql)
    entity = _entity(domain_mysql)
    expected = db.read_table(entity)
    publisher_root = tmp_path / "pub"
    publisher_root.mkdir()
    source_collection = _publish(
        collection_factory, domain_mysql, publisher_root, entity
    )

    out_table = f"e2e_out_{uuid.uuid4().hex[:8]}"
    subscriber_root = tmp_path / "sub"
    subscriber_root.mkdir()
    subscriber_info = write_subscriber_module(
        subscriber_root,
        dest_import=_DEST_IMPORT,
        dest_expr=f"MySQLDest(tables=[{out_table!r}], if_table_exists='replace')",
        input_table=f"{source_collection.name}/{_TABLE}",
    )
    destination_collection = collection_factory(
        "destinations", _dest_conn(domain_mysql)
    )
    try:
        run_function(destination_collection, subscriber_info, subscriber_root)
        assert_frames_match(db.read_table(out_table), expected)
    finally:
        db.drop_table(out_table)


@tabsdata_instance(consumer="tdtest")
@mysql_domain("mysql")
def test_publisher_initial_values_advance_across_runs(
    instance_tdtest: Instance,
    domain_mysql: MysqlDomain,
    collection_factory: CollectionFactory,
    sql,
    tmp_path: Path,
) -> None:
    # Incremental publish: the publisher seeds a watermark bind (wm=0) and, each
    # run, advances it to 3 via ``ctx.set_attr``. Triggered twice against a
    # deterministic 5-row source: run 1 reads all rows (id > 0), run 2 reads only
    # rows above the persisted watermark (id > 3). This verifies both that the
    # ``initial_values`` seed applies on the first run and that the advanced value
    # persists to (and is re-applied on) the next run.
    db = sql(domain_mysql)
    src_table = f"td_iv_{uuid.uuid4().hex[:8]}"
    db.create_int_table(src_table, [{"id": i, "name": f"n{i}"} for i in range(1, 6)])
    query = f"SELECT id, name FROM `{src_table}` WHERE id > :wm ORDER BY id"
    project_root = tmp_path / "pub"
    project_root.mkdir()
    module = write_module(
        project_root,
        "e2e_publisher",
        "from tabsdatak.api import publisher, TableFrameSpec, TrxCtx\n"
        f"{_SRC_IMPORT}\n"
        "\n"
        f"@publisher(source=MySQLSrc(queries=[{query!r}], "
        f"initial_values={{'wm': 0}}), output_tables=[{_TABLE!r}])\n"
        "def e2e_pub(frame: TableFrameSpec, ctx: TrxCtx) -> TableFrameSpec:\n"
        "    ctx.set_attr('wm', 3)\n"
        "    return frame\n",
    )
    collection = collection_factory("sources", _src_conn(domain_mysql))
    try:
        function = collection.register_function(
            info=module.e2e_pub, project_root=str(project_root)
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
