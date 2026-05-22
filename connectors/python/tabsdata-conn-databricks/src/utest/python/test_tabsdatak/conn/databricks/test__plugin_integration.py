"""End-to-end Databricks plugin tests against a real Databricks
workspace.

Databricks is a managed cloud platform -- there's no docker fallback
the way there is for MySQL / Postgres / ClickHouse. The fixture
expects an externally-configured workspace and Personal Access Token
exposed via the same env-var convention the legacy
``tabsdata-databricks`` test suite uses (see
``connectors/python/tabsdata-databricks/.../conftest.py``):

  - DBX0__HOST            -- workspace URL
  - DBX0__TOKEN           -- Personal Access Token
  - DBX0__CATALOG         -- Unity Catalog catalog
  - DBX0__SCHEMA          -- schema within the catalog
  - DBX0__VOLUME          -- managed volume name (1- / 2- / 3-part)
  - DBX0__WAREHOUSE_NAME  -- warehouse to execute SQL on

If any of these are missing the whole module skips with a clear
reason. Each test allocates a unique scratch table prefixed with
``itest_<test_name>_<uuid>`` so concurrent / sequential runs don't
collide; tables are dropped before and after.

Error-path coverage (fault-injection via mocks) lives in
``test__plugin.py``.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.databricks import (
    DatabricksDestConn,
    DatabricksSrcConn,
    DatabricksDest,
    DatabricksSrc,
)
# noinspection PyProtectedMember
from tabsdatak.conn.databricks import _plugin as plugin
from tabsdatak.spi import DestContext, SrcPluginCtx


pytestmark = pytest.mark.databricks


# ---------------------------------------------------------------------------
# Env gate -- skip the whole module unless an external Databricks
# workspace is configured (DBX0__* prefix, same as the legacy
# ``tabsdata-databricks`` conftest).
# ---------------------------------------------------------------------------


_PREFIX = "DBX0"


def _env(name: str) -> str | None:
    v = os.environ.get(name)
    return v if v else None


_REQUIRED_ENVS = (
    f"{_PREFIX}__HOST",
    f"{_PREFIX}__TOKEN",
    f"{_PREFIX}__CATALOG",
    f"{_PREFIX}__SCHEMA",
    f"{_PREFIX}__VOLUME",
    f"{_PREFIX}__WAREHOUSE_NAME",
)
_MISSING_ENVS = [name for name in _REQUIRED_ENVS if not _env(name)]
_GATE_OK = not _MISSING_ENVS

pytestmark = [
    pytestmark,
    pytest.mark.skipif(
        not _GATE_OK,
        reason=(
            "Databricks integration tests need "
            + " + ".join(_REQUIRED_ENVS)
            + " ("
            + ", ".join(_MISSING_ENVS or ["all set"])
            + " missing)"
        ),
    ),
    pytest.mark.xdist_group("databricks-plugin_integration_seq"),
]


# ---------------------------------------------------------------------------
# Test context shims -- concrete SPI subclasses so integration runs
# touch the real protocol rather than a duck-typed surrogate.
# ---------------------------------------------------------------------------


class _SrcCtx(SrcPluginCtx):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "databricks-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "databricks-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def store_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass

class _DestCtx(DestContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "databricks-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "databricks-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def store_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def work_dir(tmp_path):
    out = tmp_path / "work"
    out.mkdir()
    return out


@pytest.fixture(scope="session")
def databricks_config():
    """Pin the env-var-derived config once per session.

    Same env vars the legacy ``tabsdata-databricks`` suite reads --
    ``DBX0__{HOST,TOKEN,CATALOG,SCHEMA,VOLUME,WAREHOUSE_NAME}``.
    """

    @dataclass(frozen=True)
    class _Cfg:
        host: str
        token: str
        warehouse: str
        catalog: str
        schema: str
        volume: str

    return _Cfg(
        host=os.environ[f"{_PREFIX}__HOST"],
        token=os.environ[f"{_PREFIX}__TOKEN"],
        warehouse=os.environ[f"{_PREFIX}__WAREHOUSE_NAME"],
        catalog=os.environ[f"{_PREFIX}__CATALOG"],
        schema=os.environ[f"{_PREFIX}__SCHEMA"],
        volume=os.environ[f"{_PREFIX}__VOLUME"],
    )


def _src_conn_from(cfg) -> DatabricksSrcConn:
    return DatabricksSrcConn(
        host_url=Secret.lit(cfg.host),
        token=Secret.lit(cfg.token),
        volume=Secret.lit(cfg.volume),
        warehouse=Secret.lit(cfg.warehouse),
        catalog=Secret.lit(cfg.catalog),
        schema=Secret.lit(cfg.schema),
    )


def _dest_conn_from(cfg) -> DatabricksDestConn:
    return DatabricksDestConn(
        host_url=Secret.lit(cfg.host),
        token=Secret.lit(cfg.token),
        volume=Secret.lit(cfg.volume),
        warehouse=Secret.lit(cfg.warehouse),
        catalog=Secret.lit(cfg.catalog),
        schema=Secret.lit(cfg.schema),
    )


@pytest.fixture
def scratch_table(databricks_config, request):
    """Allocate a per-test fully-qualified table name and drop it
    before and after via ``databricks.sql``."""
    base = (request.node.originalname or request.node.name).replace(
        "test_", ""
    )
    suffix = uuid.uuid4().hex[:8]
    fq = f"{databricks_config.catalog}.{databricks_config.schema}.itest_{base}_{suffix}"
    # noinspection PyProtectedMember
    from tabsdatak.conn.databricks import _impl

    ws = _impl._build_workspace_client(_src_conn_from(databricks_config))
    sql_conn = _impl._build_sql_connection(
        ws, _src_conn_from(databricks_config)
    )
    cursor = sql_conn.cursor()
    sql_fq = _impl._fq_for_sql(fq)
    cursor.execute(f"DROP TABLE IF EXISTS {sql_fq}")
    yield fq
    cursor.execute(f"DROP TABLE IF EXISTS {sql_fq}")
    sql_conn.close()


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


def _count_rows(databricks_config, fq_table: str) -> int:
    # noinspection PyProtectedMember
    from tabsdatak.conn.databricks import _impl

    ws = _impl._build_workspace_client(_src_conn_from(databricks_config))
    sql_conn = _impl._build_sql_connection(
        ws, _src_conn_from(databricks_config)
    )
    try:
        cursor = sql_conn.cursor()
        cursor.execute(f"SELECT count(*) FROM {_impl._fq_for_sql(fq_table)}")
        return cursor.fetchall()[0][0]
    finally:
        sql_conn.close()


# ---------------------------------------------------------------------------
# Destination scenarios
# ---------------------------------------------------------------------------


def test_dest_plugin_creates_new_table(
    databricks_config, work_dir, scratch_table,
):
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    dest = DatabricksDest(
        conn=_dest_conn_from(databricks_config),
        tables=[scratch_table],
    )
    plugin._DatabricksDestPlugin().write_out(
        _DestCtx(work_dir), dest, [src],
    )
    assert _count_rows(databricks_config, scratch_table) == 2


def test_dest_plugin_appends_to_existing(
    databricks_config, work_dir, scratch_table,
):
    # Seed
    seed = _make_parquet(
        work_dir / "seed.parquet",
        [{"id": 1, "name": "alice"}],
    )
    plugin._DatabricksDestPlugin().write_out(
        _DestCtx(work_dir),
        DatabricksDest(
            conn=_dest_conn_from(databricks_config),
            tables=[scratch_table],
        ),
        [seed],
    )
    # Append
    add = _make_parquet(
        work_dir / "add.parquet",
        [{"id": 2, "name": "bob"}, {"id": 3, "name": "carol"}],
    )
    plugin._DatabricksDestPlugin().write_out(
        _DestCtx(work_dir),
        DatabricksDest(
            conn=_dest_conn_from(databricks_config),
            tables=[scratch_table],
        ),
        [add],
    )
    assert _count_rows(databricks_config, scratch_table) == 3


def test_dest_plugin_replace_clears_existing(
    databricks_config, work_dir, scratch_table,
):
    seed = _make_parquet(
        work_dir / "seed.parquet",
        [{"id": 100, "name": "old"}, {"id": 101, "name": "also_old"}],
    )
    plugin._DatabricksDestPlugin().write_out(
        _DestCtx(work_dir),
        DatabricksDest(
            conn=_dest_conn_from(databricks_config),
            tables=[scratch_table],
        ),
        [seed],
    )
    new = _make_parquet(
        work_dir / "new.parquet",
        [{"id": 1, "name": "only"}],
    )
    plugin._DatabricksDestPlugin().write_out(
        _DestCtx(work_dir),
        DatabricksDest(
            conn=_dest_conn_from(databricks_config),
            tables=[scratch_table],
            if_table_exists="replace",
        ),
        [new],
    )
    assert _count_rows(databricks_config, scratch_table) == 1


# ---------------------------------------------------------------------------
# FQN coverage -- the dialect's ``_fully_qualify_table`` resolves
# 1- / 2- / 3-part table names against ``conn.catalog`` /
# ``conn.schema``, then ``_fq_for_sql`` backtick-wraps each part
# for the SQL it emits. ``test__adapter.py`` covers the algebra at
# the unit level; these tests confirm the full chain lands as an
# actual ``COPY INTO`` against real Databricks for each accepted
# spelling. Each test seeds via the 3-part scratch_table so the
# table exists, then addresses the SAME physical table via the
# variant spelling and asserts the row count.
# ---------------------------------------------------------------------------


def _seed(databricks_config, work_dir, scratch_table):
    src = _make_parquet(
        work_dir / "seed.parquet",
        [{"id": 1, "name": "alice"}],
    )
    plugin._DatabricksDestPlugin().write_out(
        _DestCtx(work_dir),
        DatabricksDest(
            conn=_dest_conn_from(databricks_config),
            tables=[scratch_table],
        ),
        [src],
    )


def test_fqn_bare_table_uses_conn_defaults(
    databricks_config, work_dir, scratch_table,
):
    """1-part bare table -- relies on ``conn.catalog`` +
    ``conn.schema`` for resolution."""
    _seed(databricks_config, work_dir, scratch_table)
    bare = scratch_table.rsplit(".", 1)[-1]
    add = _make_parquet(
        work_dir / "add.parquet",
        [{"id": 2, "name": "bob"}],
    )
    plugin._DatabricksDestPlugin().write_out(
        _DestCtx(work_dir),
        DatabricksDest(
            conn=_dest_conn_from(databricks_config),
            tables=[bare],
        ),
        [add],
    )
    assert _count_rows(databricks_config, scratch_table) == 2


def test_fqn_two_part_uses_conn_catalog(
    databricks_config, work_dir, scratch_table,
):
    """2-part ``<schema>.<table>`` -- relies on ``conn.catalog``
    for the missing leading part."""
    _seed(databricks_config, work_dir, scratch_table)
    _catalog, schema, bare = scratch_table.split(".")
    two_part = f"{schema}.{bare}"
    add = _make_parquet(
        work_dir / "add.parquet",
        [{"id": 2, "name": "bob"}],
    )
    plugin._DatabricksDestPlugin().write_out(
        _DestCtx(work_dir),
        DatabricksDest(
            conn=_dest_conn_from(databricks_config),
            tables=[two_part],
        ),
        [add],
    )
    assert _count_rows(databricks_config, scratch_table) == 2


def test_fqn_quoted_three_part(
    databricks_config, work_dir, scratch_table,
):
    """Each FQN part is supplied backtick-quoted by the caller --
    exercises ``unfqn`` -> ``_fully_qualify_table`` ->
    ``_fq_for_sql`` quote-round-trip end-to-end against the real
    warehouse."""
    _seed(databricks_config, work_dir, scratch_table)
    catalog, schema, bare = scratch_table.split(".")
    quoted = f"`{catalog}`.`{schema}`.`{bare}`"
    add = _make_parquet(
        work_dir / "add.parquet",
        [{"id": 2, "name": "bob"}],
    )
    plugin._DatabricksDestPlugin().write_out(
        _DestCtx(work_dir),
        DatabricksDest(
            conn=_dest_conn_from(databricks_config),
            tables=[quoted],
        ),
        [add],
    )
    assert _count_rows(databricks_config, scratch_table) == 2


# ---------------------------------------------------------------------------
# Source scenarios
# ---------------------------------------------------------------------------


def test_src_plugin_round_trip(
    databricks_config, work_dir, scratch_table,
):
    """Seed a table via the dest plugin, then read it back via the
    src plugin and verify the row count."""
    from tabsdatak.conn.databricks import _impl

    seed = _make_parquet(
        work_dir / "seed.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    plugin._DatabricksDestPlugin().write_out(
        _DestCtx(work_dir),
        DatabricksDest(
            conn=_dest_conn_from(databricks_config),
            tables=[scratch_table],
        ),
        [seed],
    )

    src = DatabricksSrc(
        conn=_src_conn_from(databricks_config),
        queries=[
            f"SELECT id, name FROM {_impl._fq_for_sql(scratch_table)} ORDER BY id"
        ],
    )
    result = plugin._DatabricksSrcPlugin().read_in(
        _SrcCtx(work_dir), src,
    )
    assert len(result) == 1  # one query
    assert len(result[0]) == 1  # one parquet
    df = pl.read_parquet(result[0][0])
    assert df.shape == (2, 2)
    assert df["name"].to_list() == ["alice", "bob"]
