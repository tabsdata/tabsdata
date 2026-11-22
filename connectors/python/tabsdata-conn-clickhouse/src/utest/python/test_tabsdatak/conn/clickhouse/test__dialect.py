#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for ``_dialect.py`` -- focused on the FQN/escape helpers
and the ``ClickHouseDialect`` SQL generation contract.

The helpers ``clickhouse_fqn`` / ``clickhouse_fqn_parts`` are tested
directly (no client required). Dialect methods are tested with a stub
client and a mocked ``clickhouse_connect.get_client`` so we can
inspect the SQL strings the dialect emits.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._clickhouse_config import ClickHouseConfig
# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._dialect import (
    ClickHouseDialect,
    clickhouse_fqn,
    clickhouse_fqn_parts,
)
from tabsdatak.conn.clickhouse.error import (
    ClickHouseErrorCode,
    ClickHouseRuntimeException,
)


pytestmark = pytest.mark.clickhouse


# ---------------------------------------------------------------------------
# FQN helpers
# ---------------------------------------------------------------------------


class TestClickhouseFqnParts:
    def test_one_part_uses_default_db(self):
        assert clickhouse_fqn_parts("users", "analytics") == (
            "analytics",
            "users",
        )

    def test_two_parts_kept_as_is(self):
        assert clickhouse_fqn_parts(
            "warehouse.users", "analytics"
        ) == ("warehouse", "users")

    def test_quoted_part_unescaped(self):
        # Backtick-quoted parts are unwrapped (raw identifier).
        assert clickhouse_fqn_parts(
            "`weird name`.users", "analytics"
        ) == ("weird name", "users")

    def test_doubled_backtick_is_literal(self):
        # ``with``embedded backtick`` -> literal ``with`embedded backtick``.
        assert clickhouse_fqn_parts(
            "`with``embedded backtick`", "db"
        ) == ("db", "with`embedded backtick")

    def test_quoted_db_and_table(self):
        assert clickhouse_fqn_parts(
            "`weird db`.`weird tbl`", "default"
        ) == ("weird db", "weird tbl")


class TestClickhouseFqn:
    def test_one_part_prepends_default_db(self):
        assert clickhouse_fqn("users", "analytics") == (
            "`analytics`.`users`"
        )

    def test_two_parts_kept(self):
        assert clickhouse_fqn(
            "warehouse.users", "analytics"
        ) == "`warehouse`.`users`"

    def test_quoted_part_round_trips_with_escape(self):
        # User wrote ``some`tbl`` quoted (so the raw identifier is
        # ``some`tbl``); the rendered SQL must double the embedded
        # backtick.
        assert clickhouse_fqn(
            "`some``tbl`", "analytics"
        ) == "`analytics`.`some``tbl`"

    def test_quoted_db_and_table_render(self):
        assert clickhouse_fqn(
            "`weird db`.`weird tbl`", "default"
        ) == "`weird db`.`weird tbl`"

    def test_three_parts_raises(self):
        # max_parts=2 enforced by unfqn.
        with pytest.raises(ValueError, match="exceeds total_parts=2"):
            clickhouse_fqn("a.b.c", "default")


# ---------------------------------------------------------------------------
# ClickHouseDialect: SQL generation uses FQN+escape
# ---------------------------------------------------------------------------


class _CapturingClient:
    """Stub for ``clickhouse_connect`` client: records every
    ``command`` / ``query`` call so tests can assert the SQL produced.
    """

    def __init__(self) -> None:
        self.commands: list[str] = []
        self.queries: list[tuple[str, dict]] = []
        self._table_exists = False
        self._columns_rows: list = []
        self._engine: str | None = "Atomic"
        self._partition_key = ""
        self._partitions: int | Exception = 0
        self._limit = 100

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def query(self, sql: str, parameters: dict | None = None):
        self.queries.append((sql, dict(parameters or {})))
        # Stubbed responses for the query patterns the dialect uses.
        result = MagicMock()
        if "system.databases" in sql:
            result.result_rows = [] if self._engine is None else [[self._engine]]
        elif "partition_key" in sql:
            result.result_rows = [[self._partition_key]]
        elif "uniqExact" in sql:
            if isinstance(self._partitions, Exception):
                raise self._partitions
            result.result_rows = [[self._partitions]]
        elif "getSetting" in sql:
            result.result_rows = [[self._limit]]
        elif "system.tables" in sql:
            result.result_rows = [[1 if self._table_exists else 0]]
        elif "system.columns" in sql:
            result.result_rows = self._columns_rows
        else:
            result.result_rows = []
        return result


@pytest.fixture
def captured_client(monkeypatch):
    """Patch ``clickhouse_connect.get_client`` to return a
    ``_CapturingClient``. Returns the client instance so the test can
    inspect / configure it.
    """
    client = _CapturingClient()
    fake_ch = types.SimpleNamespace(get_client=lambda **_kw: client)
    monkeypatch.setitem(sys.modules, "clickhouse_connect", fake_ch)
    return client


def _config(db: str = "analytics") -> ClickHouseConfig:
    return ClickHouseConfig(
        host="ch.example.invalid",
        port=8123,
        database=db,
        username="u",
        password="p",
        secure=False,
    )


def _arrow_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )


class TestDialectCreateTable:
    def test_unqualified_name_uses_default_db(self, captured_client):
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        d.create_table(MagicMock(), "users", _arrow_schema())
        assert len(captured_client.commands) == 1
        ddl = captured_client.commands[0]
        assert "CREATE TABLE IF NOT EXISTS `analytics`.`users`" in ddl

    def test_qualified_name_overrides_default_db(
        self, captured_client
    ):
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        d.create_table(MagicMock(), "warehouse.events", _arrow_schema())
        ddl = captured_client.commands[0]
        assert "CREATE TABLE IF NOT EXISTS `warehouse`.`events`" in ddl

    def test_quoted_table_name_unwrapped_and_reescaped(
        self, captured_client
    ):
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        d.create_table(
            MagicMock(),
            "`weird name`",
            _arrow_schema(),
        )
        ddl = captured_client.commands[0]
        assert "CREATE TABLE IF NOT EXISTS `analytics`.`weird name`" in ddl

    def test_embedded_backtick_doubled_in_output(
        self, captured_client
    ):
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        # User supplied ``some``tbl`` (raw identifier ``some`tbl``);
        # rendered SQL must double the embedded backtick.
        d.create_table(
            MagicMock(),
            "`some``tbl`",
            _arrow_schema(),
        )
        ddl = captured_client.commands[0]
        assert "CREATE TABLE IF NOT EXISTS `analytics`.`some``tbl`" in ddl


class TestDialectCreateTableColumns:
    def test_a_backtick_in_a_column_name_is_escaped(self, captured_client):
        import pyarrow as pa

        d = ClickHouseDialect(_config("analytics"), MagicMock())
        d.create_table(
            MagicMock(), "t", pa.schema([pa.field("we`ird", pa.int64(), False)])
        )
        assert "`we``ird` Int64" in captured_client.commands[0]


class TestDialectAlterTable:
    """Verify the FQN baked into the schema-evolution delta is
    correctly assembled.

    The renderer emits ``ALTER TABLE <delta.table_name> ...``. We need
    the dialect to set ``delta.table_name`` to the escaped FQN built
    from the connection's database + the user's table name.
    """

    def test_get_evolution_table_schema_uses_fqn_for_table_name(
        self, captured_client
    ):
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        captured_client._columns_rows = [
            ("id", "Int64", "", ""),
        ]
        ts = d.get_evolution_table_schema("users")
        # The schema's ``table_name`` carries the escaped FQN used by
        # the renderer to assemble ``ALTER TABLE <fq>``.
        assert ts.table_name == "`analytics`.`users`"

    def test_get_evolution_table_schema_qualified_name(
        self, captured_client
    ):
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        captured_client._columns_rows = [
            ("id", "Int64", "", ""),
        ]
        ts = d.get_evolution_table_schema("warehouse.events")
        assert ts.table_name == "`warehouse`.`events`"

    def test_get_evolution_table_schema_quotes_db_and_table(
        self, captured_client
    ):
        d = ClickHouseDialect(_config("default"), MagicMock())
        captured_client._columns_rows = []
        ts = d.get_evolution_table_schema("`weird db`.`weird tbl`")
        assert ts.table_name == "`weird db`.`weird tbl`"


class TestDialectSwap:
    @pytest.mark.parametrize(
        "name,swap",
        [
            ("users", "`analytics`.`users__td_swap`"),
            ("warehouse.events", "`warehouse`.`events__td_swap`"),
            ("`we``ird`", "`analytics`.`we``ird__td_swap`"),
        ],
    )
    def test_the_swap_table_sits_next_to_its_table(self, captured_client, name, swap):
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        assert d.swap_table_name(name) == swap
        d.copy_table(MagicMock(), d.swap_table_name(name), name)
        assert captured_client.commands == [
            f"CREATE TABLE {swap} AS {clickhouse_fqn(name, 'analytics')}"
        ]

    def test_exchange_and_drop_use_fqn(self, captured_client):
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        d.exchange_tables(MagicMock(), "users__td_swap", "users")
        d.drop_table(MagicMock(), "warehouse.events")
        assert captured_client.commands == [
            "EXCHANGE TABLES `analytics`.`users__td_swap` AND `analytics`.`users`",
            "DROP TABLE IF EXISTS `warehouse`.`events` SYNC",
        ]

    @pytest.mark.parametrize("engine", ["Atomic", "Shared", "Replicated"])
    def test_databases_that_exchange_are_swappable(self, captured_client, engine):
        captured_client._engine = engine
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        d.check_swappable(MagicMock(), "warehouse.events")
        sql, params = captured_client.queries[0]
        assert "system.databases" in sql
        assert params == {"db": "warehouse"}

    @pytest.mark.parametrize("engine", ["Ordinary", "Lazy", "MySQL", None])
    def test_other_databases_are_refused(self, captured_client, engine):
        captured_client._engine = engine
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        with pytest.raises(ClickHouseRuntimeException) as ei:
            d.check_swappable(MagicMock(), "users")
        assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_13

    def test_a_delta_applies_to_the_table_named_not_the_one_it_came_from(
        self, captured_client
    ):
        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._schema.delta import (
            Action,
            SchemaChange,
            SchemaDelta,
        )

        d = ClickHouseDialect(_config("analytics"), MagicMock())
        delta = SchemaDelta(
            table_name="`analytics`.`users`",
            changes=[
                SchemaChange(
                    action=Action.ADD_COLUMN,
                    column="extra",
                    safe=True,
                    detail={"type": "String", "nullable": True},
                )
            ],
        )
        d.apply_schema_delta(MagicMock(), "users__td_swap", delta)
        assert captured_client.commands
        assert all(
            "`analytics`.`users__td_swap`" in command
            for command in captured_client.commands
        )


class TestDialectBulkLoad:
    def test_bulk_load_insert_uses_fqn(self, captured_client):
        cloud = MagicMock()
        cloud.clickhouse_table_function.return_value = (
            "s3('https://...', 'Parquet')"
        )
        d = ClickHouseDialect(_config("analytics"), cloud)
        d.bulk_load(MagicMock(), "users", ["ref1"], ["id"])
        assert captured_client.commands == [
            "INSERT INTO `analytics`.`users` (`id`) SELECT `id` FROM "
            "s3('https://...', 'Parquet')"
        ]

    def test_bulk_load_qualified_name(self, captured_client):
        cloud = MagicMock()
        cloud.clickhouse_table_function.return_value = (
            "s3('https://...', 'Parquet')"
        )
        d = ClickHouseDialect(_config("analytics"), cloud)
        d.bulk_load(MagicMock(), "warehouse.events", ["ref1"], ["id"])
        assert captured_client.commands == [
            "INSERT INTO `warehouse`.`events` (`id`) SELECT `id` FROM "
            "s3('https://...', 'Parquet')"
        ]

    def test_bulk_load_matches_the_staged_columns_by_name(self, captured_client):
        """A table whose columns are in another order, or that has more of
        them, must still get each value in its own column."""
        cloud = MagicMock()
        cloud.clickhouse_table_function.return_value = "s3('x', 'Parquet')"
        d = ClickHouseDialect(_config("analytics"), cloud)
        d.bulk_load(MagicMock(), "users", ["ref1"], ["name", "id", "we`ird"])
        assert captured_client.commands == [
            "INSERT INTO `analytics`.`users` (`name`, `id`, `we``ird`) "
            "SELECT `name`, `id`, `we``ird` FROM s3('x', 'Parquet')"
        ]

    @pytest.mark.parametrize(
        "key,partitions,limit,settings",
        [
            ("", 500, 100, ""),
            ("toYYYYMM(day)", 50, 100, ""),
            ("toYYYYMM(day)", 100, 100, ""),
            (
                "toYYYYMM(day)",
                150,
                100,
                " SETTINGS max_partitions_per_insert_block = 150",
            ),
            ("(toYYYYMM(day), id)", 150, 0, ""),
            ("toYYYYMM(day)", RuntimeError("no column day"), 100, ""),
        ],
    )
    def test_an_insert_spanning_more_partitions_than_allowed_raises_the_limit(
        self, captured_client, key, partitions, limit, settings
    ):
        captured_client._partition_key = key
        captured_client._partitions = partitions
        captured_client._limit = limit
        cloud = MagicMock()
        cloud.clickhouse_table_function.return_value = "s3('x', 'Parquet')"
        d = ClickHouseDialect(_config("analytics"), cloud)
        d.bulk_load(MagicMock(), "users", ["ref1"], ["id"])
        assert captured_client.commands == [
            "INSERT INTO `analytics`.`users` (`id`) SELECT `id` FROM "
            f"s3('x', 'Parquet'){settings}"
        ]
        if key:
            assert (
                f"SELECT uniqExact({key}) FROM s3('x', 'Parquet')",
                {},
            ) in captured_client.queries


class TestDialectSystemQueries:
    """``table_exists`` / ``get_table_schema`` / ``_get_raw_columns``
    use parameterized queries with bare db/table parts (NOT the
    backtick-escaped FQN -- those would not match
    ``system.columns.name``).
    """

    def test_table_exists_passes_bare_parts_to_system_tables(
        self, captured_client
    ):
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        d.table_exists(MagicMock(), "warehouse.events")
        sql, params = captured_client.queries[0]
        assert "system.tables" in sql
        assert params == {"db": "warehouse", "tbl": "events"}

    def test_get_table_schema_unqualified_uses_default_db(
        self, captured_client
    ):
        captured_client._columns_rows = []
        d = ClickHouseDialect(_config("analytics"), MagicMock())
        d.get_table_schema(MagicMock(), "users")
        sql, params = captured_client.queries[0]
        assert "system.columns" in sql
        assert params == {"db": "analytics", "tbl": "users"}

    def test_system_query_uses_unescaped_identifier(
        self, captured_client
    ):
        """Backtick-quoted user input is unescaped before being
        passed as a query parameter -- the raw identifier is what
        ``system.columns.name`` stores."""
        captured_client._columns_rows = []
        d = ClickHouseDialect(_config("default"), MagicMock())
        d.get_table_schema(MagicMock(), "`weird name`")
        _, params = captured_client.queries[0]
        assert params == {"db": "default", "tbl": "weird name"}
