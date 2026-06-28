from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeDestConn

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _impl as impl


pytestmark = pytest.mark.snowflake


def _credentials() -> UserPassword:
    return UserPassword(
        user=Secret("etl"),
        password=Secret("hunter2"),
    )


def _conn(**overrides) -> SnowflakeDestConn:
    kwargs: dict = dict(
        account="ACME-MAIN",
        credentials=_credentials(),
        role="ETL_ROLE",
        database="ANALYTICS",
        schema="PUBLIC",
        warehouse="ETL_WH",
        stage="MY_STAGE",
    )
    kwargs.update(overrides)
    return SnowflakeDestConn(**kwargs)


# ---------------------------------------------------------------------------
# Connection-parameter assembly (the kwargs we hand to
# ``snowflake.connector.connect``).
# ---------------------------------------------------------------------------


def test_connection_parameters_pulls_account_user_password():
    params = impl._connection_parameters(_conn())
    assert params["account"] == "ACME-MAIN"
    assert params["user"] == "etl"
    assert params["password"] == "hunter2"


def test_connection_parameters_includes_role_db_schema_warehouse():
    params = impl._connection_parameters(_conn())
    assert params["role"] == "ETL_ROLE"
    assert params["database"] == "ANALYTICS"
    assert params["schema"] == "PUBLIC"
    assert params["warehouse"] == "ETL_WH"


def test_connection_parameters_omits_optional_fields_when_missing():
    conn = SnowflakeDestConn(account="ACME-MAIN", credentials=_credentials())
    params = impl._connection_parameters(conn)
    for k in ("role", "database", "schema", "warehouse"):
        assert k not in params


# ---------------------------------------------------------------------------
# _fq_for_sql -- unfqn -> fqn round-trip
#
# Inputs to ``write_all_tables`` may be bare dotted FQNs OR have
# per-part double-quoted spellings (the SnowflakeTableNameSpec
# validator accepts both). The impl routes every embedded reference
# through ``_fq_for_sql`` so the SQL emitted by COPY / TRUNCATE /
# CREATE TABLE is always per-part double-quoted regardless of how
# the user spelled the original name.
# ---------------------------------------------------------------------------


def test_fq_for_sql_quotes_bare_three_part_name():
    assert impl._fq_for_sql("DB.S.T") == '"DB"."S"."T"'


def test_fq_for_sql_quotes_bare_two_part_name():
    assert impl._fq_for_sql("S.T") == '"S"."T"'


def test_fq_for_sql_quotes_bare_one_part_name():
    assert impl._fq_for_sql("T") == '"T"'


def test_fq_for_sql_is_idempotent_on_quoted_input():
    quoted = '"DB"."S"."T"'
    assert impl._fq_for_sql(quoted) == quoted


def test_fq_for_sql_preserves_special_chars_in_quoted_input():
    # Hyphenated database name is legal once double-quoted.
    quoted = '"my-db"."public"."my_table"'
    assert impl._fq_for_sql(quoted) == quoted


def test_fq_for_sql_preserves_doubled_double_quote_escape():
    # ``""`` inside a quoted identifier represents a literal ``"``;
    # the round-trip strips the escape on parse and re-applies it on
    # emit, leaving the wire form unchanged.
    quoted = '"weird""name"'
    assert impl._fq_for_sql(quoted) == quoted


def test_fq_for_sql_round_trip_strips_then_reapplies_quotes():
    # Mixed-quoting input: bare DB + quoted schema + bare table.
    out = impl._fq_for_sql('DB."weird schema".T')
    assert out == '"DB"."weird schema"."T"'


# ---------------------------------------------------------------------------
# Stage resolution
# ---------------------------------------------------------------------------


def test_stage_uses_conn_stage_as_permanent_when_set():
    stage, is_temp = impl._stage(_conn())
    assert stage == "MY_STAGE"
    assert is_temp is False


def test_stage_falls_back_to_session_temp_stage_when_unset():
    conn = SnowflakeDestConn(account="ACME-MAIN", credentials=_credentials())
    stage, is_temp = impl._stage(conn)
    assert stage == "tabsdata_temp_stage"
    assert is_temp is True


# ---------------------------------------------------------------------------
# build_snowflake_connection -- driver kwargs handoff
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, log: list[str]) -> None:
        self._log = log
        self.closed = False

    def execute(self, sql: str) -> None:
        self._log.append(sql)

    def close(self) -> None:
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class _FakeConnection:
    def __init__(self) -> None:
        self.log: list[str] = []
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self.log)

    def close(self) -> None:
        self.closed = True


def test_build_snowflake_connection_calls_connect_with_resolved_params(monkeypatch):
    captured: dict = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return _FakeConnection()

    # Stub ``snowflake.connector.connect`` so the test doesn't need
    # the driver installed.
    import sys
    import types

    sf_mod = types.ModuleType("snowflake")
    sf_conn = types.ModuleType("snowflake.connector")
    sf_conn.connect = fake_connect
    sf_mod.connector = sf_conn
    monkeypatch.setitem(sys.modules, "snowflake", sf_mod)
    monkeypatch.setitem(sys.modules, "snowflake.connector", sf_conn)

    impl.build_snowflake_connection(_conn())
    assert captured["account"] == "ACME-MAIN"
    assert captured["user"] == "etl"
    assert captured["password"] == "hunter2"
    assert captured["role"] == "ETL_ROLE"
    assert captured["database"] == "ANALYTICS"
    assert captured["schema"] == "PUBLIC"
    assert captured["warehouse"] == "ETL_WH"


# ---------------------------------------------------------------------------
# write_all_tables -- SQL sequence + close-on-exit
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_sf_connection(monkeypatch):
    """Replace ``impl.build_snowflake_connection`` with a stub that
    returns a fake connection capturing every executed SQL string."""
    fake = _FakeConnection()
    monkeypatch.setattr(impl, "build_snowflake_connection", lambda conn: fake)
    return fake


def test_write_all_tables_drives_use_warehouse_first(
    fake_sf_connection, tmp_path
):
    parquet = tmp_path / "in.parquet"
    parquet.write_bytes(b"\x00")
    cfg = impl.SnowflakeWriteCfg(
        conn=_conn(),
        target_tables=["ANALYTICS.PUBLIC.T"],
        if_table_exists="append",
    )
    impl.write_all_tables(cfg, [parquet])
    assert fake_sf_connection.log[0] == "USE WAREHOUSE ETL_WH"


def test_write_all_tables_creates_stage_puts_then_copies(
    fake_sf_connection, tmp_path
):
    parquet = tmp_path / "in.parquet"
    parquet.write_bytes(b"\x00")
    cfg = impl.SnowflakeWriteCfg(
        conn=_conn(),
        target_tables=["ANALYTICS.PUBLIC.T"],
        if_table_exists="append",
    )
    impl.write_all_tables(cfg, [parquet])
    sql = fake_sf_connection.log
    # Order: USE WAREHOUSE, CREATE STAGE, CREATE FILE FORMAT, CREATE TABLE,
    # PUT, COPY INTO. Tables go through _fq_for_sql so each part is
    # double-quoted in the emitted SQL.
    assert any(s.startswith("CREATE STAGE IF NOT EXISTS MY_STAGE") for s in sql)
    assert any(s.startswith("PUT file://") for s in sql)
    assert any(
        s.startswith('COPY INTO "ANALYTICS"."PUBLIC"."T"') for s in sql
    )


def test_write_all_tables_uses_temporary_stage_when_conn_stage_unset(
    fake_sf_connection, tmp_path
):
    parquet = tmp_path / "in.parquet"
    parquet.write_bytes(b"\x00")
    conn = SnowflakeDestConn(
        account="ACME-MAIN",
        credentials=_credentials(),
        warehouse="ETL_WH",
        database="ANALYTICS",
        schema="PUBLIC",
    )
    cfg = impl.SnowflakeWriteCfg(
        conn=conn,
        target_tables=["ANALYTICS.PUBLIC.T"],
        if_table_exists="append",
    )
    impl.write_all_tables(cfg, [parquet])
    sql = fake_sf_connection.log
    assert any(
        s.startswith("CREATE TEMPORARY STAGE IF NOT EXISTS tabsdata_temp_stage")
        for s in sql
    )


def test_write_all_tables_truncates_before_copy_on_replace(
    fake_sf_connection, tmp_path
):
    parquet = tmp_path / "in.parquet"
    parquet.write_bytes(b"\x00")
    cfg = impl.SnowflakeWriteCfg(
        conn=_conn(),
        target_tables=["ANALYTICS.PUBLIC.T"],
        if_table_exists="replace",
    )
    impl.write_all_tables(cfg, [parquet])
    sql = fake_sf_connection.log
    truncate_idx = next(
        i
        for i, s in enumerate(sql)
        if s == 'TRUNCATE "ANALYTICS"."PUBLIC"."T"'
    )
    copy_idx = next(
        i
        for i, s in enumerate(sql)
        if s.startswith('COPY INTO "ANALYTICS"."PUBLIC"."T"')
    )
    assert truncate_idx < copy_idx


def test_write_all_tables_does_not_truncate_on_append(
    fake_sf_connection, tmp_path
):
    parquet = tmp_path / "in.parquet"
    parquet.write_bytes(b"\x00")
    cfg = impl.SnowflakeWriteCfg(
        conn=_conn(),
        target_tables=["ANALYTICS.PUBLIC.T"],
        if_table_exists="append",
    )
    impl.write_all_tables(cfg, [parquet])
    assert not any(s.startswith("TRUNCATE") for s in fake_sf_connection.log)


def test_write_all_tables_iterates_one_per_table(
    fake_sf_connection, tmp_path
):
    p1 = tmp_path / "a.parquet"
    p1.write_bytes(b"\x00")
    p2 = tmp_path / "b.parquet"
    p2.write_bytes(b"\x00")
    cfg = impl.SnowflakeWriteCfg(
        conn=_conn(),
        target_tables=["DB.S.T1", "DB.S.T2"],
        if_table_exists="append",
    )
    impl.write_all_tables(cfg, [p1, p2])
    copies = [s for s in fake_sf_connection.log if s.startswith("COPY INTO ")]
    assert len(copies) == 2
    assert 'COPY INTO "DB"."S"."T1"' in copies[0]
    assert 'COPY INTO "DB"."S"."T2"' in copies[1]


def test_write_all_tables_closes_connection_on_exit(
    fake_sf_connection, tmp_path
):
    parquet = tmp_path / "in.parquet"
    parquet.write_bytes(b"\x00")
    cfg = impl.SnowflakeWriteCfg(
        conn=_conn(),
        target_tables=["ANALYTICS.PUBLIC.T"],
        if_table_exists="append",
    )
    impl.write_all_tables(cfg, [parquet])
    assert fake_sf_connection.closed is True


def test_write_all_tables_closes_connection_on_failure(
    monkeypatch, tmp_path
):
    parquet = tmp_path / "in.parquet"
    parquet.write_bytes(b"\x00")

    fake = _FakeConnection()

    def cursor_that_fails(*_args, **_kwargs):
        class _BoomCursor(_FakeCursor):
            def execute(self, sql: str) -> None:  # noqa: ARG002
                raise RuntimeError("boom")
        return _BoomCursor(fake.log)

    fake.cursor = cursor_that_fails  # type: ignore[assignment]
    monkeypatch.setattr(impl, "build_snowflake_connection", lambda conn: fake)

    cfg = impl.SnowflakeWriteCfg(
        conn=_conn(),
        target_tables=["ANALYTICS.PUBLIC.T"],
        if_table_exists="append",
    )
    with pytest.raises(RuntimeError, match="boom"):
        impl.write_all_tables(cfg, [parquet])
    assert fake.closed is True
