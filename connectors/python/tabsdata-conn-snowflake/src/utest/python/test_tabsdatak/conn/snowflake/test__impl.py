#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql.cdc.constants import CdcOperation
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeDestConn

from test_tabsdatak.conn.snowflake.support import (
    _FakeConnection,
    _INFER_PREFIX,
    _FakeCtx,
    _FakeCursor,
    column_fakes as _column_fakes,
    insert_lists as _insert_lists,
    watermark_answers as _watermark_answers,
)

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _iceberg, _impl as impl
from tabsdatak.conn.snowflake.error import (
    SnowflakeErrorCode,
    SnowflakeRuntimeException,
)


pytestmark = pytest.mark.snowflake



def _parquet(path: Path, **columns) -> Path:
    """A real one-row parquet at `path`.

    `write_all_tables` reads each file's schema before it loads anything, so a
    placeholder byte string is not enough -- it has to be a parseable parquet.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.table(columns or {"c": pa.array([1], pa.int64())}), str(path))
    return path


def _cdc_parquet(path: Path, **columns) -> Path:
    """A one-row CDC batch: the key `_merge_cfgs` matches on, plus its marker.

    Merge mode coalesces the file before staging it, so the key and marker
    columns have to really be in the parquet -- a fake cursor reporting them
    among the staged columns is not enough.
    """
    import pyarrow as pa

    return _parquet(
        path,
        **(
            columns
            or {"c": pa.array([1], pa.int64()), "op": pa.array(["i"], pa.string())}
        ),
    )


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


def _write_cfg(**overrides) -> impl.SnowflakeWriteCfg:
    """A write cfg carrying the dispositions ``SnowflakeDest`` defaults to.

    ``schema_evolution`` mirrors the dest's own default of ``"update"``, so a
    test names it only when evolution is the thing under test.
    """
    kwargs: dict = dict(
        conn=_conn(),
        target_tables=["ANALYTICS.PUBLIC.T"],
        if_table_exists="append",
        schema_evolution="update",
        schema_evolution_engine="native",
        ctx=_FakeCtx(),
    )
    kwargs.update(overrides)
    return impl.SnowflakeWriteCfg(**kwargs)


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
# _use_ident -- USE DATABASE/SCHEMA reference
# ---------------------------------------------------------------------------
#
# tdx provisions databases/schemas UNQUOTED, so Snowflake stores them
# upper-folded. Any name expressible as an unquoted identifier must be emitted
# UNQUOTED so it folds back onto the stored object -- otherwise a quoted,
# case-sensitive reference misses it.
# ---------------------------------------------------------------------------


def test_use_ident_leaves_lowercase_bare_name_unquoted():
    assert impl._use_ident("public") == "public"


def test_use_ident_leaves_uppercase_bare_name_unquoted():
    assert impl._use_ident("ANALYTICS") == "ANALYTICS"


def test_use_ident_leaves_mixed_case_run_stamp_unquoted():
    # Regression: the uppercase ``T`` in a tdx run stamp must NOT force a quoted,
    # case-sensitive reference. Unquoted, Snowflake folds it onto the stored
    # ``XP_TUCU_20260722T144945_SNOWFLAKE`` schema; quoted, it missed and raised
    # "could not adopt ... schema=...: Object does not exist".
    name = "xp_tucu_20260722T144945_snowflake"
    assert impl._use_ident(name) == name
    assert '"' not in impl._use_ident(name)


def test_use_ident_quotes_name_with_space():
    assert impl._use_ident("weird schema") == '"weird schema"'


def test_use_ident_quotes_name_with_leading_digit():
    # A leading digit is not a legal unquoted identifier, so it must be quoted.
    assert impl._use_ident("1_schema") == '"1_schema"'


def test_use_ident_quotes_dotted_name_per_part():
    # A dotted name is not a single unquoted identifier -> per-part quoting.
    assert impl._use_ident("DB.S") == '"DB"."S"'


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
    parquet = _parquet(tmp_path / "in.parquet")
    cfg = _write_cfg()
    impl.write_all_tables(cfg, [parquet])
    assert fake_sf_connection.log[0] == "USE WAREHOUSE ETL_WH"


def test_write_all_tables_forces_database_and_schema_before_stage(
    fake_sf_connection, tmp_path
):
    parquet = _parquet(tmp_path / "in.parquet")
    # _conn() carries database=ANALYTICS, schema=PUBLIC.
    cfg = _write_cfg()
    impl.write_all_tables(cfg, [parquet])
    sql = fake_sf_connection.log
    use_wh = sql.index("USE WAREHOUSE ETL_WH")
    # Bare identifiers are emitted UNQUOTED (Snowflake folds them onto the
    # unquoted-provisioned objects); only names that can't be spelled unquoted
    # are double-quoted.
    use_db = sql.index("USE DATABASE ANALYTICS")
    use_schema = sql.index("USE SCHEMA PUBLIC")
    first_ddl = next(i for i, s in enumerate(sql) if s.startswith("CREATE"))
    assert use_wh < use_db < use_schema < first_ddl


def test_write_all_tables_skips_namespace_when_database_unset(
    fake_sf_connection, tmp_path
):
    parquet = _parquet(tmp_path / "in.parquet")
    cfg = _write_cfg(conn=_conn(database=None, schema=None, warehouse=None), target_tables=["T"])
    impl.write_all_tables(cfg, [parquet])
    assert not any(s.startswith("USE DATABASE") for s in fake_sf_connection.log)
    assert not any(s.startswith("USE SCHEMA") for s in fake_sf_connection.log)


def test_write_all_tables_raises_when_namespace_not_adopted(monkeypatch, tmp_path):
    from tabsdatak.conn.snowflake.error import SnowflakeRuntimeException

    class _BoomOnUseCursor(_FakeCursor):
        def execute(self, sql: str) -> None:
            if sql.startswith("USE DATABASE"):
                raise RuntimeError("Object does not exist or not authorized")
            super().execute(sql)

    fake = _FakeConnection()
    fake.cursor = lambda: _BoomOnUseCursor(fake.log)  # type: ignore[method-assign]
    monkeypatch.setattr(impl, "build_snowflake_connection", lambda conn: fake)
    parquet = _parquet(tmp_path / "in.parquet")
    cfg = _write_cfg()
    with pytest.raises(SnowflakeRuntimeException, match="could not adopt database"):
        impl.write_all_tables(cfg, [parquet])


def test_write_all_tables_creates_stage_puts_then_copies(
    fake_sf_connection, tmp_path
):
    parquet = _parquet(tmp_path / "in.parquet")
    cfg = _write_cfg()
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


def test_write_all_tables_reads_the_parquet_logical_types(
    fake_sf_connection, tmp_path
):
    # Without USE_LOGICAL_TYPE the inferred schema is the file's physical types:
    # a timestamp column comes back as the NUMBER holding its epoch, and a
    # decimal without its precision and scale. OR REPLACE because an account
    # holding an earlier definition of the format would otherwise keep it.
    parquet = _parquet(tmp_path / "in.parquet")
    impl.write_all_tables(_write_cfg(), [parquet])
    assert (
        "CREATE OR REPLACE FILE FORMAT tabsdata_parquet"
        " TYPE = PARQUET USE_LOGICAL_TYPE = TRUE"
    ) in fake_sf_connection.log


def test_write_all_tables_uses_temporary_stage_when_conn_stage_unset(
    fake_sf_connection, tmp_path
):
    parquet = _parquet(tmp_path / "in.parquet")
    conn = SnowflakeDestConn(
        account="ACME-MAIN",
        credentials=_credentials(),
        warehouse="ETL_WH",
        database="ANALYTICS",
        schema="PUBLIC",
    )
    cfg = _write_cfg(conn=conn)
    impl.write_all_tables(cfg, [parquet])
    sql = fake_sf_connection.log
    assert any(
        s.startswith("CREATE TEMPORARY STAGE IF NOT EXISTS tabsdata_temp_stage")
        for s in sql
    )


def test_write_all_tables_truncates_before_copy_on_replace(
    fake_sf_connection, tmp_path
):
    parquet = _parquet(tmp_path / "in.parquet")
    cfg = _write_cfg(if_table_exists="replace")
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
    parquet = _parquet(tmp_path / "in.parquet")
    cfg = _write_cfg()
    impl.write_all_tables(cfg, [parquet])
    assert not any(s.startswith("TRUNCATE") for s in fake_sf_connection.log)


def test_write_all_tables_iterates_one_per_table(
    fake_sf_connection, tmp_path
):
    p1 = _parquet(tmp_path / "a.parquet")
    p2 = _parquet(tmp_path / "b.parquet")
    cfg = _write_cfg(target_tables=["DB.S.T1", "DB.S.T2"])
    impl.write_all_tables(cfg, [p1, p2])
    copies = [s for s in fake_sf_connection.log if s.startswith("COPY INTO ")]
    assert len(copies) == 2
    assert 'COPY INTO "DB"."S"."T1"' in copies[0]
    assert 'COPY INTO "DB"."S"."T2"' in copies[1]


def test_write_all_tables_closes_connection_on_exit(
    fake_sf_connection, tmp_path
):
    parquet = _parquet(tmp_path / "in.parquet")
    cfg = _write_cfg()
    impl.write_all_tables(cfg, [parquet])
    assert fake_sf_connection.closed is True


def test_write_all_tables_closes_connection_on_failure(
    monkeypatch, tmp_path
):
    parquet = _parquet(tmp_path / "in.parquet")

    fake = _FakeConnection()

    def cursor_that_fails(*_args, **_kwargs):
        class _BoomCursor(_FakeCursor):
            def execute(self, sql: str) -> None:  # noqa: ARG002
                raise RuntimeError("boom")
        return _BoomCursor(fake.log)

    fake.cursor = cursor_that_fails  # type: ignore[assignment]
    monkeypatch.setattr(impl, "build_snowflake_connection", lambda conn: fake)

    cfg = _write_cfg()
    with pytest.raises(RuntimeError, match="boom"):
        impl.write_all_tables(cfg, [parquet])
    assert fake.closed is True


def test_write_all_tables_rejects_a_binary_column_before_connecting(tmp_path):
    """A binary column has to be refused up front, not by COPY INTO after the
    stage and the table already exist."""
    import pyarrow as pa

    from tabsdatak.conn.snowflake.error import (
        SnowflakeErrorCode,
        SnowflakeRuntimeException,
    )

    parquet = _parquet(
        tmp_path / "bin.parquet",
        ok=pa.array([1], pa.int64()),
        payload=pa.array([b"\x00\x01"], pa.binary()),
    )
    cfg = _write_cfg()
    # No fake_sf_connection fixture: the check must fire before any connect.
    with pytest.raises(SnowflakeRuntimeException) as ei:
        impl.write_all_tables(cfg, [parquet])
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_8
    assert "'payload'" in str(ei.value)
    assert "'ok'" not in str(ei.value)
    assert "ANALYTICS.PUBLIC.T" in str(ei.value)


def test_write_all_tables_rejects_large_and_fixed_binary(tmp_path):
    import pyarrow as pa

    from tabsdatak.conn.snowflake.error import SnowflakeRuntimeException

    for name, array in (
        ("large", pa.array([b"x"], pa.large_binary())),
        ("fixed", pa.array([b"xy"], pa.binary(2))),
    ):
        parquet = _parquet(tmp_path / f"{name}.parquet", **{name: array})
        cfg = _write_cfg()
        with pytest.raises(SnowflakeRuntimeException):
            impl.write_all_tables(cfg, [parquet])


def test_write_all_tables_names_the_table_the_offending_file_targets(tmp_path):
    """With several slots the error has to point at the right table."""
    import pyarrow as pa

    from tabsdatak.conn.snowflake.error import SnowflakeRuntimeException

    clean = _parquet(tmp_path / "clean.parquet")
    dirty = _parquet(
        tmp_path / "dirty.parquet", payload=pa.array([b"x"], pa.binary())
    )
    cfg = _write_cfg(target_tables=["ANALYTICS.PUBLIC.FIRST", "ANALYTICS.PUBLIC.SECOND"])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        impl.write_all_tables(cfg, [clean, dirty])
    assert "ANALYTICS.PUBLIC.SECOND" in str(ei.value)


# ---------------------------------------------------------------------------
# schema_evolution
#
# ``"strict"`` (the dest's default) leaves the target's columns alone.
# ``"update"`` lets a widened file through, by two different mechanisms:
#
# * a COPY INTO evolves the target itself once the table is marked with
#   ENABLE_SCHEMA_EVOLUTION, so append / replace need nothing else;
# * a MERGE does not honour that mark, so merge mode also has to ALTER the
#   missing columns in before the statement runs.
# ---------------------------------------------------------------------------


def _evolution_mark(enabled: bool, table: str = '"ANALYTICS"."PUBLIC"."T"') -> str:
    return f"ALTER TABLE {table} SET ENABLE_SCHEMA_EVOLUTION = {enabled};"


def _evolving_connection(monkeypatch, table_columns, file_columns):
    """A fake write connection whose target has ``table_columns`` and whose
    staged file infers to ``file_columns``."""
    fake = _column_fakes(table_columns, file_columns)
    monkeypatch.setattr(impl, "build_snowflake_connection", lambda conn: fake)
    return fake


def _merge_cfgs(*keys):
    from tabsdatak.conn.snowflake import MergeCfg

    return [MergeCfg(on=[key], op_column="op") for key in keys]


@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
@pytest.mark.parametrize(
    "schema_evolution,enabled", [("update", True), ("strict", False)]
)
def test_write_all_tables_sets_the_mark_to_match_the_setting(
    monkeypatch, tmp_path, if_table_exists, schema_evolution, enabled
):
    """The mark is set on every write, not only when evolving. Strict has to
    clear it explicitly: a target an earlier update-mode run marked would
    otherwise keep evolving on every COPY after it."""
    fake = _evolving_connection(
        monkeypatch, ["C", "OP"], [("C", "NUMBER(38,0)"), ("OP", "TEXT")]
    )
    parquet = _cdc_parquet(tmp_path / "in.parquet")
    cfg = _write_cfg(
        if_table_exists=if_table_exists,
        schema_evolution=schema_evolution,
        merge_cfgs=_merge_cfgs("c") if if_table_exists == "merge" else None,
    )
    impl.write_all_tables(cfg, [parquet])
    marks = [s for s in fake.log if "ENABLE_SCHEMA_EVOLUTION" in s]
    assert marks == [_evolution_mark(enabled)]


@pytest.mark.parametrize("schema_evolution", ["update", "strict"])
def test_write_all_tables_leaves_the_mark_alone_on_merge(
    monkeypatch, tmp_path, schema_evolution
):
    """The mark is read by COPY INTO, and the only COPY a merge runs loads its
    scratch table -- which is templated from the batch and has nothing to evolve
    onto. A merge evolves its target with ALTER statements instead."""
    fake = _evolving_connection(
        monkeypatch, ["C", "OP"], [("C", "NUMBER(38,0)"), ("OP", "TEXT")]
    )
    cfg = _write_cfg(
        if_table_exists="merge",
        schema_evolution=schema_evolution,
        merge_cfgs=_merge_cfgs("c"),
    )
    impl.write_all_tables(cfg, [_cdc_parquet(tmp_path / "in.parquet")])
    assert not any("ENABLE_SCHEMA_EVOLUTION" in s for s in fake.log)


# ---------------------------------------------------------------------------
# schema_evolution_engine="iceberg"
#
# Snowflake's own evolution only ever adds a column. Diffing the target against
# the staged file instead also widens a column whose type the batch has
# outgrown, and refuses a change Snowflake could not make -- so the option is
# opt-in per write, and turns the native mark off while it is in play.
# ---------------------------------------------------------------------------


def _iceberg_cfg(**overrides):
    kwargs: dict = dict(schema_evolution="update", schema_evolution_engine="iceberg")
    kwargs.update(overrides)
    return _write_cfg(**kwargs)


def _alters(log):
    return [s for s in log if s.startswith("ALTER TABLE") and "ENABLE_" not in s]


def test_iceberg_engine_widens_a_column_the_batch_has_outgrown(
    monkeypatch, tmp_path
):
    fake = _evolving_connection(monkeypatch, [("NAME", "VARCHAR(10)")], [])
    fake.results[_INFER_PREFIX] = (
        [("COLUMN_NAME",), ("TYPE",)],
        [("NAME", "VARCHAR(20)")],
    )
    impl.write_all_tables(_iceberg_cfg(), [_parquet(tmp_path / "in.parquet")])
    assert _alters(fake.log) == [
        'ALTER TABLE "ANALYTICS"."PUBLIC"."T" ALTER COLUMN "NAME"'
        " SET DATA TYPE VARCHAR(20)"
    ]


def test_iceberg_engine_adds_a_column_the_target_lacks(monkeypatch, tmp_path):
    fake = _evolving_connection(
        monkeypatch, ["ID"], [("ID", "NUMBER(38,0)"), ("NOTE", "TEXT")]
    )
    impl.write_all_tables(_iceberg_cfg(), [_parquet(tmp_path / "in.parquet")])
    assert _alters(fake.log) == [
        'ALTER TABLE "ANALYTICS"."PUBLIC"."T" ADD COLUMN IF NOT EXISTS "NOTE" TEXT'
    ]


def test_iceberg_engine_turns_the_native_mark_off(monkeypatch, tmp_path):
    """The mark persists on the table, so a target an earlier native run set
    would otherwise go on evolving underneath the Iceberg pass -- two engines
    on one write, only one of which the write asked for."""
    fake = _evolving_connection(monkeypatch, ["ID"], [("ID", "NUMBER(38,0)")])
    impl.write_all_tables(_iceberg_cfg(), [_parquet(tmp_path / "in.parquet")])
    assert [s for s in fake.log if "ENABLE_SCHEMA_EVOLUTION" in s] == [
        _evolution_mark(False)
    ]


def test_iceberg_engine_leaves_a_synonym_of_the_same_type_alone(
    monkeypatch, tmp_path
):
    """DESC TABLE reports VARCHAR(16777216) where infer_schema reports TEXT for
    the same column. Without aligning the two vocabularies every write on a
    table with an unparameterized text column would be refused."""
    fake = _evolving_connection(
        monkeypatch, [("NAME", "VARCHAR(16777216)")], [("NAME", "TEXT")]
    )
    impl.write_all_tables(_iceberg_cfg(), [_parquet(tmp_path / "in.parquet")])
    assert _alters(fake.log) == []


def test_iceberg_engine_refuses_a_change_snowflake_cannot_make(
    monkeypatch, tmp_path
):
    fake = _evolving_connection(
        monkeypatch, [("AMOUNT", "NUMBER(38,0)")], [("AMOUNT", "TEXT")]
    )
    with pytest.raises(_iceberg.UnsafeSchemaChangeError):
        impl.write_all_tables(_iceberg_cfg(), [_parquet(tmp_path / "in.parquet")])
    assert _alters(fake.log) == []
    assert not any(s.startswith("COPY INTO") for s in fake.log)


def test_iceberg_engine_evolves_before_it_truncates_a_replace(
    monkeypatch, tmp_path
):
    # A replace refills the table it just evolved, so the ALTER has to land
    # first -- and before the COPY, which purges the file it is read from.
    fake = _evolving_connection(
        monkeypatch, ["ID"], [("ID", "NUMBER(38,0)"), ("NOTE", "TEXT")]
    )
    impl.write_all_tables(
        _iceberg_cfg(if_table_exists="replace"), [_parquet(tmp_path / "in.parquet")]
    )
    alter = next(i for i, s in enumerate(fake.log) if s.startswith("ALTER TABLE")
                 and "ENABLE_" not in s)
    truncate = next(i for i, s in enumerate(fake.log) if s.startswith("TRUNCATE"))
    copy = next(i for i, s in enumerate(fake.log) if s.startswith("COPY INTO"))
    assert alter < truncate < copy


def test_iceberg_engine_does_nothing_under_strict(monkeypatch, tmp_path):
    """Strict pins the target's schema; the engine only says *how* a schema that
    is allowed to move is moved."""
    fake = _evolving_connection(
        monkeypatch, ["ID"], [("ID", "NUMBER(38,0)"), ("NOTE", "TEXT")]
    )
    impl.write_all_tables(
        _iceberg_cfg(schema_evolution="strict"), [_parquet(tmp_path / "in.parquet")]
    )
    assert _alters(fake.log) == []
    assert [s for s in fake.log if "ENABLE_SCHEMA_EVOLUTION" in s] == [
        _evolution_mark(False)
    ]


def test_native_engine_emits_no_alters_of_its_own(monkeypatch, tmp_path):
    """The counterpart: native evolution is the mark and nothing else, so a
    target short a column is left to COPY INTO."""
    fake = _evolving_connection(
        monkeypatch, ["ID"], [("ID", "NUMBER(38,0)"), ("NOTE", "TEXT")]
    )
    impl.write_all_tables(_write_cfg(schema_evolution="update"), [
        _parquet(tmp_path / "in.parquet")
    ])
    assert _alters(fake.log) == []
    assert [s for s in fake.log if "ENABLE_SCHEMA_EVOLUTION" in s] == [
        _evolution_mark(True)
    ]


def test_write_all_tables_sets_the_mark_after_creating_and_before_copying(
    fake_sf_connection, tmp_path
):
    # The mark is an ALTER, so the table has to exist; it changes how the COPY
    # behaves, so it has to precede it.
    parquet = _parquet(tmp_path / "in.parquet")
    impl.write_all_tables(_write_cfg(schema_evolution="update"), [parquet])
    sql = fake_sf_connection.log
    create = next(i for i, s in enumerate(sql) if s.startswith("CREATE TABLE"))
    mark = sql.index(_evolution_mark(True))
    copy = next(i for i, s in enumerate(sql) if s.startswith("COPY INTO"))
    assert create < mark < copy


def test_write_all_tables_sets_the_mark_on_every_table_it_writes(
    fake_sf_connection, tmp_path
):
    cfg = _write_cfg(
        target_tables=["DB.S.T1", "DB.S.T2"], schema_evolution="update"
    )
    impl.write_all_tables(
        cfg, [_parquet(tmp_path / f"{n}.parquet") for n in ("a", "b")]
    )
    marks = [s for s in fake_sf_connection.log if "ENABLE_SCHEMA_EVOLUTION" in s]
    assert marks == [
        _evolution_mark(True, '"DB"."S"."T1"'),
        _evolution_mark(True, '"DB"."S"."T2"'),
    ]


def test_write_all_tables_does_not_add_columns_itself_outside_merge(
    monkeypatch, tmp_path
):
    """A COPY evolves the marked table on its own, so append / replace never
    reach for the ALTER ... ADD COLUMN path -- no column probe is issued."""
    fake = _evolving_connection(
        monkeypatch,
        ["C", "OP"],
        [("C", "NUMBER(38,0)"), ("OP", "TEXT"), ("EXTRA", "TEXT")],
    )
    parquet = _parquet(tmp_path / "in.parquet")
    impl.write_all_tables(_write_cfg(schema_evolution="update"), [parquet])
    assert not any(s.startswith("SELECT * FROM") for s in fake.log)
    assert not any("ADD COLUMN" in s for s in fake.log)


def test_write_all_tables_adds_missing_columns_before_merging_on_update(
    monkeypatch, tmp_path
):
    """ENABLE_SCHEMA_EVOLUTION does not reach a MERGE, so merge mode has to put
    the missing columns in itself -- before the statement that writes them.

    The scratch table is loaded ahead of both: it is templated from the batch
    and owes the target's schema nothing.
    """
    fake = _evolving_connection(
        monkeypatch,
        ["C", "OP"],
        [("C", "NUMBER(38,0)"), ("OP", "TEXT"), ("EXTRA", "TEXT")],
    )
    parquet = _cdc_parquet(tmp_path / "in.parquet")
    cfg = _write_cfg(
        if_table_exists="merge",
        schema_evolution="update",
        merge_cfgs=_merge_cfgs("c"),
    )
    impl.write_all_tables(cfg, [parquet])
    sql = fake.log
    temp = next(i for i, s in enumerate(sql) if s.startswith("CREATE TEMPORARY TABLE"))
    add = next(i for i, s in enumerate(sql) if "ADD COLUMN" in s)
    merge = next(i for i, s in enumerate(sql) if s.startswith("MERGE INTO"))
    assert temp < add < merge
    assert 'ADD COLUMN IF NOT EXISTS "EXTRA" TEXT' in sql[add]


def test_write_all_tables_merges_a_column_it_just_added(monkeypatch, tmp_path):
    """The shared-column read happens after the ALTERs, so a column added a
    moment ago is in the projection rather than being left behind by it."""
    fake = _evolving_connection(
        monkeypatch,
        ["C", "OP"],
        [("C", "NUMBER(38,0)"), ("OP", "TEXT"), ("EXTRA", "TEXT")],
    )
    parquet = _cdc_parquet(tmp_path / "in.parquet")
    cfg = _write_cfg(
        if_table_exists="merge",
        schema_evolution="update",
        merge_cfgs=_merge_cfgs("c"),
    )
    impl.write_all_tables(cfg, [parquet])
    merge = next(s for s in fake.log if s.startswith("MERGE INTO"))
    columns, _ = _insert_lists(merge)
    assert columns == ['"C"', '"EXTRA"']


def test_write_all_tables_does_not_add_columns_before_merging_on_strict(
    monkeypatch, tmp_path
):
    fake = _evolving_connection(
        monkeypatch,
        ["C", "OP"],
        [("C", "NUMBER(38,0)"), ("OP", "TEXT"), ("EXTRA", "TEXT")],
    )
    parquet = _cdc_parquet(tmp_path / "in.parquet")
    cfg = _write_cfg(
        if_table_exists="merge",
        schema_evolution="strict",
        merge_cfgs=_merge_cfgs("c"),
    )
    impl.write_all_tables(cfg, [parquet])
    assert not any("ADD COLUMN" in s for s in fake.log)
    # The merge itself still runs; the column the target lacks is simply not
    # among the columns written, and neither is the marker.
    merge = next(s for s in fake.log if s.startswith("MERGE INTO"))
    columns, _ = _insert_lists(merge)
    assert columns == ['"C"']


@pytest.mark.parametrize("enabled", [True, False])
def test_set_copy_into_schema_evolution_statement(enabled):
    fake = _FakeConnection()
    impl._set_copy_into_schema_evolution(fake.cursor(), enabled, '"DB"."S"."T"')
    assert fake.log == [
        f'ALTER TABLE "DB"."S"."T" SET ENABLE_SCHEMA_EVOLUTION = {enabled};'
    ]


# ---------------------------------------------------------------------------
# The marker and the created table
#
# A merge's marker column has to be kept out of the target at creation as well
# as at write time: an ALTER can add a column later but nothing takes one back,
# so a first run that templated the marker in would leave it there for good.
# ---------------------------------------------------------------------------


def _create_statement(log: list[str]) -> str:
    return next(s for s in log if s.startswith("CREATE TABLE IF NOT EXISTS"))


def _temp_create_statement(log: list[str]) -> str:
    return next(s for s in log if s.startswith("CREATE TEMPORARY TABLE"))


def test_write_all_tables_creates_a_merge_target_without_the_marker(
    monkeypatch, tmp_path
):
    fake = _evolving_connection(
        monkeypatch, ["C", "OP"], [("C", "NUMBER(38,0)"), ("OP", "TEXT")]
    )
    cfg = _write_cfg(if_table_exists="merge", merge_cfgs=_merge_cfgs("c"))
    impl.write_all_tables(cfg, [_cdc_parquet(tmp_path / "in.parquet")])
    assert 'SELECT * EXCLUDE ("OP")' in _create_statement(fake.log)


@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
def test_write_all_tables_creates_a_loaded_target_whole(
    fake_sf_connection, tmp_path, if_table_exists
):
    # Only a merge reads a marker, so only a merge target loses a column to it.
    cfg = _write_cfg(if_table_exists=if_table_exists)
    impl.write_all_tables(cfg, [_parquet(tmp_path / "in.parquet")])
    assert "WHERE" not in _create_statement(fake_sf_connection.log)


def test_write_all_tables_keeps_the_marker_in_the_staging_table(
    monkeypatch, tmp_path
):
    """The staging copy must keep the marker -- it is what the clauses read."""
    fake = _evolving_connection(
        monkeypatch, ["C", "OP"], [("C", "NUMBER(38,0)"), ("OP", "TEXT")]
    )
    cfg = _write_cfg(if_table_exists="merge", merge_cfgs=_merge_cfgs("c"))
    impl.write_all_tables(cfg, [_cdc_parquet(tmp_path / "in.parquet")])
    assert "WHERE" not in _temp_create_statement(fake.log)




# ---------------------------------------------------------------------------
# watermark_column on an append
#
# An append is not idempotent on its own: Snowflake's load history does not
# dedup a re-staged file, so a rerun of the same transaction loads the batch
# twice. Naming a watermark makes it idempotent -- the column is written into
# the staged file so the COPY stamps the rows it loads, and a rerun that finds
# the target already carrying this trx's id does nothing at all.
# ---------------------------------------------------------------------------

_WATERMARK = "WM"


def _watermark_cfg(**overrides):
    kwargs: dict = dict(watermark_column=_WATERMARK, schema_evolution="strict")
    kwargs.update(overrides)
    return _write_cfg(**kwargs)


def _watermark_alters(log):
    return [s for s in log if f'"{_WATERMARK}"' in s and s.startswith("ALTER TABLE")]


def _staged_files(monkeypatch):
    """Capture what each ``PUT`` was handed, read while it still exists.

    The rewritten batch lives in a temp directory removed as soon as it has
    been staged, so a test that reads it afterwards finds nothing. Reading it
    from inside the ``PUT`` is the only moment it is on disk.
    """
    import polars as pl

    staged: list[tuple[Path, pl.DataFrame]] = []
    original = impl.put_file_to_stage

    def capture(cursor, file_path, stage):
        staged.append((file_path, pl.read_parquet(file_path)))
        return original(cursor, file_path, stage)

    monkeypatch.setattr(impl, "put_file_to_stage", capture)
    return staged


def test_append_watermark_stamps_every_staged_row_with_the_trx_id(
    monkeypatch, tmp_path
):
    staged = _staged_files(monkeypatch)
    _evolving_connection(monkeypatch, ["C"], [("C", "NUMBER(38,0)")])
    impl.write_all_tables(_watermark_cfg(), [_parquet(tmp_path / "in.parquet")])
    _, frame = staged[0]
    assert frame[_WATERMARK].to_list() == ["TRX_1"]


def test_append_watermark_stages_a_copy_keeping_the_original_basename(
    monkeypatch, tmp_path
):
    # Everything downstream is keyed off the staged file's name -- the COPY
    # names it in FILES, and infer_schema reads it by path -- so the rewrite
    # has to be indistinguishable from the file the write was handed.
    staged = _staged_files(monkeypatch)
    _evolving_connection(monkeypatch, ["C"], [("C", "NUMBER(38,0)")])
    parquet = _parquet(tmp_path / "in.parquet")
    impl.write_all_tables(_watermark_cfg(), [parquet])
    path, _ = staged[0]
    assert path != parquet
    assert path.name == parquet.name


def test_append_without_a_watermark_stages_the_file_it_was_handed(
    monkeypatch, tmp_path
):
    staged = _staged_files(monkeypatch)
    _evolving_connection(monkeypatch, ["C"], [("C", "NUMBER(38,0)")])
    parquet = _parquet(tmp_path / "in.parquet")
    impl.write_all_tables(_write_cfg(), [parquet])
    path, frame = staged[0]
    assert path == parquet
    assert frame.columns == ["c"]


def test_append_watermark_removes_the_rewritten_file_once_it_is_staged(
    monkeypatch, tmp_path
):
    staged = _staged_files(monkeypatch)
    _evolving_connection(monkeypatch, ["C"], [("C", "NUMBER(38,0)")])
    impl.write_all_tables(_watermark_cfg(), [_parquet(tmp_path / "in.parquet")])
    path, _ = staged[0]
    assert not path.exists()


def test_append_watermark_adds_the_column_to_the_target_without_a_default(
    monkeypatch, tmp_path
):
    # Without a DEFAULT: Snowflake backfills a constant one into the rows
    # already there, and those belong to earlier transactions.
    fake = _evolving_connection(monkeypatch, ["C"], [("C", "NUMBER(38,0)")])
    impl.write_all_tables(_watermark_cfg(), [_parquet(tmp_path / "in.parquet")])
    assert _watermark_alters(fake.log) == [
        'ALTER TABLE "ANALYTICS"."PUBLIC"."T" ADD COLUMN IF NOT EXISTS "WM" STRING'
    ]


def test_append_watermark_leaves_a_target_that_already_has_the_column_alone(
    monkeypatch, tmp_path
):
    fake = _evolving_connection(
        monkeypatch, ["C", _WATERMARK], [("C", "NUMBER(38,0)")]
    )
    impl.write_all_tables(_watermark_cfg(), [_parquet(tmp_path / "in.parquet")])
    assert _watermark_alters(fake.log) == []


def test_append_watermark_adds_the_column_before_the_copy(monkeypatch, tmp_path):
    # A column added after the load would be NULL in the rows just written, and
    # the guard would read the next rerun as the first.
    fake = _evolving_connection(monkeypatch, ["C"], [("C", "NUMBER(38,0)")])
    impl.write_all_tables(_watermark_cfg(), [_parquet(tmp_path / "in.parquet")])
    alter = fake.log.index(_watermark_alters(fake.log)[0])
    copy = next(i for i, s in enumerate(fake.log) if s.startswith("COPY INTO"))
    assert alter < copy


def test_append_watermark_is_added_under_strict(monkeypatch, tmp_path):
    """Strict pins the target's schema and this is its one exception: nothing
    else adds the column, so without it the COPY drops what it cannot match and
    the watermark silently does nothing."""
    fake = _evolving_connection(monkeypatch, ["C"], [("C", "NUMBER(38,0)")])
    impl.write_all_tables(
        _watermark_cfg(schema_evolution="strict"),
        [_parquet(tmp_path / "in.parquet")],
    )
    assert len(_watermark_alters(fake.log)) == 1


def test_append_watermark_leaves_the_iceberg_pass_nothing_to_add(
    monkeypatch, tmp_path
):
    """Added ahead of the diff, so the engine that may also add columns finds
    it already there -- one statement, not two claims on the same column."""
    fake = _evolving_connection(monkeypatch, ["C"], [("C", "NUMBER(38,0)")])
    fake.results[_INFER_PREFIX] = (
        [("COLUMN_NAME",), ("TYPE",)],
        [("C", "NUMBER(38,0)"), (_WATERMARK, "TEXT")],
    )
    impl.write_all_tables(
        _watermark_cfg(schema_evolution="update", schema_evolution_engine="iceberg"),
        [_parquet(tmp_path / "in.parquet")],
    )
    after = fake.log[fake.log.index(_watermark_alters(fake.log)[0]) + 1 :]
    # The diff really did run afterwards, and found nothing left to do.
    assert any(s.startswith("DESC TABLE") for s in after)
    assert not [s for s in after if s.startswith("ALTER TABLE") and "ENABLE_" not in s]


def test_append_skips_entirely_when_the_trx_is_already_stamped(
    monkeypatch, tmp_path
):
    fake = _column_fakes(
        ["C", _WATERMARK],
        [("C", "NUMBER(38,0)")],
        _watermark_answers(has_column=True, stamped=True),
    )
    monkeypatch.setattr(impl, "build_snowflake_connection", lambda conn: fake)
    impl.write_all_tables(_watermark_cfg(), [_parquet(tmp_path / "in.parquet")])
    assert not any(s.startswith("PUT ") for s in fake.log)
    assert not any(s.startswith("COPY INTO") for s in fake.log)


def test_append_proceeds_when_the_target_carries_a_different_trx(
    monkeypatch, tmp_path
):
    fake = _column_fakes(
        ["C", _WATERMARK],
        [("C", "NUMBER(38,0)")],
        _watermark_answers(has_column=True, stamped=False),
    )
    monkeypatch.setattr(impl, "build_snowflake_connection", lambda conn: fake)
    impl.write_all_tables(_watermark_cfg(), [_parquet(tmp_path / "in.parquet")])
    assert any(s.startswith("COPY INTO") for s in fake.log)


def test_append_checks_the_watermark_before_it_stages_anything(
    monkeypatch, tmp_path
):
    # A rerun with nothing to do should not pay to rewrite and upload a file it
    # is about to decline to load.
    fake = _column_fakes(
        ["C", _WATERMARK],
        [("C", "NUMBER(38,0)")],
        _watermark_answers(has_column=True, stamped=False),
    )
    monkeypatch.setattr(impl, "build_snowflake_connection", lambda conn: fake)
    impl.write_all_tables(_watermark_cfg(), [_parquet(tmp_path / "in.parquet")])
    probe = next(
        i for i, s in enumerate(fake.log) if s.startswith("SELECT COUNT(*) > 0 FROM")
    )
    put = next(i for i, s in enumerate(fake.log) if s.startswith("PUT "))
    assert probe < put


def test_append_binds_the_trx_id_rather_than_interpolating_it(
    monkeypatch, tmp_path
):
    fake = _column_fakes(
        ["C", _WATERMARK],
        [("C", "NUMBER(38,0)")],
        _watermark_answers(has_column=True, stamped=False),
    )
    monkeypatch.setattr(impl, "build_snowflake_connection", lambda conn: fake)
    impl.write_all_tables(_watermark_cfg(), [_parquet(tmp_path / "in.parquet")])
    stamp = next(
        params
        for sql, params in fake.binds
        if sql.startswith("SELECT COUNT(*) > 0 FROM")
    )
    assert stamp == ("TRX_1",)


def test_a_replace_never_stamps_even_when_handed_a_watermark(
    monkeypatch, tmp_path
):
    """SnowflakeDest refuses the pairing, and the write path declines it again
    rather than resting on a validation two layers away."""
    staged = _staged_files(monkeypatch)
    fake = _evolving_connection(monkeypatch, ["C"], [("C", "NUMBER(38,0)")])
    parquet = _parquet(tmp_path / "in.parquet")
    impl.write_all_tables(_watermark_cfg(if_table_exists="replace"), [parquet])
    assert staged[0][0] == parquet
    assert _watermark_alters(fake.log) == []
