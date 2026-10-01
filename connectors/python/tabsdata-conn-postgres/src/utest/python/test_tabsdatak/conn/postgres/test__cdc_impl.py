#
# Copyright 2026 Tabsdata Inc.
#

"""``_cdc_impl.py`` -- pure-function coverage for the Postgres CDC
dialect, wal2json ingester and offset. Catalog / engine calls
(``server_timezone``, ``get_catalog_columns``, slot create /
``start_replication``, ``poll_changes``) need a real Postgres CDC
domain and live in the integration tests (``test_plugin_cdc.py``)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import sqlalchemy.types as sa_types
from sqlalchemy.dialects.postgresql import BYTEA, JSON, JSONB, OID, TEXT
from sqlalchemy.types import LargeBinary, Text

from tabsdatak.conn.common.sql.cdc._ingester import CDC_OFFSET_STATE_KEY
from tabsdatak.conn.common.sql.cdc._model import TableObject, TrackedTable
from tabsdatak.conn.common.sql.cdc.constants import CdcOperation
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat
from tabsdatak.conn.postgres._cdc_impl import (
    DEFAULT_WAL2JSON_FORMAT_VERSION,
    LsnOffset,
    PostgresDialect,
    TransactionSnapshotSession,
    Wal2JsonIngester,
)
from tabsdatak.conn.postgres.error import PostgresErrorCode
from tabsdatak.error import TabsdataException


# =====================================================================
# PostgresDialect
# =====================================================================


TS = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def d() -> PostgresDialect:
    return PostgresDialect()


# --- Identifier handling --------------------------------------------


def test_quotes(d):
    assert d.open_quote == '"'
    assert d.close_quote == '"'


def test_default_case_fold_lowers(d):
    # Postgres folds unquoted identifiers to lower case.
    assert d.default_case_fold("Users") == "users"
    assert d.default_case_fold("MIXED_Case") == "mixed_case"


def test_parse_qualified_table_simple(d):
    schema, table = d.parse_qualified_table("public.users")
    assert str(schema) == "public"
    assert str(table) == "users"


def test_parse_qualified_table_double_quoted(d):
    schema, table = d.parse_qualified_table('"MySchema"."Users"')
    assert str(schema) == "MySchema"
    assert str(table) == "Users"
    assert schema.quote is True
    assert table.quote is True


def test_parse_qualified_table_doubled_quote_escape(d):
    # Postgres' standard ``""``-inside-``"..."`` escape.
    schema, table = d.parse_qualified_table('"weird ""schema".users')
    assert str(schema) == 'weird "schema'


def test_parse_qualified_table_allows_unqualified(d):
    # Unqualified name: schema is None; the ingester fills it from the
    # connection's default schema (current_schema()); see default_schema.
    schema, table = d.parse_qualified_table("users")
    assert schema is None
    assert str(table) == "users"


def test_parse_qualified_table_allows_unqualified_double_quoted(d):
    schema, table = d.parse_qualified_table('"Users"')
    assert schema is None
    assert str(table) == "Users"
    assert table.quote is True


def test_parse_qualified_table_rejects_empty_side(d):
    with pytest.raises(ValueError):
        d.parse_qualified_table(".users")
    with pytest.raises(ValueError):
        d.parse_qualified_table("public.")
    with pytest.raises(ValueError):
        d.parse_qualified_table("")


def test_default_schema_uses_inspector_default(d):
    # Postgres resolves an unqualified name against current_schema(), which is
    # what SQLAlchemy reports as default_schema_name.
    inspector = MagicMock()
    inspector.default_schema_name = "public"
    assert d.default_schema(inspector) == "public"


# --- Large-column classification ------------------------------------


def test_large_column_kind_object_for_bytea_and_large_objects(d):
    assert d.large_column_kind(BYTEA()) == "object"
    assert d.large_column_kind(LargeBinary()) == "object"
    assert d.large_column_kind(OID()) == "object"


def test_large_column_kind_text_for_text_and_json(d):
    assert d.large_column_kind(Text()) == "text"
    assert d.large_column_kind(TEXT()) == "text"
    assert d.large_column_kind(JSON()) == "text"
    assert d.large_column_kind(JSONB()) == "text"


def test_large_column_kind_none_for_regular_types(d):
    assert d.large_column_kind(sa_types.Integer()) is None
    assert d.large_column_kind(sa_types.String()) is None
    assert d.large_column_kind(sa_types.String(length=100_000)) is None


def test_replica_identity_reads_relreplident_for_schema_and_table(d):
    connection = MagicMock()
    connection.execute.return_value.scalar.return_value = "f"

    assert d.replica_identity(connection, "public", "orders") == "f"

    statement, params = connection.execute.call_args.args
    sql = str(statement).lower()
    assert "relreplident" in sql
    assert "pg_catalog.pg_class" in sql
    assert "pg_catalog.pg_namespace" in sql
    assert params == {"schema": "public", "table": "orders"}


# --- cdc_to_sa ingester dispatch ------------------------------------


def test_cdc_to_sa_unknown_ingester_raises(d):
    with pytest.raises(NotImplementedError):
        d.cdc_to_sa("binlog", "integer")


# --- cdc_to_sa for wal2json: type-string mapping --------------------


def test_wal2json_primitive_types(d):
    assert isinstance(d.cdc_to_sa("wal2json", "integer"), sa_types.Integer)
    assert isinstance(d.cdc_to_sa("wal2json", "bigint"), sa_types.BigInteger)
    assert isinstance(d.cdc_to_sa("wal2json", "smallint"), sa_types.SmallInteger)
    assert isinstance(d.cdc_to_sa("wal2json", "boolean"), sa_types.Boolean)
    assert isinstance(d.cdc_to_sa("wal2json", "text"), sa_types.Text)
    assert isinstance(d.cdc_to_sa("wal2json", "uuid"), sa_types.String)
    assert isinstance(d.cdc_to_sa("wal2json", "date"), sa_types.Date)
    assert isinstance(d.cdc_to_sa("wal2json", "bytea"), sa_types.LargeBinary)


def test_wal2json_varchar_length(d):
    t = d.cdc_to_sa("wal2json", "character varying(64)")
    assert isinstance(t, sa_types.String)
    assert t.length == 64

    t = d.cdc_to_sa("wal2json", "varchar(255)")
    assert isinstance(t, sa_types.String)
    assert t.length == 255


def test_wal2json_numeric_precision_scale(d):
    t = d.cdc_to_sa("wal2json", "numeric(10,2)")
    assert isinstance(t, sa_types.Numeric)
    assert t.precision == 10
    assert t.scale == 2


def test_wal2json_timestamp_timezone(d):
    t_naive = d.cdc_to_sa("wal2json", "timestamp without time zone")
    assert isinstance(t_naive, sa_types.DateTime)
    assert t_naive.timezone is False

    t_aware = d.cdc_to_sa("wal2json", "timestamp with time zone")
    assert isinstance(t_aware, sa_types.DateTime)
    assert t_aware.timezone is True


def test_wal2json_unknown_falls_back_to_string(d):
    t = d.cdc_to_sa("wal2json", "some_extension_type")
    assert isinstance(t, sa_types.String)


def test_wal2json_jsonb_and_json(d):
    assert isinstance(d.cdc_to_sa("wal2json", "json"), sa_types.JSON)
    assert isinstance(d.cdc_to_sa("wal2json", "jsonb"), sa_types.JSON)


# =====================================================================
# LsnOffset
# =====================================================================


def test_lsn_offset_round_trip():
    a = LsnOffset(lsn=12345)
    serialised = a.write()
    assert isinstance(serialised, str)
    assert LsnOffset.read(serialised).lsn == 12345


def test_lsn_offset_read_missing_lsn_defaults_to_zero():
    assert LsnOffset.read("{}").lsn == 0
    assert LsnOffset.read("").lsn == 0


def test_lsn_offset_update_with_another_lsn_offset():
    a = LsnOffset(lsn=1)
    a.update(LsnOffset(lsn=999))
    assert a.lsn == 999


def test_lsn_offset_update_rejects_foreign_offset():
    a = LsnOffset(lsn=1)

    class _Foreign:
        pass

    with pytest.raises(TabsdataException) as ei:
        a.update(_Foreign())  # type: ignore[arg-type]
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_16


def test_lsn_offset_copy_is_independent():
    a = LsnOffset(lsn=1)
    b = a.copy()
    b.lsn = 999
    assert a.lsn == 1


def test_lsn_offset_default_lsn_is_zero():
    assert LsnOffset().lsn == 0


# =====================================================================
# Wal2JsonIngester -- pure-function coverage
# =====================================================================


def _ingester_with_tables(qualified: list[str]) -> Wal2JsonIngester:
    conn = MagicMock()
    conn.uri = "postgresql://h:5432/d"
    bi = Wal2JsonIngester(
        conn=conn,
        tables=qualified,
        start_from="tail",
        replication_slot="testslot",
    )
    bi._tracked_tables = {
        q: TrackedTable(
            data_table=TableObject(schema=q.split(".")[0], table=q.split(".")[1]),
            cdc_table=TableObject(schema=q.split(".")[0], table=q.split(".")[1]),
        )
        for q in qualified
    }
    return bi


# --- schema_table_filter --------------------------------------------


def _partial_text_capture_check(
    monkeypatch, caplog, *, columns, identity, unchanged_large_text_as_null=False
):
    import logging

    from tabsdatak.conn.postgres import _cdc_impl

    inspector = MagicMock()
    inspector.get_columns.return_value = columns
    monkeypatch.setattr(_cdc_impl.sa, "inspect", lambda engine: inspector)

    bi = _ingester_with_tables(["public.docs"])
    bi._unchanged_large_text_as_null = unchanged_large_text_as_null
    monkeypatch.setattr(
        PostgresDialect,
        "replica_identity",
        lambda self, connection, schema, table: identity,
    )
    with caplog.at_level(logging.WARNING, logger=_cdc_impl.__name__):
        bi._check_partial_text_capture(MagicMock(), bi._tracked_tables)
    return [record for record in caplog.records if record.levelno == logging.WARNING]


_TEXT_AND_ID = [
    {"name": "id", "type": sa_types.Integer()},
    {"name": "body", "type": TEXT()},
]


def test_text_columns_without_replica_identity_full_fail_by_default(
    monkeypatch, caplog
):
    with pytest.raises(TabsdataException) as ei:
        _partial_text_capture_check(
            monkeypatch, caplog, columns=_TEXT_AND_ID, identity="d"
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_20
    assert "public.docs" in str(ei.value)
    assert "body" in str(ei.value)


def test_text_columns_without_replica_identity_full_warn_when_nulls_accepted(
    monkeypatch, caplog
):
    warnings = _partial_text_capture_check(
        monkeypatch,
        caplog,
        columns=_TEXT_AND_ID,
        identity="d",
        unchanged_large_text_as_null=True,
    )
    assert len(warnings) == 1
    assert "public.docs" in warnings[0].getMessage()
    assert "body" in warnings[0].getMessage()


def test_replica_identity_full_passes_silently(monkeypatch, caplog):
    warnings = _partial_text_capture_check(
        monkeypatch,
        caplog,
        columns=[{"name": "body", "type": TEXT()}],
        identity="f",
    )
    assert warnings == []


def test_tables_without_text_columns_pass_silently(monkeypatch, caplog):
    warnings = _partial_text_capture_check(
        monkeypatch,
        caplog,
        columns=[{"name": "id", "type": sa_types.Integer()}],
        identity="d",
    )
    assert warnings == []


# --- truncate ---------------------------------------------------------


def _truncate_ready(qualified: str = "public.users") -> Wal2JsonIngester:
    """An ingester whose registry is live, so a truncate can resolve the
    table definition it stamps on the emitted row."""
    bi = _ingester_with_tables([qualified])
    bi.init_schema_registry(CdcFormat())
    return bi


def _truncate_payload(schema: str = "public", table: str = "users") -> dict:
    return {"action": "T", "schema": schema, "table": table}


def test_truncate_is_emitted_with_the_known_table_definition():
    """wal2json reports a truncate per table with no row image. The row
    carries the table's current definition so it lands beside that table's
    changes, with every value null."""
    bi = _truncate_ready()
    bi._schema_registry.register_from_log(
        "public.users", (("id", "integer", "integer"), ("name", "text", "text"))
    )

    change = bi._truncate_change(
        payload=_truncate_payload(),
        xid="77",
        sequence=3,
        timestamp=TS,
        offset=LsnOffset(lsn=900),
    )

    assert change.operation is CdcOperation.TRUNCATE
    assert change.table == "public.users"
    assert change.transaction == "77"
    assert [column.name for column in change.schema.columns] == ["id", "name"]
    assert change.new_values == {"id": None, "name": None}
    assert change.old_values == {"id": None, "name": None}


def test_truncate_of_a_never_captured_table_is_still_emitted():
    """A table truncated before any change revealed its columns has no
    definition. The event must still reach the destination -- dropping it
    would leave rows there that nothing ever removes -- so it carries the
    unknown definition instead."""
    bi = _truncate_ready()

    change = bi._truncate_change(
        payload=_truncate_payload(),
        xid="77",
        sequence=1,
        timestamp=TS,
        offset=LsnOffset(lsn=900),
    )

    assert change is not None
    assert change.operation is CdcOperation.TRUNCATE
    assert change.schema.columns == ()
    assert change.new_values == {}


def test_truncate_keeps_the_transaction_position():
    """Postgres truncates inside the transaction that issued them, so the
    row takes that transaction's LSN and sequence and sorts between the
    changes around it."""
    bi = _truncate_ready()

    change = bi._truncate_change(
        payload=_truncate_payload(),
        xid="88",
        sequence=5,
        timestamp=TS,
        offset=LsnOffset(lsn=1234),
    )

    assert change.segment == 1234
    assert change.ordinal == 5
    assert change.sequence == 5
    assert change.timestamp == TS


def test_truncate_of_an_untracked_table_is_ignored():
    bi = _truncate_ready()

    assert (
        bi._truncate_change(
            payload=_truncate_payload(table="other"),
            xid="77",
            sequence=1,
            timestamp=TS,
            offset=LsnOffset(lsn=900),
        )
        is None
    )


def test_truncate_without_a_timestamp_falls_back_to_now():
    """A truncate outside any transaction framing carries no commit
    timestamp, so the row is stamped when it is captured."""
    bi = _truncate_ready()

    change = bi._truncate_change(
        payload=_truncate_payload(),
        xid="",
        sequence=1,
        timestamp=None,
        offset=LsnOffset(lsn=900),
    )

    assert change.timestamp is not None
    assert change.timestamp.tzinfo is not None


def test_schema_table_filter_default_is_wildcard_when_empty():
    conn = MagicMock()
    bi = Wal2JsonIngester(
        conn=conn, tables=[], start_from="tail", replication_slot="s",
    )
    # wal2json's ``add-tables`` accepts ``"*.*"`` for "every table"
    # -- the schema-registry will reject events for untracked tables
    # downstream.
    assert bi._schema_table_filter() == "*.*"


def test_schema_table_filter_lists_tracked_tables():
    bi = _ingester_with_tables(["public.users", "public.orders"])
    out = bi._schema_table_filter()
    # Order is dict-iteration order, but content must match.
    assert set(out.split(",")) == {"public.users", "public.orders"}


# --- _as_qualified_table --------------------------------------------


def test_as_qualified_table_known():
    bi = _ingester_with_tables(["public.users"])
    assert bi._as_qualified_table("public", "users") == "public.users"


def test_as_qualified_table_unknown_returns_none():
    bi = _ingester_with_tables(["public.users"])
    assert bi._as_qualified_table("other", "users") is None


# --- extract_raw_schema_v1 -----------------------------------------


def test_extract_raw_schema_v1_from_column_names_and_types():
    change = {
        "schema": "public",
        "table": "users",
        "columnnames": ["id", "name", "age"],
        "columntypes": ["integer", "character varying(64)", "integer"],
    }
    rs = Wal2JsonIngester._extract_raw_schema_v1(change)
    names = [t[0] for t in rs]
    types = [t[1] for t in rs]
    assert names == ["id", "name", "age"]
    assert types == ["integer", "character varying(64)", "integer"]


def test_extract_raw_schema_v1_falls_back_to_oldkeys_on_delete():
    change = {
        "schema": "public",
        "table": "users",
        "kind": "delete",
        "oldkeys": {
            "keynames": ["id"],
            "keytypes": ["integer"],
        },
    }
    rs = Wal2JsonIngester._extract_raw_schema_v1(change)
    assert [t[0] for t in rs] == ["id"]


def test_extract_raw_schema_v1_rejects_empty_payload():
    with pytest.raises(TabsdataException) as ei:
        Wal2JsonIngester._extract_raw_schema_v1({"schema": "s", "table": "t"})
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_15


# --- extract_raw_schema_v2 -----------------------------------------


def test_extract_raw_schema_v2_from_columns():
    payload = {
        "action": "I",
        "columns": [
            {"name": "id", "type": "integer", "value": 1},
            {"name": "name", "type": "character varying(64)", "value": "alice"},
        ],
    }
    rs = Wal2JsonIngester._extract_raw_schema_v2(payload)
    assert [t[0] for t in rs] == ["id", "name"]
    assert [t[1] for t in rs] == ["integer", "character varying(64)"]


def test_extract_raw_schema_v2_falls_back_to_identity_on_delete():
    payload = {
        "action": "D",
        "identity": [
            {"name": "id", "type": "integer", "value": 7},
        ],
    }
    rs = Wal2JsonIngester._extract_raw_schema_v2(payload)
    assert [t[0] for t in rs] == ["id"]


def test_extract_raw_schema_v2_rejects_empty_payload():
    with pytest.raises(TabsdataException) as ei:
        Wal2JsonIngester._extract_raw_schema_v2({"action": "I"})
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_15


# --- value extraction ----------------------------------------------


def test_extract_new_values_v1_zips_columns_and_values():
    change = {
        "columnnames": ["id", "name"],
        "columnvalues": [1, "alice"],
    }
    assert Wal2JsonIngester._extract_new_values_v1(change) == {
        "id": 1, "name": "alice",
    }


def test_extract_new_values_v1_empty_when_missing():
    assert Wal2JsonIngester._extract_new_values_v1({}) == {}


def test_extract_old_values_v1_uses_oldkeys():
    change = {
        "oldkeys": {
            "keynames": ["id"],
            "keyvalues": [1],
        },
    }
    assert Wal2JsonIngester._extract_old_values_v1(change) == {"id": 1}


def test_extract_old_values_v1_empty_when_oldkeys_missing():
    assert Wal2JsonIngester._extract_old_values_v1({}) == {}


def test_extract_new_values_v2_from_columns():
    payload = {
        "columns": [
            {"name": "id", "type": "integer", "value": 1},
            {"name": "name", "type": "varchar", "value": "alice"},
        ],
    }
    assert Wal2JsonIngester._extract_new_values_v2(payload) == {
        "id": 1, "name": "alice",
    }


def test_extract_new_values_v2_empty_when_no_columns():
    assert Wal2JsonIngester._extract_new_values_v2({}) == {}


def test_extract_old_values_v2_from_identity():
    payload = {
        "identity": [{"name": "id", "type": "integer", "value": 7}],
    }
    assert Wal2JsonIngester._extract_old_values_v2(payload) == {"id": 7}


# --- base_offset / commit_offset ------------------------------------


def test_base_offset_is_zero_before_init_stream():
    bi = _ingester_with_tables(["public.users"])
    base = bi.base_offset()
    assert isinstance(base, LsnOffset)
    assert base.lsn == 0


def test_commit_offset_rejects_foreign_offset():
    bi = _ingester_with_tables(["public.users"])

    class _Foreign:
        pass

    with pytest.raises(TabsdataException) as ei:
        bi.commit_offset(_Foreign())  # type: ignore[arg-type]
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_16


# --- ingester defaults ----------------------------------------------


def test_ingester_type_is_wal2json():
    bi = _ingester_with_tables(["public.users"])
    assert bi.ingester_type == "wal2json"


def test_ingester_default_format_version_is_v2():
    assert DEFAULT_WAL2JSON_FORMAT_VERSION == 2


# =====================================================================
# Offset ownership -- Tabsdata persists the LSN in the stream state; the
# slot is only acknowledged once a batch is staged.
# =====================================================================


def test_commit_state_persists_lsn_in_stream_state():
    bi = _ingester_with_tables(["public.users"])
    spec = bi.commit_state(LsnOffset(lsn=5))
    assert isinstance(spec, dict)
    assert LsnOffset.read(spec[CDC_OFFSET_STATE_KEY]).lsn == 5


def test_offset_from_state_reconstructs_lsn_offset():
    bi = _ingester_with_tables(["public.users"])
    offset = bi.offset_from_state(LsnOffset(lsn=9).write())
    assert isinstance(offset, LsnOffset)
    assert offset.lsn == 9


def test_resume_offset_round_trips_persisted_lsn():
    bi = _ingester_with_tables(["public.users"])
    state = bi.commit_state(LsnOffset(lsn=11))
    resumed = bi.resume_offset(first_stage=False, stream_state=state)
    assert isinstance(resumed, LsnOffset)
    assert resumed.lsn == 11


def test_resume_offset_first_stage_ignores_persisted_state():
    bi = _ingester_with_tables(["public.users"])
    state = bi.commit_state(LsnOffset(lsn=11))
    assert bi.resume_offset(first_stage=True, stream_state=state) is None
    assert bi.resume_offset(first_stage=False, stream_state=None) is None


def test_commit_offset_acknowledges_the_offset_to_the_slot():
    bi = _ingester_with_tables(["public.users"])
    bi._cursor = MagicMock()
    bi._mark = LsnOffset(lsn=99)

    bi.commit_offset(LsnOffset(lsn=7))

    bi._cursor.send_feedback.assert_called_once_with(flush_lsn=7, reply=True)


def _replication_cursor(monkeypatch, bi: Wal2JsonIngester) -> MagicMock:
    bi._conn._uri.return_value = "postgresql://h:5432/d"
    bi._conn.credentials.user = "u"
    bi._conn.credentials.password = "p"
    cursor = MagicMock()
    replicator = MagicMock()
    replicator.cursor.return_value = cursor
    monkeypatch.setattr(
        "tabsdatak.conn.postgres._cdc_impl.psycopg2.connect",
        lambda **settings: replicator,
    )
    return cursor


def test_init_stream_acknowledges_a_seeded_mark(monkeypatch):
    bi = _ingester_with_tables(["public.users"])
    cursor = _replication_cursor(monkeypatch, bi)
    bi.seed_offset(LsnOffset(lsn=42))

    bi.init_stream()

    assert cursor.start_replication.call_args.kwargs["start_lsn"] == 42
    cursor.send_feedback.assert_called_once_with(flush_lsn=42, reply=True)


def test_init_stream_does_not_acknowledge_a_cold_head_start(monkeypatch):
    bi = _ingester_with_tables(["public.users"])
    bi._start_from = "head"
    cursor = _replication_cursor(monkeypatch, bi)

    bi.init_stream()

    assert cursor.start_replication.call_args.kwargs["start_lsn"] == 0
    cursor.send_feedback.assert_not_called()


# =====================================================================
# TransactionSnapshotSession
# =====================================================================


class _Result:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _Connection:
    def __init__(self, engine: "_Engine", number: int):
        self._engine = engine
        self.number = number

    def execution_options(self, **options):
        self._engine.log.append((self.number, "options", options))
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def close(self):
        pass

    def execute(self, statement, parameters=None):
        sql = str(statement)
        self._engine.log.append((self.number, "execute", sql))
        if "pg_replication_slots" in sql:
            return _Result(1)
        if "pg_current_wal_lsn" in sql:
            return _Result(4096)
        if "pg_export_snapshot" in sql:
            return _Result("00000003-0000001B-1")
        raise AssertionError(f"unexpected statement: {sql}")


class _Engine:
    def __init__(self):
        self.log: list[tuple] = []
        self._connections = 0

    def connect(self):
        self._connections += 1
        return _Connection(self, self._connections)

    def dispose(self):
        pass


def _executed(engine: _Engine, fragment: str) -> tuple[int, int]:
    return next(
        (index, entry[0])
        for index, entry in enumerate(engine.log)
        if entry[1] == "execute" and fragment in entry[2]
    )


def test_the_wal_position_is_read_before_the_snapshot_is_exported():
    engine = _Engine()
    session = TransactionSnapshotSession(engine=engine, replication_slot="slot")
    lsn_at, lsn_connection = _executed(engine, "pg_current_wal_lsn")
    export_at, holder_connection = _executed(engine, "pg_export_snapshot")
    assert lsn_at < export_at
    assert lsn_connection != holder_connection
    assert session.offset.lsn == 4096


def test_the_snapshot_holder_is_a_read_only_repeatable_read_transaction():
    engine = _Engine()
    TransactionSnapshotSession(engine=engine, replication_slot="slot")
    _, holder_connection = _executed(engine, "pg_export_snapshot")
    assert (
        holder_connection,
        "options",
        {"isolation_level": "REPEATABLE READ", "postgresql_readonly": True},
    ) in engine.log
