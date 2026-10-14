#
# Copyright 2026 Tabsdata Inc.
#

"""``_cdc_impl.py`` -- pure-function coverage for the Postgres CDC
dialect, wal2json ingester and offset. Catalog / engine calls
(``server_timezone``, ``get_catalog_columns``, slot create /
``start_replication``, ``poll_changes``) need a real Postgres CDC
domain and live in the integration tests (``test_plugin_cdc.py``)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import psycopg2
import pyarrow as pa
import pytest
import sqlalchemy.types as sa_types
from sqlalchemy.dialects.postgresql import BYTEA, JSON, JSONB, OID, TEXT
from sqlalchemy.types import LargeBinary, Text

from tabsdatak.api import BatchThresholds, Secret, StreamThresholds

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._ledger import FileLedger

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._lifecycle import StreamLifecycle

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._plugin import RunLifecycle

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._sentinels import BufferSentinel, DiskSentinel

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._signal import StopSignal
from tabsdatak.conn.common.sql.cdc._ingester import CDC_OFFSET_STATE_KEY
from tabsdatak.conn.common.sql.cdc._model import TableObject, TrackedTable
from tabsdatak.conn.common.sql.cdc._runner import CdcRunConfig
from tabsdatak.conn.common.sql.cdc.constants import CdcOperation
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.postgres import _cdc_impl
from tabsdatak.conn.postgres._cdc_impl import (
    DEFAULT_WAL2JSON_FORMAT_VERSION,
    LsnOffset,
    PostgresDialect,
    SlotSnapshotSession,
    Wal2JsonIngester,
    accepted_gap,
    lsn_to_int,
    lsn_to_text,
)
from tabsdatak.conn.postgres._cdc_plugin import PostgresCdcStreamSrcPlugin
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


@pytest.mark.parametrize(
    ("row", "logged"),
    [
        (("f", False, False), True),
        (("d", True, False), True),
        (("d", False, False), False),
        (("i", False, True), True),
        (("i", False, False), False),
        (("n", True, False), False),
        (None, False),
    ],
    ids=[
        "full",
        "default-with-primary-key",
        "default-without-usable-primary-key",
        "using-index",
        "using-a-dropped-index",
        "nothing",
        "no-such-table",
    ],
)
def test_logs_row_identity_decides_from_the_catalog(d, row, logged):
    connection = MagicMock()
    connection.execute.return_value.fetchone.return_value = row
    assert d.logs_row_identity(connection, "public", "orders") is logged
    statement, params = connection.execute.call_args.args
    assert "condeferrable" in str(statement)
    assert params == {"schema": "public", "table": "orders"}


def test_a_table_without_a_logged_row_identity_fails_the_stream(monkeypatch):
    bi = _ingester_with_tables(["public.docs"])
    monkeypatch.setattr(
        PostgresDialect,
        "logs_row_identity",
        lambda self, connection, schema, table: False,
    )
    with pytest.raises(TabsdataException) as ei:
        bi._check_row_identity(MagicMock(), bi._tracked_tables)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_31
    assert "public.docs" in str(ei.value)


def test_a_table_with_a_logged_row_identity_passes(monkeypatch):
    bi = _ingester_with_tables(["public.docs"])
    monkeypatch.setattr(
        PostgresDialect,
        "logs_row_identity",
        lambda self, connection, schema, table: True,
    )
    bi._check_row_identity(MagicMock(), bi._tracked_tables)


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


SYSTEM_ID = "7412345678901234567"


def _identified(lsn: int, timeline: int = 1) -> LsnOffset:
    return LsnOffset(lsn=lsn, system_identifier=SYSTEM_ID, timeline=timeline)


def test_lsn_offset_round_trip():
    a = _identified(12345, timeline=3)
    serialised = a.write()
    assert isinstance(serialised, str)
    b = LsnOffset.read(serialised)
    assert (b.lsn, b.system_identifier, b.timeline) == (12345, SYSTEM_ID, 3)


@pytest.mark.parametrize(
    "state",
    [
        "",
        "{}",
        '{"lsn": 5}',
        '{"lsn": 5, "system_identifier": null, "timeline": null}',
        '{"lsn": "5", "system_identifier": "1", "timeline": 1}',
        "not json",
    ],
)
def test_lsn_offset_read_rejects_an_unexpected_format(state):
    with pytest.raises(TabsdataException) as ei:
        LsnOffset.read(state)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_29


def test_lsn_offset_update_keeps_the_server_it_was_read_from():
    a = _identified(1, timeline=2)
    a.update(LsnOffset(lsn=999))
    assert (a.lsn, a.system_identifier, a.timeline) == (999, SYSTEM_ID, 2)


def test_lsn_offset_copy_keeps_the_server_it_was_read_from():
    b = _identified(1, timeline=2).copy()
    assert (b.system_identifier, b.timeline) == (SYSTEM_ID, 2)


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
        segment=900,
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
        segment=900,
    )

    assert change is not None
    assert change.operation is CdcOperation.TRUNCATE
    assert change.schema.columns == ()
    assert change.new_values == {}


def test_truncate_keeps_the_transaction_position():
    """Postgres truncates inside the transaction that issued them, so the
    row takes that transaction's commit position and its sequence, and sorts
    between the changes around it."""
    bi = _truncate_ready()

    change = bi._truncate_change(
        payload=_truncate_payload(),
        xid="88",
        sequence=5,
        timestamp=TS,
        offset=LsnOffset(lsn=1234),
        segment=5000,
    )

    assert change.segment == 5000
    assert change.offset.lsn == 1234
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
            segment=900,
        )
        is None
    )


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


# --- base_offset / confirm_lsn --------------------------------------


def test_base_offset_is_zero_before_init_stream():
    bi = _ingester_with_tables(["public.users"])
    base = bi.base_offset()
    assert isinstance(base, LsnOffset)
    assert base.lsn == 0


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


def test_stage_state_persists_lsn_in_stream_state():
    bi = _ingester_with_tables(["public.users"])
    spec = bi.stage_state(_identified(5))
    assert isinstance(spec, dict)
    assert LsnOffset.read(spec[CDC_OFFSET_STATE_KEY]).lsn == 5


def test_the_keeper_reads_back_an_lsn_offset():
    bi = _ingester_with_tables(["public.users"])
    offset = bi.offsets.read(_identified(9).write())
    assert isinstance(offset, LsnOffset)
    assert offset.lsn == 9


def test_resume_offset_round_trips_persisted_lsn():
    bi = _ingester_with_tables(["public.users"])
    state = bi.stage_state(_identified(11))
    resumed = bi.resume_offset(first_stage=False, stream_state=state)
    assert isinstance(resumed, LsnOffset)
    assert resumed.lsn == 11


def test_resume_offset_first_stage_ignores_persisted_state():
    bi = _ingester_with_tables(["public.users"])
    state = bi.stage_state(_identified(11))
    assert bi.resume_offset(first_stage=True, stream_state=state) is None
    assert bi.resume_offset(first_stage=False, stream_state=None) is None


def test_acknowledging_the_offset_confirms_the_lsn_to_the_slot():
    bi = _ingester_with_tables(["public.users"])
    bi._cursor = MagicMock()
    bi._mark = LsnOffset(lsn=99)

    bi.acknowledge_offset(LsnOffset(lsn=7))

    bi._cursor.send_feedback.assert_called_once_with(flush_lsn=7, reply=True)


def _replication_cursor(monkeypatch, bi: Wal2JsonIngester) -> MagicMock:
    bi._conn._uri.return_value = "postgresql://h:5432/d"
    bi._conn.credentials.user = "u"
    bi._conn.credentials.password = "p"
    cursor = MagicMock()
    cursor.fetchone.return_value = (SYSTEM_ID, 1, "0/0", "d")
    replicator = MagicMock()
    replicator.cursor.return_value = cursor
    monkeypatch.setattr(
        "tabsdatak.conn.postgres._cdc_impl.psycopg2.connect",
        lambda **settings: replicator,
    )
    monkeypatch.setattr(bi, "_slot_positions", lambda: (0, "reserved", 10**6))
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


def test_init_stream_records_the_server_in_the_mark(monkeypatch):
    bi = _ingester_with_tables(["public.users"])
    _replication_cursor(monkeypatch, bi)
    bi.seed_offset(LsnOffset(lsn=42))

    bi.init_stream()

    assert (bi._mark.system_identifier, bi._mark.timeline) == (SYSTEM_ID, 1)
    assert bi.base_offset().system_identifier == SYSTEM_ID


def _resume(monkeypatch, mark, *, slot=(0, "reserved", 10**6), timeline=1, history=""):
    bi = _ingester_with_tables(["public.users"])
    cursor = _replication_cursor(monkeypatch, bi)
    answers = {
        "IDENTIFY_SYSTEM": (SYSTEM_ID, timeline, "0/0", "d"),
        "TIMELINE_HISTORY": (f"0000000{timeline}.history", history.encode()),
    }
    cursor.execute.side_effect = lambda sql: setattr(
        cursor, "_last", next(v for k, v in answers.items() if sql.startswith(k))
    )
    cursor.fetchone.side_effect = lambda: cursor._last
    monkeypatch.setattr(bi, "_slot_positions", lambda: slot)
    bi.seed_offset(mark)
    return bi, cursor


def test_a_resume_the_server_can_serve_starts_replication(monkeypatch):
    bi, cursor = _resume(monkeypatch, _identified(500), slot=(400, "reserved", 900))
    bi.init_stream()
    assert cursor.start_replication.call_args.kwargs["start_lsn"] == 500


@pytest.mark.parametrize(
    ("slot", "code"),
    [
        (None, PostgresErrorCode.POSTGRES_23),
        ((400, "lost", 900), PostgresErrorCode.POSTGRES_24),
        ((600, "reserved", 900), PostgresErrorCode.POSTGRES_25),
        ((None, "reserved", 900), PostgresErrorCode.POSTGRES_25),
        ((400, "reserved", 450), PostgresErrorCode.POSTGRES_26),
    ],
)
def test_a_resume_the_server_cannot_serve_fails(monkeypatch, slot, code):
    bi, cursor = _resume(monkeypatch, _identified(500), slot=slot)
    with pytest.raises(TabsdataException) as ei:
        bi.init_stream()
    assert ei.value.api_error is code
    cursor.start_replication.assert_not_called()


def test_a_resume_on_another_cluster_fails(monkeypatch):
    mark = LsnOffset(lsn=500, system_identifier="1", timeline=1)
    bi, _ = _resume(monkeypatch, mark)
    with pytest.raises(TabsdataException) as ei:
        bi.init_stream()
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_27


def test_a_resume_before_the_timeline_switch_continues(monkeypatch):
    history = "1\t0/2000\tno recovery target specified\n"
    bi, cursor = _resume(monkeypatch, _identified(0x1000), timeline=2, history=history)
    bi.init_stream()
    assert bi._mark.timeline == 2
    cursor.start_replication.assert_called_once()


@pytest.mark.parametrize(
    "history",
    ["1\t0/800\tno recovery target specified\n", "3\t0/9000\tother\n", ""],
)
def test_a_resume_past_the_timeline_switch_fails(monkeypatch, history):
    bi, _ = _resume(monkeypatch, _identified(0x1000), timeline=2, history=history)
    with pytest.raises(TabsdataException) as ei:
        bi.init_stream()
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_28


def test_a_head_start_checks_no_slot_position(monkeypatch):
    bi, cursor = _resume(monkeypatch, _identified(0), slot=None)
    bi.init_stream()
    assert cursor.start_replication.call_args.kwargs["start_lsn"] == 0


# --- A gap the configuration accepts ---------------------------------


def test_an_accepted_gap_is_read_as_the_failure_writes_it():
    assert accepted_gap(None) is None
    assert accepted_gap(" 0/7173FE8 - 0/7175C90 ") == (
        lsn_to_int("0/7173FE8"),
        lsn_to_int("0/7175C90"),
    )


@pytest.mark.parametrize("value", ["0/7173FE8", "0/1-0/2-0/3", "x/1-0/2", ""])
def test_a_malformed_accepted_gap_fails(value):
    with pytest.raises(TabsdataException) as ei:
        accepted_gap(value)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_33


def test_a_gap_failure_names_the_gap_to_accept(monkeypatch):
    bi, _ = _resume(monkeypatch, _identified(0x1F4), slot=(0x258, "reserved", 900))
    with pytest.raises(TabsdataException) as ei:
        bi.init_stream()
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_25
    assert "'0/1F4-0/258'" in str(ei.value)
    assert accepted_gap("0/1F4-0/258") == (0x1F4, 0x258)


class Recording(StreamLifecycle):
    def __init__(self, checkpoints: list) -> None:
        self.checkpoints = checkpoints

    def checkpoint(self, offset) -> bool:
        self.checkpoints.append(offset)
        return True


def test_the_accepted_gap_resumes_at_the_slot_and_records_it(monkeypatch):
    bi, cursor = _resume(monkeypatch, _identified(500), slot=(600, "reserved", 900))
    bi._accepted_gap = (500, 600)
    recorded = []
    bi.attach_lifecycle(Recording(recorded))

    bi.init_stream()

    assert cursor.start_replication.call_args.kwargs["start_lsn"] == 600
    assert [offset.lsn for offset in recorded] == [600]
    assert recorded[0].system_identifier == SYSTEM_ID


@pytest.mark.parametrize("gap", [(500, 700), (400, 600)], ids=["later", "earlier"])
def test_another_gap_than_the_accepted_one_fails(monkeypatch, gap):
    """A setting left in place after its gap was skipped matches no later
    one, so it cannot hide a gap nobody looked at."""
    bi, cursor = _resume(monkeypatch, _identified(500), slot=(600, "reserved", 900))
    bi._accepted_gap = gap
    with pytest.raises(TabsdataException) as ei:
        bi.init_stream()
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_25
    cursor.start_replication.assert_not_called()

# --- The slot's own moves ---------------------------------------------


PID = 4242


def _attached(monkeypatch, *, staged: int, rows: list) -> tuple:
    """An ingester attached at `staged`, whose reads of the slot return
    `rows` in turn, recording what it hands its checkpoint."""
    bi = _ingester_with_tables(["public.t"])
    bi._replicator = MagicMock()
    bi._replicator.get_backend_pid.return_value = PID
    bi._cursor = MagicMock()
    bi._base_offset = _identified(staged)
    bi._slot_floor = bi._recorded = staged
    answers = iter(rows)
    monkeypatch.setattr(bi, "_slot_row", lambda: next(answers))
    recorded = []
    bi.attach_lifecycle(Recording(recorded))
    return bi, recorded


def test_a_move_of_the_held_slot_is_recorded(monkeypatch):
    bi, recorded = _attached(monkeypatch, staged=100, rows=[(150, PID)])
    bi._watch_slot()
    assert [(o.lsn, o.system_identifier) for o in recorded] == [(150, SYSTEM_ID)]


@pytest.mark.parametrize("row", [(150, 7), (150, None), (None, PID), None], ids=str)
def test_a_slot_not_held_by_the_stream_is_not_taken_as_its_move(monkeypatch, row):
    """Only while this stream's walsender holds the slot can nothing but the
    stream have moved it."""
    bi, recorded = _attached(monkeypatch, staged=100, rows=[row])
    bi._watch_slot()
    assert recorded == []


def test_the_slot_is_read_once_per_interval(monkeypatch):
    bi, recorded = _attached(monkeypatch, staged=100, rows=[(150, PID)])
    bi._watch_slot()
    bi._watch_slot()
    assert [o.lsn for o in recorded] == [150]


def test_an_unchanged_slot_records_nothing(monkeypatch):
    bi, recorded = _attached(monkeypatch, staged=100, rows=[(100, PID)])
    bi._watch_slot()
    assert recorded == []


def test_a_failed_read_of_the_slot_does_not_stop_the_stream(monkeypatch):
    bi, recorded = _attached(monkeypatch, staged=100, rows=[])

    def fail():
        raise _cdc_impl.psycopg2.OperationalError("gone")

    monkeypatch.setattr(bi, "_slot_row", fail)
    bi._watch_slot()
    assert recorded == []


def test_a_stage_after_the_slot_moved_past_its_changes_takes_the_slot(monkeypatch):
    """Changes buffered up to 120 and the slot moved to 150: psycopg2 moves
    it only past what it has delivered and seen confirmed, so the buffered
    transactions commit after 150, and resuming there delivers them again.
    The stage records 150 and leaves the buffer's offset as it was."""
    bi, recorded = _attached(monkeypatch, staged=100, rows=[(150, PID)])
    bi._watch_slot()
    buffered = _identified(120)

    state = bi.stage_state(buffered)

    assert LsnOffset.read(state[CDC_OFFSET_STATE_KEY]).lsn == 150
    assert buffered.lsn == 120
    assert [o.lsn for o in recorded] == [150]


def test_a_stage_past_the_slot_keeps_its_own_position(monkeypatch):
    bi, _ = _attached(monkeypatch, staged=100, rows=[(150, PID)])
    bi._watch_slot()
    state = bi.stage_state(_identified(180))
    assert LsnOffset.read(state[CDC_OFFSET_STATE_KEY]).lsn == 180


def test_a_stage_already_at_the_slot_is_not_recorded_again(monkeypatch):
    bi, recorded = _attached(monkeypatch, staged=100, rows=[(180, PID)])
    bi.stage_state(_identified(180))
    bi._watch_slot()
    assert recorded == []


# --- A clean stop -----------------------------------------------------


def test_a_clean_stop_records_where_the_released_slot_ends(monkeypatch):
    """The server releases the slot after applying every feedback message,
    so its position once released is the final one."""
    bi, recorded = _attached(
        monkeypatch, staged=100, rows=[(150, PID), (160, PID), (170, None)]
    )
    monkeypatch.setattr(_cdc_impl.time, "sleep", lambda _: None)
    replicator, cursor = bi._replicator, bi._cursor

    bi.settle_offset()

    assert [o.lsn for o in recorded] == [170]
    replicator.close.assert_called_once()
    cursor.close.assert_called_once()
    assert (bi._replicator, bi._cursor) == (None, None)


@pytest.mark.parametrize(
    "rows", [[(170, 7)], [None]], ids=["taken-by-another", "dropped"]
)
def test_a_clean_stop_records_nothing_it_cannot_tell_is_its_own(monkeypatch, rows):
    bi, recorded = _attached(monkeypatch, staged=100, rows=rows)
    bi.settle_offset()
    assert recorded == []


def test_a_clean_stop_gives_up_on_a_slot_never_released(monkeypatch):
    bi, recorded = _attached(monkeypatch, staged=100, rows=[(150, PID)] * 3)
    monkeypatch.setattr(_cdc_impl, "SLOT_RELEASE_TIMEOUT_SECONDS", 0)
    bi.settle_offset()
    assert recorded == []


def test_a_stream_never_attached_settles_nothing(monkeypatch):
    bi = _ingester_with_tables(["public.t"])
    monkeypatch.setattr(bi, "_slot_row", lambda: pytest.fail("slot read"))
    bi.settle_offset()


class _StagedCtx:
    def __init__(self, staged: int) -> None:
        self.first_stage = False
        self.thresholds = StreamThresholds(batch=BatchThresholds(max_rows=1 << 62))
        self.stream_state = {CDC_OFFSET_STATE_KEY: _identified(staged).write()}
        self.checkpoints: list[dict] = []
        self.staged: list[dict] = []

    def checkpoint(self, new_state) -> None:
        self.checkpoints.append(new_state)

    def stage(self, tables_files, new_state) -> None:
        self.staged.append(new_state)


def _armed(monkeypatch, tmp_path, *, staged: int, moved: int) -> tuple:
    """A run staged up to `staged` whose ledger holds a transaction not staged
    yet, attached to a slot the watch reads at `moved`."""
    bi, _ = _attached(monkeypatch, staged=staged, rows=[(moved, PID)])
    ctx = _StagedCtx(staged)
    plugin = PostgresCdcStreamSrcPlugin()
    run = plugin.build_run(
        ctx=ctx, ingester=bi, cfg=CdcRunConfig(), stop_signal=StopSignal()
    )
    run.staged = _identified(staged)
    ledger = FileLedger(landing=tmp_path, slots=["public.t"])
    ledger.write("public.t", pa.table({"id": [4]}))
    buffer = BufferSentinel(
        ledger=ledger,
        encoder=MagicMock(),
        slots=["public.t"],
        max_rows=None,
        max_bytes=None,
        max_secs=60,
        offset=_identified(0x2EF8DA0),
    )
    disk = DiskSentinel(
        ledger=ledger, buffer=buffer, max_rows=None, max_bytes=None, max_secs=60
    )
    lifecycle = RunLifecycle(plugin, run)
    lifecycle.arm(ledger, disk, buffer)
    bi.attach_lifecycle(lifecycle)
    return bi, ctx, plugin, run, ledger, disk, buffer


def test_a_slot_move_past_unstaged_changes_is_not_recorded(monkeypatch, tmp_path):
    """CDC-5: a stream staged up to 0/2EF8E28, then read a whole transaction
    (S4, commit ending 0/2EF8F08) into its ledger without staging it, and
    psycopg2 moved the held slot to 0/2EF8F08 on a keepalive. The slot watch
    must not record a position past what is staged while S4 waits to be
    staged: a crash before the next stage would resume after S4 and lose it."""
    staged, moved = 0x2EF8E28, 0x2EF8F08
    bi, ctx, *_ = _armed(monkeypatch, tmp_path, staged=staged, moved=moved)

    bi._watch_slot()

    recorded = [
        LsnOffset.read(state[CDC_OFFSET_STATE_KEY]).lsn for state in ctx.checkpoints
    ]
    assert all(lsn <= staged for lsn in recorded), [lsn_to_text(n) for n in recorded]


def test_a_slot_move_past_unstaged_changes_is_staged_with_them(monkeypatch, tmp_path):
    staged, moved = 0x2EF8E28, 0x2EF8F08
    bi, ctx, plugin, run, ledger, disk, buffer = _armed(
        monkeypatch, tmp_path, staged=staged, moved=moved
    )
    bi._watch_slot()
    # Left to the advanced offset alone, without the lift at stage time.
    bi._slot_floor = staged

    buffer.flush()
    assert plugin.publish(run, ledger, disk, buffer) is True

    [state] = ctx.staged
    assert LsnOffset.read(state[CDC_OFFSET_STATE_KEY]).lsn == moved


# =====================================================================
# SlotSnapshotSession
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
            return _Result(1 if self._engine.slot_exists else None)
        if "set transaction snapshot" in sql:
            return _Result(None)
        if "select now()" in sql:
            return _Result(
                datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=2)))
            )
        raise AssertionError(f"unexpected statement: {sql}")


class _Engine:
    def __init__(self, monkeypatch, slot_exists: bool = True):
        self.log: list[tuple] = []
        self.slot_exists = slot_exists
        self.commands: list[str] = []
        self._connections = 0
        cursor = MagicMock()
        cursor.execute.side_effect = self._command
        cursor.fetchone.side_effect = [
            (SYSTEM_ID, 4, "0/0", "d"),
            ("slot_snapshot", "0/1000", "00000003-0000001B-1", "wal2json"),
        ]
        replicator = MagicMock()
        replicator.cursor.return_value = cursor
        monkeypatch.setattr(
            "tabsdatak.conn.postgres._cdc_impl.psycopg2.connect",
            lambda **settings: replicator,
        )

    def _command(self, command: str) -> None:
        self.commands.append(command)
        creating = command.startswith("CREATE_REPLICATION_SLOT slot LOGICAL")
        if creating and self.slot_exists:
            raise psycopg2.errors.DuplicateObject()

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


def test_the_load_time_is_the_snapshot_transaction_start_in_utc(monkeypatch):
    engine = _Engine(monkeypatch)
    session = _reused_session(engine)

    list(session.tables({"public.t": MagicMock()}))

    assert session.timestamp == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    snapshot_at, snapshot_connection = _executed(engine, "set transaction snapshot")
    now_at, now_connection = _executed(engine, "select now()")
    assert snapshot_at < now_at
    assert snapshot_connection == now_connection


def _reused_session(engine: _Engine) -> SlotSnapshotSession:
    return SlotSnapshotSession(
        engine=engine, replication_settings={}, replication_slot="slot", create=False
    )


def _creating_session(engine: _Engine) -> SlotSnapshotSession:
    return SlotSnapshotSession(
        engine=engine, replication_settings={}, replication_slot="slot", create=True
    )


def test_a_reused_slot_is_left_alone_and_the_snapshot_comes_from_a_temporary_one(
    monkeypatch,
):
    """A snapshot taken beside the slot between two readings of the WAL
    position misses a commit written before the first reading that turns
    visible after the snapshot forms, as one waiting on a synchronous
    standby does; the server's own exported snapshot is consistent at the
    point it names, so the load resumes there with no window to
    deduplicate."""
    engine = _Engine(monkeypatch)
    session = _reused_session(engine)

    assert engine.commands == [
        "IDENTIFY_SYSTEM",
        "CREATE_REPLICATION_SLOT slot_snapshot TEMPORARY LOGICAL wal2json "
        "EXPORT_SNAPSHOT",
    ]
    assert session.offset.lsn == 0x1000
    assert session.window_end is None
    assert not any("pg_current_wal_lsn" in entry[2] for entry in engine.log)


def test_a_reused_slot_must_exist_before_its_snapshot_slot_is_created(monkeypatch):
    engine = _Engine(monkeypatch, slot_exists=False)
    with pytest.raises(TabsdataException) as ei:
        _reused_session(engine)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_22
    assert engine.commands == ["IDENTIFY_SYSTEM"]


def test_a_creating_load_snapshots_beside_a_slot_that_is_already_there(monkeypatch):
    """An attempt that created the slot and died before staging leaves it
    there; the next one loads beside it and the stream resumes it, rather
    than refusing until someone drops the slot by hand."""
    engine = _Engine(monkeypatch, slot_exists=True)
    session = _creating_session(engine)

    assert engine.commands == [
        "IDENTIFY_SYSTEM",
        "CREATE_REPLICATION_SLOT slot LOGICAL wal2json EXPORT_SNAPSHOT",
        "CREATE_REPLICATION_SLOT slot_snapshot TEMPORARY LOGICAL wal2json "
        "EXPORT_SNAPSHOT",
    ]
    assert session.offset.lsn == 0x1000
    assert session.window_end is None


def test_a_creating_load_snapshots_from_the_slot_it_creates(monkeypatch):
    engine = _Engine(monkeypatch, slot_exists=False)
    session = _creating_session(engine)

    assert engine.commands == [
        "IDENTIFY_SYSTEM",
        "CREATE_REPLICATION_SLOT slot LOGICAL wal2json EXPORT_SNAPSHOT",
    ]
    assert session.offset.lsn == 0x1000
    assert not any("pg_replication_slots" in entry[2] for entry in engine.log)


def test_the_snapshot_slot_name_stays_within_what_the_server_accepts():
    assert _cdc_impl._snapshot_slot("slot") == "slot_snapshot"
    longest = _cdc_impl._snapshot_slot("s" * 80)
    assert len(longest) == 63
    assert longest.endswith("_snapshot")


def test_the_load_offset_records_the_server_it_was_read_from(monkeypatch):
    session = _reused_session(_Engine(monkeypatch))
    assert (session.offset.system_identifier, session.offset.timeline) == (
        SYSTEM_ID,
        4,
    )
    assert LsnOffset.read(session.offset.write()).timeline == 4


def test_a_created_slot_is_identified_before_it_exports_its_snapshot(monkeypatch):
    commands = []
    cursor = MagicMock()
    cursor.execute.side_effect = commands.append
    cursor.fetchone.side_effect = [
        (SYSTEM_ID, 5, "0/0", "d"),
        ("slot", "0/3000", "00000003-00000002-1", "wal2json"),
    ]
    replicator = MagicMock()
    replicator.cursor.return_value = cursor
    monkeypatch.setattr(
        "tabsdatak.conn.postgres._cdc_impl.psycopg2.connect",
        lambda **settings: replicator,
    )
    session = SlotSnapshotSession(
        engine=MagicMock(),
        replication_settings={},
        replication_slot="slot",
        create=True,
    )
    assert commands[0] == "IDENTIFY_SYSTEM"
    assert commands[1].startswith("CREATE_REPLICATION_SLOT slot LOGICAL")
    assert (session.offset.lsn, session.offset.timeline) == (0x3000, 5)
    # The slot's exported snapshot is exact: no change is both loaded and
    # streamed, so there is no window to deduplicate.
    assert session.window_end is None


# =====================================================================
# Format 2: every row is positioned at its transaction's commit
# =====================================================================


class _Message:
    def __init__(self, data_start: str, payload: dict):
        self.data_start = self.wal_end = lsn_to_int(data_start)
        self.payload = json.dumps(payload)


class _ReplicationCursor:
    wal_end = 0

    def __init__(self, messages: list[_Message]):
        self._messages = list(messages)
        self.confirmed: list[int] = []

    def read_message(self):
        return self._messages.pop(0) if self._messages else None

    def send_feedback(self, *, flush_lsn: int, reply: bool):
        self.confirmed.append(flush_lsn)


def _row(xid: int, row_id: int) -> dict:
    return {
        "action": "I",
        "xid": xid,
        "schema": "public",
        "table": "t",
        "columns": [{"name": "id", "type": "integer", "value": row_id}],
    }


def _transaction(xid: int, commit: str, rows: list[tuple[str, int]], end: str):
    begin = {"action": "B", "xid": xid, "timestamp": "2026-09-29 19:15:18+00"}
    return [
        _Message(rows[0][0], begin | {"lsn": commit}),
        *[_Message(start, _row(xid, row_id)) for start, row_id in rows],
        _Message(end, {"action": "C", "xid": xid, "lsn": commit}),
    ]


def _format_2_ingester(messages: list[_Message]) -> Wal2JsonIngester:
    bi = _ingester_with_tables(["public.t"])
    bi.init_schema_registry(CdcFormat())
    bi._cursor = _ReplicationCursor(messages)
    bi._poll_max_secs = 0.05
    return bi


def _logical_message(lsn: str, transactional: bool) -> _Message:
    return _Message(
        lsn,
        {
            "action": "M",
            "transactional": transactional,
            "prefix": "app",
            "content": "hello",
        },
    )


def test_a_logical_message_outside_a_transaction_is_passed_over():
    """``pg_logical_emit_message(false, ...)`` writes one with no transaction
    open, which any extension on the source may do; it carries nothing for
    a table, so the stream reads on rather than stopping."""
    after = _transaction(11, "0/300", [("0/280", 2)], "0/320")
    bi = _format_2_ingester([_logical_message("0/200", False), *after])

    changes = list(bi._poll_changes_v2(None))

    assert [change.new_values["id"] for change in changes] == [2]


def test_a_logical_message_inside_a_transaction_is_passed_over():
    inside = _transaction(11, "0/300", [("0/280", 2)], "0/320")
    inside.insert(2, _logical_message("0/290", True))
    bi = _format_2_ingester(inside)

    changes = list(bi._poll_changes_v2(None))

    assert [change.new_values["id"] for change in changes] == [2]


@pytest.mark.parametrize(
    "payload",
    [
        {"action": "T", "schema": "public", "table": "t"},
        _row(7, 1),
    ],
    ids=["truncate", "insert"],
)
def test_a_change_before_its_begin_stops_the_stream(payload):
    messages = [_Message("0/90", payload)]
    with pytest.raises(TabsdataException) as ei:
        list(_format_2_ingester(messages)._poll_changes_v2(None))
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_34
    assert "'T'" in str(ei.value) or "'I'" in str(ei.value)


def test_a_transaction_committed_later_is_positioned_after_an_earlier_one():
    """What wal2json sent for two overlapping transactions (verified live on
    PostgreSQL 18.6): T2 wrote a row, T1 wrote and committed, T2 wrote again
    and committed. T1 arrives first; T2's first row was written before T1's,
    so by its own position it would sort below T1 and be dropped once T1 is
    published."""
    t1 = _transaction(816, "0/368A110", [("0/368A090", 2)], "0/368A140")
    t2 = _transaction(
        815, "0/368A1C8", [("0/3689F90", 1), ("0/368A140", 3)], "0/368A1F8"
    )
    changes = list(_format_2_ingester(t1 + t2)._poll_changes_v2(None))

    assert [change.new_values["id"] for change in changes] == [2, 1, 3]
    assert [change.segment for change in changes] == [
        lsn_to_int("0/368A110"),
        lsn_to_int("0/368A1C8"),
        lsn_to_int("0/368A1C8"),
    ]
    assert [(c.segment, c.ordinal) for c in changes] == sorted(
        (c.segment, c.ordinal) for c in changes
    )
    # The offset still records where each row was written, which a resume
    # starts from.
    assert changes[1].offset.lsn == lsn_to_int("0/3689F90")


def test_the_offset_of_overlapping_transactions_never_moves_back():
    """CDC-5: S1 wrote its row first (0/1BDC230) and committed last; S2 wrote
    later (0/1BDC328) and committed first, so S2's row arrives first. Folding
    each change's offset into the stream's offset, as the buffer does when it
    flushes, must never move the recorded position backwards: a lower saved
    position than the slot's confirmed one fails the next start with a false
    POSTGRES_25 gap."""
    s2 = _transaction(968, "0/1BDC3F0", [("0/1BDC328", 2)], "0/1BDC3F0")
    s1 = _transaction(967, "0/1BDC420", [("0/1BDC230", 1)], "0/1BDC420")
    offset = _identified(0x1BDC1F8)
    positions = []
    for change in _format_2_ingester(s2 + s1)._poll_changes_v2(None):
        offset.update(change.offset)
        positions.append(offset.lsn)

    assert positions == sorted(positions), [lsn_to_text(n) for n in positions]


def _format_1_transaction(
    start: str, xid: int, row_ids: list[int], commit_end: str
) -> _Message:
    return _Message(
        start,
        {
            "xid": xid,
            "nextlsn": commit_end,
            "timestamp": "2026-09-29 19:15:18+00",
            "change": [
                {
                    "kind": "insert",
                    "schema": "public",
                    "table": "t",
                    "columnnames": ["id"],
                    "columntypes": ["integer"],
                    "columnvalues": [row_id],
                }
                for row_id in row_ids
            ],
        },
    )


def test_a_format_1_transaction_committed_later_is_positioned_after_an_earlier_one():
    """CDC-4: with wal2json format 1, T1 began at 0/368A090 and committed
    first; T2 began earlier, at 0/3689F90, and committed later, so T1's
    message arrives first. Once T1 is published, the publisher keeps only
    rows positioned after T1's, so every row of T2 must be positioned after
    every row of T1, as format 2 already does; today format 1 positions each
    row at its transaction's start and T2's rows are dropped."""
    t1 = _format_1_transaction("0/368A090", 816, [2], "0/368A1C8")
    t2 = _format_1_transaction("0/3689F90", 815, [1, 3], "0/368A2E0")
    bi = _format_2_ingester([t1, t2])
    bi._wal2json_format_version = 1

    changes = list(bi._poll_changes_v1(None))

    assert [change.new_values["id"] for change in changes] == [2, 1, 3]
    published = (changes[0].segment, changes[0].ordinal)
    assert all((c.segment, c.ordinal) > published for c in changes[1:]), [
        (lsn_to_text(c.segment), c.ordinal) for c in changes
    ]


def test_a_format_1_transaction_without_its_commit_position_fails():
    message = _format_1_transaction("0/100", 9, [1], "0/180")
    payload = json.loads(message.payload)
    del payload["nextlsn"]
    message.payload = json.dumps(payload)
    bi = _format_2_ingester([message])
    bi._wal2json_format_version = 1

    with pytest.raises(TabsdataException) as ei:
        list(bi._poll_changes_v1(None))
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_30


def test_a_begin_without_its_commit_position_fails():
    messages = _transaction(9, "0/100", [("0/90", 1)], "0/120")
    messages[0].payload = json.dumps(
        {"action": "B", "xid": 9, "timestamp": "2026-09-29 19:15:18+00"}
    )
    with pytest.raises(TabsdataException) as ei:
        list(_format_2_ingester(messages)._poll_changes_v2(None))
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_30


class _Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, secs: float) -> None:
        self.now += secs


class _PacedCursor(_ReplicationCursor):
    def __init__(self, messages: list[_Message], clock: _Clock, pause: float):
        super().__init__(messages)
        self._clock = clock
        self._pause = pause

    def read_message(self):
        self._clock.now += self._pause
        return super().read_message()


def _paced_ingester(monkeypatch, messages: list[_Message], pause: float):
    clock = _Clock()
    monkeypatch.setattr(_cdc_impl, "time", clock)
    bi = _format_2_ingester(messages)
    bi._cursor = _PacedCursor(messages, clock, pause)
    bi._poll_max_secs = 1
    return bi, clock


def test_a_transaction_longer_than_the_pending_budget_is_read_whole(monkeypatch):
    budget = _cdc_impl.PENDING_TIMEOUT_MULTIPLIER
    rows = [(f"0/{0x100 + 0x10 * i:X}", i) for i in range(1, 3 * budget)]
    messages = _transaction(7, "0/9000", rows, "0/9100")
    bi, clock = _paced_ingester(monkeypatch, messages, pause=1.0)

    changes = list(bi._poll_changes_v2(None))

    assert len(changes) == len(rows)
    assert clock.now > budget


def test_a_source_silent_mid_transaction_fails_the_stream(monkeypatch):
    messages = _transaction(7, "0/9000", [("0/100", 1)], "0/9100")[:-1]
    bi, _clock = _paced_ingester(monkeypatch, messages, pause=1.0)

    with pytest.raises(TabsdataException) as ei:
        list(bi._poll_changes_v2(None))
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_18


def test_a_naive_timestamp_keeps_its_wall_clock_whatever_the_server_zone():
    """A ``timestamp without time zone`` is a wall-clock reading: the stream
    publishes it as the initial load does, untouched, rather than tagging it
    with the server zone, which the naive Arrow column would then shift."""
    begin = {
        "action": "B",
        "xid": 9,
        "timestamp": "2026-09-29 19:15:18+00",
        "lsn": "0/100",
    }
    naive = {"name": "at", "type": "timestamp without time zone"}
    aware = {"name": "seen", "type": "timestamp with time zone"}
    row = {
        "action": "I",
        "xid": 9,
        "schema": "public",
        "table": "t",
        "columns": [
            {"name": "id", "type": "integer", "value": "1"},
            naive | {"value": "2026-01-01 12:00:00"},
            aware | {"value": "2026-01-01 12:00:00+02"},
        ],
    }
    messages = [
        _Message("0/90", begin),
        _Message("0/90", row),
        _Message("0/120", {"action": "C", "xid": 9, "lsn": "0/100"}),
    ]
    bi = _format_2_ingester(messages)
    bi._server_tz = timezone(timedelta(hours=2))

    [change] = list(bi._poll_changes_v2(None))

    assert change.new_values["at"] == datetime(2026, 1, 1, 12, 0)
    assert change.new_values["at"].tzinfo is None
    assert change.new_values["seen"] == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)


def test_conn_cfg_reaches_the_engine_and_the_replication_connection(monkeypatch):
    """psycopg2 keywords such as sslmode apply to the catalog engine and to
    the replication connection; the URI's address and the replication
    connection factory are never overridden by them."""
    bi = _ingester_with_tables(["public.users"])
    bi._conn._uri.return_value = "postgresql://etl:pw@h:5432/d"
    bi._conn.credentials = UserPassword(user=Secret("etl"), password=Secret("pw"))
    conn_cfg = {"sslmode": "require", "host": "x", "connection_factory": None}
    bi._conn.conn_cfg = conn_cfg
    calls = []
    monkeypatch.setattr(
        _cdc_impl.sqlalchemy_utils,
        "create_engine",
        lambda url, **kwargs: calls.append(kwargs) or "engine",
    )

    assert bi._build_engine() == "engine"
    settings = bi._replication_connection_settings()

    assert calls == [{"connect_args": conn_cfg}]
    assert settings["sslmode"] == "require"
    assert (settings["host"], settings["dbname"], settings["user"]) == ("h", "d", "etl")
    assert settings["connection_factory"] is _cdc_impl.LogicalReplicationConnection


def test_the_stream_asks_wal2json_for_commit_positions(monkeypatch):
    bi = _ingester_with_tables(["public.users"])
    cursor = _replication_cursor(monkeypatch, bi)
    bi.init_stream()
    options = cursor.start_replication.call_args.kwargs["options"]
    assert options["include-lsn"] == "true"
    assert options["numeric-data-types-as-string"] == "true"


def test_a_numeric_sent_as_text_keeps_every_digit():
    """With numbers sent as strings, a numeric reaches the Decimal exact; parsed
    as a JSON number it would have gone through a float first and kept 15
    digits of its 30. Integers and floats sent the same way still type."""
    begin = {
        "action": "B",
        "xid": 9,
        "timestamp": "2026-09-29 19:15:18+00",
        "lsn": "0/100",
    }
    row = {
        "action": "I",
        "xid": 9,
        "schema": "public",
        "table": "t",
        "columns": [
            {"name": "id", "type": "integer", "value": "7"},
            {
                "name": "amount",
                "type": "numeric(30,10)",
                "value": "12345678901234567890.1234567890",
            },
            {"name": "ratio", "type": "double precision", "value": "0.5"},
        ],
    }
    messages = [
        _Message("0/90", begin),
        _Message("0/90", row),
        _Message("0/120", {"action": "C", "xid": 9, "lsn": "0/100"}),
    ]

    [change] = list(_format_2_ingester(messages)._poll_changes_v2(None))

    assert change.new_values["amount"] == Decimal("12345678901234567890.1234567890")
    assert change.new_values["id"] == 7 and change.new_values["ratio"] == 0.5


# =====================================================================
# Attaching to a slot the previous stream still holds
# =====================================================================


# --- Idle confirmation ------------------------------------------------


def _idle(messages=(), *, wal_end: str = "0/900") -> tuple[Wal2JsonIngester, list]:
    """An ingester attached at 0/100 whose server has decoded up to
    `wal_end`, recording what it hands its checkpoint."""
    bi = _format_2_ingester(list(messages))
    bi._cursor.wal_end = lsn_to_int(wal_end)
    bi._base_offset = _identified(0x100)
    bi._slot_floor = bi._recorded = bi._confirmed = 0x100
    recorded = []
    bi.attach_lifecycle(Recording(recorded))
    return bi, recorded


def test_an_idle_poll_confirms_up_to_where_the_server_decoded():
    bi, recorded = _idle()

    assert list(bi._poll_changes_v2(None)) == []

    assert [offset.lsn for offset in recorded] == [0x900]
    assert recorded[0].system_identifier == SYSTEM_ID
    assert bi._cursor.confirmed == [0x900]
    assert (bi._slot_floor, bi._recorded) == (0x900, 0x900)
    assert bi.staged_offset(LsnOffset(lsn=0x500)).lsn == 0x900


def test_the_position_is_recorded_before_it_is_confirmed():
    """A crash between the two must find the slot at or behind the recorded
    offset, never past it, so a position the run does not record is not
    sent."""
    bi, _ = _idle()
    bi.attach_lifecycle(StreamLifecycle())

    assert list(bi._poll_changes_v2(None)) == []

    assert bi._cursor.confirmed == []
    assert (bi._slot_floor, bi._recorded) == (0x100, 0x100)


def test_delivered_changes_hold_the_idle_confirmation_until_they_are_staged():
    bi, recorded = _idle(_transaction(10, "0/300", [("0/280", 1)], "0/320"))

    [change] = list(bi._poll_changes_v2(None))
    assert bi._cursor.confirmed == []

    bi.confirm_lsn(change.offset)
    assert list(bi._poll_changes_v2(None)) == []
    assert bi._cursor.confirmed == [0x280, 0x900]
    assert [offset.lsn for offset in recorded] == [0x900]


def test_an_idle_confirmation_is_not_repeated_until_the_server_moves_on():
    bi, _ = _idle()
    list(bi._poll_changes_v2(None))
    list(bi._poll_changes_v2(None))
    assert bi._cursor.confirmed == [0x900]

    bi._cursor.wal_end = 0xA00
    list(bi._poll_changes_v2(None))
    assert bi._cursor.confirmed == [0x900, 0xA00]


def test_the_format_1_poll_confirms_when_idle_too():
    bi, recorded = _idle()

    assert list(bi._poll_changes_v1(None)) == []

    assert bi._cursor.confirmed == [0x900]
    assert [offset.lsn for offset in recorded] == [0x900]


class _HeldSlotCursor:
    def __init__(self, held_for: int):
        self.held_for = held_for
        self.attempts = 0

    def start_replication(self, **kwargs):
        from tabsdatak.conn.postgres import _cdc_impl

        self.attempts += 1
        if self.attempts <= self.held_for:
            raise _cdc_impl.psycopg2.errors.ObjectInUse(
                'replication slot "slot" is active for PID 4079'
            )


def test_a_slot_released_in_time_is_attached(monkeypatch):
    from tabsdatak.conn.postgres import _cdc_impl

    monkeypatch.setattr(_cdc_impl.time, "sleep", lambda seconds: None)
    bi = _ingester_with_tables(["public.t"])
    bi._mark = LsnOffset(lsn=0)
    bi._cursor = _HeldSlotCursor(held_for=2)
    bi._attach({})
    assert bi._cursor.attempts == 3


def test_a_slot_held_past_the_timeout_fails(monkeypatch):
    from tabsdatak.conn.postgres import _cdc_impl

    monkeypatch.setattr(_cdc_impl.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(_cdc_impl, "SLOT_ATTACH_TIMEOUT_SECONDS", 0)
    bi = _ingester_with_tables(["public.t"])
    bi._mark = LsnOffset(lsn=0)
    bi._cursor = _HeldSlotCursor(held_for=100)
    with pytest.raises(TabsdataException) as ei:
        bi._attach({})
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_32
    assert "PID 4079" in str(ei.value)
