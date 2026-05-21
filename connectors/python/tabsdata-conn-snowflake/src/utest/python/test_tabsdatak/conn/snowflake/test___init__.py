from __future__ import annotations

import typing

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import CfgSpec, UserPassword
from tabsdatak.conn.snowflake import (
    AccountSpec,
    ConnSpec,
    CredentialsSpec,
    DestCfgSpec,
    IdentifierSpec,
    IfTableExistsSpec,
    NameSpec,
    SnowflakeConn,
    SnowflakeDest,
    SnowflakeTableNamesSpec,
)
from tabsdatak.conn.snowflake.error import (
    SnowflakeErrorCode,
    SnowflakeValidateException,
)


pytestmark = pytest.mark.snowflake


def _conn() -> SnowflakeConn:
    return SnowflakeConn(
        account="ACME-MAIN",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        role="ETL_ROLE",
        database="ANALYTICS",
        schema="PUBLIC",
        warehouse="ETL_WH",
        stage="MY_STAGE",
        conn_cfg={"snowflake.timeout": 30},
    )


# ---------------------------------------------------------------------------
# SnowflakeConn / SnowflakeDest round-trip
# ---------------------------------------------------------------------------


def test_snowflake_dest_full_round_trip():
    dest = SnowflakeDest(
        conn=_conn(),
        tables=["ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.ORDERS"],
        if_table_exists="replace",
        dest_cfg={"snowflake.logging_level": 20},
    )
    loaded = SnowflakeDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.account.value() == "ACME-MAIN"
    assert loaded.conn.role.value() == "ETL_ROLE"
    assert loaded.conn.database.value() == "ANALYTICS"
    assert loaded.conn.schema.value() == "PUBLIC"
    assert loaded.conn.warehouse.value() == "ETL_WH"
    assert loaded.conn.stage.value() == "MY_STAGE"
    assert loaded.conn.conn_cfg == {"snowflake.timeout": 30}
    assert loaded.tables == ["ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.ORDERS"]
    assert loaded.if_table_exists == "replace"
    assert loaded.dest_cfg == {"snowflake.logging_level": 20}


def test_snowflake_dest_round_trip_with_named_conn():
    dest = SnowflakeDest(
        conn=NamedConn(name="prod_snowflake"),
        tables=["ANALYTICS.PUBLIC.USERS"],
    )
    loaded = SnowflakeDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_snowflake"


def test_snowflake_conn_validate_rejects_schema_without_database():
    with pytest.raises(SnowflakeValidateException) as ei:
        SnowflakeConn(
            account="ACME-MAIN",
            credentials=UserPassword(
                user=Secret.lit("etl"),
                password=Secret.lit("hunter2"),
            ),
            schema="PUBLIC",
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_2


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_snowflake_conn_fields_wire_expected_specs():
    assert _hint(SnowflakeConn, "account") == AccountSpec
    assert _hint(SnowflakeConn, "credentials") == CredentialsSpec
    assert _hint(SnowflakeConn, "role") == NameSpec | None
    assert _hint(SnowflakeConn, "database") == NameSpec | None
    assert _hint(SnowflakeConn, "schema") == NameSpec | None
    assert _hint(SnowflakeConn, "warehouse") == NameSpec | None
    assert _hint(SnowflakeConn, "stage") == IdentifierSpec | None
    assert _hint(SnowflakeConn, "conn_cfg") == CfgSpec | None


def test_snowflake_dest_fields_wire_expected_specs():
    assert _hint(SnowflakeDest, "conn") == ConnSpec
    assert _hint(SnowflakeDest, "tables") == SnowflakeTableNamesSpec
    assert _hint(SnowflakeDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(SnowflakeDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_snowflake_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        SnowflakeDest(
            conn=_conn(),
            tables=["analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_3


def test_snowflake_dest_accepts_known_dest_cfg_keys():
    dest = SnowflakeDest(
        conn=_conn(),
        tables=["analytics.users"],
        dest_cfg={"snowflake.logging_level": 20},
    )
    loaded = SnowflakeDest.from_json(dest.to_json())
    assert loaded == dest


# ---------------------------------------------------------------------------
# SnowflakeConn init validation error code
# ---------------------------------------------------------------------------


def test_snowflake_conn_rejects_missing_account():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        SnowflakeConn(  # type: ignore[call-arg]
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
            database="DB",
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_1


def test_snowflake_conn_rejects_missing_credentials():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        SnowflakeConn(account="ACME", database="DB")  # type: ignore[call-arg]
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_1
