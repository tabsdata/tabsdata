from __future__ import annotations

import typing

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import CfgSpec, UserPassword
from tabsdatak.conn.snowflake import (
    AccountSpec,
    CredentialsSpec,
    DestCfgSpec,
    IdentifierSpec,
    IfTableExistsSpec,
    NameSpec,
    SnowflakeDestConn,
    SnowflakeDest,
    SnowflakeTableNamesSpec,
)
from tabsdatak.conn.snowflake.error import (
    SnowflakeErrorCode,
    SnowflakeValidateException,
)


pytestmark = pytest.mark.snowflake


# ---------------------------------------------------------------------------
# SnowflakeDestConn / SnowflakeDest round-trip
# ---------------------------------------------------------------------------


def test_snowflake_dest_full_round_trip():
    dest = SnowflakeDest(
        tables=["ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.ORDERS"],
        if_table_exists="replace",
        dest_cfg={"snowflake.logging_level": 20},
    )
    loaded = SnowflakeDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.ORDERS"]
    assert loaded.if_table_exists == "replace"
    assert loaded.dest_cfg == {"snowflake.logging_level": 20}


def test_snowflake_conn_validate_rejects_schema_without_database():
    with pytest.raises(SnowflakeValidateException) as ei:
        SnowflakeDestConn(
            account="ACME-MAIN",
            credentials=UserPassword(
                user=Secret("etl"),
                password=Secret("hunter2"),
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
    assert _hint(SnowflakeDestConn, "account") == AccountSpec
    assert _hint(SnowflakeDestConn, "credentials") == CredentialsSpec
    assert _hint(SnowflakeDestConn, "role") == NameSpec | None
    assert _hint(SnowflakeDestConn, "database") == NameSpec | None
    assert _hint(SnowflakeDestConn, "schema") == NameSpec | None
    assert _hint(SnowflakeDestConn, "warehouse") == NameSpec | None
    assert _hint(SnowflakeDestConn, "stage") == IdentifierSpec | None
    assert _hint(SnowflakeDestConn, "conn_cfg") == CfgSpec | None


def test_snowflake_dest_fields_wire_expected_specs():
    assert _hint(SnowflakeDest, "tables") == SnowflakeTableNamesSpec
    assert _hint(SnowflakeDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(SnowflakeDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_snowflake_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        SnowflakeDest(
            tables=["analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_3


def test_snowflake_dest_accepts_known_dest_cfg_keys():
    dest = SnowflakeDest(
        tables=["analytics.users"],
        dest_cfg={"snowflake.logging_level": 20},
    )
    loaded = SnowflakeDest.from_json(dest.to_json())
    assert loaded == dest


# ---------------------------------------------------------------------------
# SnowflakeDestConn init validation error code
# ---------------------------------------------------------------------------


def test_snowflake_conn_rejects_missing_account():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        SnowflakeDestConn(  # type: ignore[call-arg]
            credentials=UserPassword(
                user=Secret("u"),
                password=Secret("p"),
            ),
            database="DB",
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_1


def test_snowflake_conn_rejects_missing_credentials():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        SnowflakeDestConn(account="ACME", database="DB")  # type: ignore[call-arg]
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_1
