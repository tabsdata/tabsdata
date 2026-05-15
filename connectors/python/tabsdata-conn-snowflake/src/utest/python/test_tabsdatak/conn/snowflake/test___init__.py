
import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.snowflake.error import SnowflakeValidateException
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeConn, SnowflakeDest
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode


pytestmark = pytest.mark.snowflake


def _conn():
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


def test_snowflake_dest_full_round_trip():
    dest = SnowflakeDest(
        conn=_conn(),
        tables=["ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.ORDERS"],
        if_table_exists="replace",
        dest_cfg={"chunkSize": 2048},
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
    assert loaded.dest_cfg == {"chunkSize": 2048}


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
