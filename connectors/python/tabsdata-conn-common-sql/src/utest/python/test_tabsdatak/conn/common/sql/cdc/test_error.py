"""Error codes for the SQL-CDC reusable layer -- assert the enum
shape and that each code constructs its declared exception."""

from __future__ import annotations

from tabsdatak.conn.common.error import ConnException
from tabsdatak.conn.common.sql.cdc.error import (
    SqlCdcErrorCode,
    SqlCdcInitException,
    SqlCdcRuntimeException,
)
from tabsdatak.error import TabsdataException


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(SqlCdcRuntimeException, ConnException)


def test_init_exception_is_a_conn_exception():
    assert issubclass(SqlCdcInitException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_codes_are_numbered_consecutively_from_one():
    names = [code.name for code in SqlCdcErrorCode]
    assert names == [
        "SQL_CDC_1",
        "SQL_CDC_2",
        "SQL_CDC_3",
        "SQL_CDC_4",
        "SQL_CDC_5",
        "SQL_CDC_6",
        "SQL_CDC_7",
        "SQL_CDC_8",
    ]


def test_every_code_constructs_its_declared_exception(monkeypatch):
    from tabsdatak.error import DEBUG_FRAMES_ENV_VAR
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    for code in SqlCdcErrorCode:
        # Each ErrorDef is parameterized; pass dummy kwargs sufficient
        # to satisfy any `{...}` placeholders. ``ErrorDef.exception()``
        # ignores extras when the message has none.
        exc = code.exception(
            table="t",
            reason="r",
            schema="s",
            catalog=0,
            sa=0,
            expected="E",
            actual="A",
        )
        assert isinstance(exc, ConnException)
        assert type(exc) is code.value.exception
