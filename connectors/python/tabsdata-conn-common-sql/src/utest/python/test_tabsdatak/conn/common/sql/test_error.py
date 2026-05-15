from __future__ import annotations

import pytest

from tabsdatak.error import DEBUG_FRAMES_ENV_VAR, TabsdataException
from tabsdatak.conn.common.error import ConnException
from tabsdatak.conn.common.sql.error import (
    SqlCommonErrorCode,
    SqlCommonRuntimeException,
)


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


def test_sql_common_runtime_is_a_conn_exception():
    assert issubclass(SqlCommonRuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


# ---------------------------------------------------------------------------
# Error codes
# ---------------------------------------------------------------------------


def test_every_code_constructs_its_declared_exception(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    for code in SqlCommonErrorCode:
        exc = code.exception(engine="x")
        assert type(exc) is code.value.exception


def test_codes_are_numbered_consecutively_from_one():
    names = [code.name for code in SqlCommonErrorCode]
    assert names == [f"SQL_COMMON_{i}" for i in range(1, len(names) + 1)]


def test_sql_common_1_message_uses_engine_kwarg(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    exc = SqlCommonErrorCode.SQL_COMMON_1.exception(engine="weirdSqlEngine")
    assert "weirdSqlEngine" in str(exc)


def test_sql_common_1_is_a_runtime_exception(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    exc = SqlCommonErrorCode.SQL_COMMON_1.exception(engine="x")
    assert isinstance(exc, SqlCommonRuntimeException)


def test_sql_common_1_is_raised_from_factory_with_unknown_engine(monkeypatch):
    """Defence-in-depth: even if a connector lists an engine in its
    defaults' allow-list, the factory must reject it cleanly when no
    dispatch case matches."""
    from tabsdatak.conn.common.sql._engine import constants as const
    from tabsdatak.conn.common.sql._engine import factory

    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    bogus = "no-such-engine"
    defaults = {
        const.READ_ENGINE_KEY: bogus,
        const.AVAIL_READ_ENGINES_KEY: [bogus],
    }
    with pytest.raises(SqlCommonRuntimeException) as ei:
        factory.get_read_engine(None, defaults)
    assert bogus in str(ei.value)
