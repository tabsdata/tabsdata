#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.error import ConnErrorCode, ConnException
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import constants as const
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine import factory
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.sqlalchemy_reader import (
    PandasReadEngine,
    PolarsReadEngine,
)
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.sqlalchemy_writer import (
    PolarsWriteEngine,
)
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql.error import SqlCommonRuntimeException


# ---------------------------------------------------------------------------
# Defaults helpers
# ---------------------------------------------------------------------------


def _read_defaults(engine: str = const.POLARS_SQL_ALCHEMY_READ_ENGINE):
    return {
        const.READ_ENGINE_KEY: engine,
        const.AVAIL_READ_ENGINES_KEY: [
            const.POLARS_SQL_ALCHEMY_READ_ENGINE,
            const.PANDAS_SQL_ALCHEMY_READ_ENGINE,
        ],
    }


def _write_defaults(engine: str = const.POLARS_SQL_ALCHEMY_WRITE_ENGINE):
    return {
        const.WRITE_ENGINE_KEY: engine,
        const.AVAIL_WRITE_ENGINES_KEY: [
            const.POLARS_SQL_ALCHEMY_WRITE_ENGINE,
        ],
    }


# ---------------------------------------------------------------------------
# get_read_engine
# ---------------------------------------------------------------------------


def test_get_read_engine_returns_polars_for_polars_default():
    eng = factory.get_read_engine(
        None,
        _read_defaults(const.POLARS_SQL_ALCHEMY_READ_ENGINE),
    )
    assert isinstance(eng, PolarsReadEngine)


def test_get_read_engine_returns_pandas_for_pandas_default():
    eng = factory.get_read_engine(
        None,
        _read_defaults(const.PANDAS_SQL_ALCHEMY_READ_ENGINE),
    )
    assert isinstance(eng, PandasReadEngine)


def test_get_read_engine_cfg_override_wins_over_default():
    # default says polars, cfg dict picks pandas -> pandas
    eng = factory.get_read_engine(
        {const.READ_ENGINE_KEY: const.PANDAS_SQL_ALCHEMY_READ_ENGINE},
        _read_defaults(const.POLARS_SQL_ALCHEMY_READ_ENGINE),
    )
    assert isinstance(eng, PandasReadEngine)


def test_get_read_engine_unknown_in_cfg_raises_conn_8():
    # cfg value outside the allow-list -> CONN_8 from _config.get_cfg
    with pytest.raises(ConnException) as ei:
        factory.get_read_engine(
            {const.READ_ENGINE_KEY: "bogus"},
            _read_defaults(),
        )
    assert type(ei.value) is ConnErrorCode.CONN_8.value.exception


def test_get_read_engine_unknown_in_defaults_raises_sql_common_1():
    # connector defaults list a value the factory has no dispatch for
    bogus = "phantom-engine"
    defaults = {
        const.READ_ENGINE_KEY: bogus,
        const.AVAIL_READ_ENGINES_KEY: [bogus],
    }
    with pytest.raises(SqlCommonRuntimeException) as ei:
        factory.get_read_engine(None, defaults)
    assert bogus in str(ei.value)


# ---------------------------------------------------------------------------
# get_write_engine
# ---------------------------------------------------------------------------


def test_get_write_engine_returns_polars_for_polars_default():
    eng = factory.get_write_engine(
        None,
        _write_defaults(const.POLARS_SQL_ALCHEMY_WRITE_ENGINE),
    )
    assert isinstance(eng, PolarsWriteEngine)


def test_get_write_engine_cfg_override_returns_polars():
    eng = factory.get_write_engine(
        {const.WRITE_ENGINE_KEY: const.POLARS_SQL_ALCHEMY_WRITE_ENGINE},
        _write_defaults(),
    )
    assert isinstance(eng, PolarsWriteEngine)


def test_get_write_engine_unknown_in_cfg_raises_conn_8():
    with pytest.raises(ConnException) as ei:
        factory.get_write_engine(
            {const.WRITE_ENGINE_KEY: "bogus"},
            _write_defaults(),
        )
    assert type(ei.value) is ConnErrorCode.CONN_8.value.exception


def test_get_write_engine_unknown_in_defaults_raises_sql_common_1():
    bogus = "phantom-write-engine"
    defaults = {
        const.WRITE_ENGINE_KEY: bogus,
        const.AVAIL_WRITE_ENGINES_KEY: [bogus],
    }
    with pytest.raises(SqlCommonRuntimeException) as ei:
        factory.get_write_engine(None, defaults)
    assert bogus in str(ei.value)
