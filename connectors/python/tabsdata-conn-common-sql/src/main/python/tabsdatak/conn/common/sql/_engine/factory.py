from typing import Any, Mapping

# noinspection PyProtectedMember
import tabsdatak.conn.common.sql._engine.constants as const

# noinspection PyProtectedMember
from tabsdatak.conn.common import _config

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.reader import SqlReadEngine

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.sqlalchemy_reader import (
    PandasReadEngine,
    PolarsReadEngine,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.sqlalchemy_writer import PolarsWriteEngine

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.writer import SqlWriteEngine
from tabsdatak.conn.common.sql.error import SqlCommonErrorCode
from tabsdatak.conn.common.types import CfgSpec


def get_read_engine(
    src_cfg: CfgSpec,
    defaults: Mapping[str, Any],
) -> SqlReadEngine:
    engine = _config.get_cfg(
        src_cfg,
        const.READ_ENGINE_KEY,
        defaults[const.READ_ENGINE_KEY],
        defaults[const.AVAIL_READ_ENGINES_KEY],
    )
    if engine == const.PANDAS_SQL_ALCHEMY_READ_ENGINE:
        return PandasReadEngine()
    if engine == const.POLARS_SQL_ALCHEMY_READ_ENGINE:
        return PolarsReadEngine()
    raise SqlCommonErrorCode.SQL_COMMON_1.exception(engine=engine)


def get_write_engine(
    dest_cfg: CfgSpec,
    defaults: Mapping[str, Any],
) -> SqlWriteEngine:
    engine = _config.get_cfg(
        dest_cfg,
        const.WRITE_ENGINE_KEY,
        defaults[const.WRITE_ENGINE_KEY],
        defaults[const.AVAIL_WRITE_ENGINES_KEY],
    )
    if engine == const.POLARS_SQL_ALCHEMY_WRITE_ENGINE:
        return PolarsWriteEngine()
    raise SqlCommonErrorCode.SQL_COMMON_1.exception(engine=engine)
