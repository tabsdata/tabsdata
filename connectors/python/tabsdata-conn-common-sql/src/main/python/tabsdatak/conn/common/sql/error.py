from __future__ import annotations

from tabsdatak.conn.common.error import ConnException
from tabsdatak.error import ErrorCode, ErrorDef


class SqlCommonRuntimeException(ConnException):
    """Runtime failure inside the shared SQL machinery (engine
    dispatch, retry exhaustion, ...). Connector plugins translate
    this to their own per-connector codes when relevant."""


class SqlCommonErrorCode(ErrorCode):
    # _engine.factory.get_read_engine / get_write_engine: the
    # connector's defaults map listed an engine name but the factory
    # has no concrete implementation to dispatch to. ``_config.get_cfg``
    # already enforces the value is in the allow-list, so this only
    # fires on a misconfigured defaults dict.
    SQL_COMMON_1 = ErrorDef(
        SqlCommonRuntimeException,
        "Unknown SQL engine {engine!r}; the connector's defaults list "
        "it but the factory has no implementation for it",
    )
