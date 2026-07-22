#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.error import ConnException
from tabsdatak.error import ErrorCode, ErrorDef


class SqlCommonRuntimeException(ConnException):
    """Runtime failure inside the shared SQL machinery.

    :categories: error

    Raised on engine dispatch, retry exhaustion and similar failures in
    the code shared by every SQL connector. Connector plugins translate
    it to their own per-connector codes when relevant.
    """


class SqlCommonErrorCode(ErrorCode):
    """Error codes for the shared SQL machinery.

    :categories: error

    One variant per failure site. Raise via
    `SqlCommonErrorCode.SQL_COMMON_N.exception`.
    """

    # _engine.factory.get_read_engine / get_write_engine: the
    # connector's defaults map listed an engine name but the factory
    # has no concrete implementation to dispatch to. `_config.get_cfg`
    # already enforces the value is in the allow-list, so this only
    # fires on a misconfigured defaults dict.
    SQL_COMMON_1 = ErrorDef(
        SqlCommonRuntimeException,
        "Unknown SQL engine {engine!r}; the connector's defaults list "
        "it but the factory has no implementation for it",
    )
    SQL_COMMON_2 = ErrorDef(SqlCommonRuntimeException, "Cannot create SQLEntity")
    SQL_COMMON_3 = ErrorDef(SqlCommonRuntimeException, "Cannot create SQLConnMetaModel")
