from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class DruidValidateException(ConnException):
    """Cross-field validation failure for the Druid connector."""


class DruidErrorCode(ErrorCode):
    DRUID_1 = ErrorDef(
        ConnInitException,
        "DruidConn validation failed",
    )  # DruidConn
    DRUID_2 = ErrorDef(
        ConnInitException,
        "DruidDest validation failed",
    )  # DruidDest
    DRUID_3 = ErrorDef(
        ConnInitException,
        "Druid datasource must be 1-128 chars (got {length}): {value!r}",
    )  # _validate_datasource: bad length
    DRUID_4 = ErrorDef(
        ConnInitException,
        "Druid broker URL must be http(s)://host[/path]: {value!r}",
    )  # _validate_url
    DRUID_5 = ErrorDef(
        ConnInitException,
        "Druid datasource may not start with '.' or '_': {value!r}",
    )  # _validate_datasource: leading '.' / '_'
    DRUID_6 = ErrorDef(
        ConnInitException,
        "Druid datasource may not contain '/', '\\\\' or null: {value!r}",
    )  # _validate_datasource: forbidden char
