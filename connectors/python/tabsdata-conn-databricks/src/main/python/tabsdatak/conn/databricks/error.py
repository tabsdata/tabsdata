from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class DatabricksValidateException(ConnException):
    """Cross-field validation failure for the Databricks connector."""


class DatabricksErrorCode(ErrorCode):
    DATABRICKS_1 = ErrorDef(
        ConnInitException,
        "DatabricksConn validation failed",
    )  # DatabricksConn
    DATABRICKS_2 = ErrorDef(
        DatabricksValidateException,
        "Databricks{cls}: {rule}",
    )  # DatabricksConn.validate()
    DATABRICKS_3 = ErrorDef(
        ConnInitException,
        "DatabricksSrc validation failed",
    )  # DatabricksSrc
    DATABRICKS_4 = ErrorDef(
        ConnInitException,
        "DatabricksDest validation failed",
    )  # DatabricksDest
    DATABRICKS_5 = ErrorDef(
        ConnInitException,
        "Databricks identifier must be non-empty",
    )  # _validate_identifier: empty
    DATABRICKS_6 = ErrorDef(
        ConnInitException,
        "Databricks qualified name has unterminated backtick-quoted segment: {value!r}",
    )  # _split_qualified: unterminated backtick
    DATABRICKS_7 = ErrorDef(
        ConnInitException,
        "Databricks warehouse name must be 1-{max} chars (got {length}): {value!r}",
    )  # _validate_warehouse_name: bad length
    DATABRICKS_8 = ErrorDef(
        ConnInitException,
        "Databricks warehouse id must be a 16-char lowercase hex string: {value!r}",
    )  # _validate_warehouse_id
    DATABRICKS_9 = ErrorDef(
        ConnInitException,
        "Databricks identifier exceeds {max} chars: {value!r}",
    )  # _validate_identifier: too long
    DATABRICKS_10 = ErrorDef(
        ConnInitException,
        "backtick-quoted Databricks identifier may not contain backtick,"
        " period, slash, control chars, or leading/trailing whitespace: {value!r}",
    )  # _validate_identifier: backtick bad
    DATABRICKS_11 = ErrorDef(
        ConnInitException,
        "unquoted Databricks identifier must match [A-Za-z_][A-Za-z0-9_]*: {value!r}",
    )  # _validate_identifier: unquoted bad shape
    DATABRICKS_12 = ErrorDef(
        ConnInitException,
        "Databricks qualified name must be non-empty",
    )  # _validate_qualified_name: empty
    DATABRICKS_13 = ErrorDef(
        ConnInitException,
        "Databricks qualified name must be 1-3 dot-separated parts"
        " ([[<catalog>.]<schema>.]<name>): {value!r}",
    )  # _validate_qualified_name: bad arity
    DATABRICKS_14 = ErrorDef(
        ConnInitException,
        "Databricks warehouse name has leading/trailing whitespace: {value!r}",
    )  # _validate_warehouse_name: whitespace
    DATABRICKS_15 = ErrorDef(
        ConnInitException,
        "Databricks warehouse name contains control char: {value!r}",
    )  # _validate_warehouse_name: control char
