from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class BigQueryValidateException(ConnException):
    """Cross-field validation failure for the BigQuery connector."""


class BigqueryErrorCode(ErrorCode):
    BIGQUERY_1 = ErrorDef(
        ConnInitException,
        "BigQueryConn validation failed",
    )  # BigQueryConn
    BIGQUERY_2 = ErrorDef(
        BigQueryValidateException,
        "BigQueryConn.dataset={dataset!r} requires project to be set",
    )  # BigQueryConn.validate()
    BIGQUERY_3 = ErrorDef(
        ConnInitException,
        "BigQueryDest validation failed",
    )  # BigQueryDest
