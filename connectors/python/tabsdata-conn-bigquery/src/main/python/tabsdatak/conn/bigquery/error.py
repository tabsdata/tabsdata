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


class BigQueryRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the BigQuery connector."""


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
    BIGQUERY_4 = ErrorDef(
        BigQueryRuntimeException,
        "BigQueryDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _BigQueryDestPlugin: defensive recheck
    BIGQUERY_5 = ErrorDef(
        BigQueryRuntimeException,
        "BigQuery client creation failed (project={project})",
    )  # _BigQueryDestPlugin: client construction failure
    BIGQUERY_6 = ErrorDef(
        BigQueryRuntimeException,
        "BigQuery staging upload to GCS failed (uri={uri})",
    )  # _BigQueryDestPlugin: GCS staging upload failure
    BIGQUERY_7 = ErrorDef(
        BigQueryRuntimeException,
        "BigQuery load job failed (table={table})",
    )  # _BigQueryDestPlugin: load_table_from_uri failure
    BIGQUERY_8 = ErrorDef(
        BigQueryRuntimeException,
        "BigQueryDest table name {table!r} could not be parsed",
    )  # _BigQueryDestPlugin: table FQN parse failure
