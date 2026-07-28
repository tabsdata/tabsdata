#
# Copyright 2026 Tabsdata Inc.
#

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
    """Cross-field validation failure for the BigQuery connector.

    :categories: error
    """


class BigQueryRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the BigQuery connector.

    :categories: error
    """


class BigQueryErrorCode(ErrorCode):
    """Error codes for the BigQuery connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `BigQueryErrorCode.BIGQUERY_N.exception`.
    """

    BIGQUERY_1 = ErrorDef(
        ConnInitException,
        "BigQueryDestConn validation failed",
    )  # BigQueryDestConn
    BIGQUERY_2 = ErrorDef(
        BigQueryValidateException,
        "BigQueryDestConn.dataset={dataset!r} requires project to be set",
    )  # BigQueryDestConn.validate()
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
        ConnInitException,
        "Cannot create GCSLocation",
    )  # GCSLocation
