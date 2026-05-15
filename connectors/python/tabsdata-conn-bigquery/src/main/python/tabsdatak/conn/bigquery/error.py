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
    BIGQUERY_4 = ErrorDef(
        ConnInitException,
        "BigQuery project id must be 6-30 chars, lowercase letters / digits"
        " / hyphens, start with a letter, end alphanumeric: {value!r}",
    )  # _validate_project_id
    BIGQUERY_5 = ErrorDef(
        ConnInitException,
        "BigQuery dataset id must be 1-1024 chars, letters / digits"
        " / underscores: {value!r}",
    )  # _validate_dataset_id
    BIGQUERY_6 = ErrorDef(
        ConnInitException,
        "BigQuery table id must be 1-1024 chars, letters / digits"
        " / underscores: {value!r}",
    )  # _validate_table_id
    BIGQUERY_7 = ErrorDef(
        ConnInitException,
        "BigQuery table name parse failed: {value!r}",
    )  # _validate_table_fqn: split_fqn raised
    BIGQUERY_8 = ErrorDef(
        ConnInitException,
        "BigQuery table name must be 'table', 'dataset.table' or"
        " 'project.dataset.table': {value!r}",
    )  # _validate_table_fqn: bad arity
