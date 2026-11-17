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


class AwsGlueRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the AWS Glue connector.

    :categories: error
    """


class AwsGlueErrorCode(ErrorCode):
    """Error codes for the AWS Glue connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `AwsGlueErrorCode.AWSGLUE_N.exception`.
    """

    AWSGLUE_1 = ErrorDef(
        ConnInitException,
        "AwsGlueDestConn validation failed",
    )  # AwsGlueDestConn
    AWSGLUE_2 = ErrorDef(
        ConnInitException,
        "AwsGlueDest validation failed",
    )  # AwsGlueDest
    AWSGLUE_3 = ErrorDef(
        ConnInitException,
        "not a valid AWS Glue database name (1-255 chars, lowercase letters, "
        "digits and underscores): {value!r}",
    )  # DatabaseSpec
    AWSGLUE_4 = ErrorDef(
        ConnInitException,
        "not a valid AWS Glue table name (`table` or `database.table`, each part "
        "1-255 chars, lowercase letters, digits and underscores): {value!r}",
    )  # AwsGlueTableNameSpec
    AWSGLUE_5 = ErrorDef(
        ConnInitException,
        "not a valid AWS account id (12 digits): {value!r}",
    )  # CatalogIdSpec
    AWSGLUE_6 = ErrorDef(
        AwsGlueRuntimeException,
        "AwsGlueDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # AwsGlueDestPlugin: defensive recheck
    AWSGLUE_7 = ErrorDef(
        AwsGlueRuntimeException,
        "AWS Glue credential resolution failed: {reason}",
    )  # _impl: Secret-backed credentials without a resolver
    AWSGLUE_8 = ErrorDef(
        AwsGlueRuntimeException,
        "AwsGlueDest table {table!r} names no database and the connection has no "
        "default database",
    )  # _impl.identifier
    AWSGLUE_9 = ErrorDef(
        AwsGlueRuntimeException,
        "AWS Glue database {database!r} does not exist; AwsGlueDest creates "
        "tables, not databases",
    )  # _impl.target: table creation
    AWSGLUE_10 = ErrorDef(
        AwsGlueRuntimeException,
        "AWS Glue table {table} is not an Iceberg table",
    )  # _impl.target
    AWSGLUE_11 = ErrorDef(
        AwsGlueRuntimeException,
        "AWS Glue table {table} is partitioned; AwsGlueDest writes "
        "unpartitioned tables only",
    )  # _impl.write_table
    AWSGLUE_12 = ErrorDef(
        AwsGlueRuntimeException,
        "column {column!r} has type {dtype}, which an Iceberg table cannot hold; "
        "cast it in the function",
    )  # _impl.iceberg_frame
    AWSGLUE_13 = ErrorDef(
        AwsGlueRuntimeException,
        "AWS Glue table {table} requires columns the batch lacks: {columns}",
    )  # _impl.evolution
    AWSGLUE_14 = ErrorDef(
        AwsGlueRuntimeException,
        "AWS Glue table {table} is located at {location}, which is not in S3",
    )  # _impl.data_target
    AWSGLUE_15 = ErrorDef(
        AwsGlueRuntimeException,
        "AWS Glue write failed (table={table})",
    )  # AwsGlueDestPlugin: any other failure
