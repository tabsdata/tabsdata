#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


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
