from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class SalesforceValidateException(ConnException):
    """Cross-field validation failure for the Salesforce connector."""


class SalesforceErrorCode(ErrorCode):
    SALESFORCE_1 = ErrorDef(
        ConnInitException,
        "SalesforceTokenCredentials validation failed",
    )  # SalesforceTokenCredentials
    SALESFORCE_2 = ErrorDef(
        ConnInitException,
        "SalesforceConn validation failed",
    )  # SalesforceConn
    SALESFORCE_3 = ErrorDef(
        SalesforceValidateException,
        "SalesforceConn: category={category!r} and objects={objects!r} are"
        " mutually exclusive",
    )  # SalesforceConn.validate()
    SALESFORCE_4 = ErrorDef(
        ConnInitException,
        "SalesforceSrc validation failed",
    )  # SalesforceSrc
    SALESFORCE_5 = ErrorDef(
        ConnInitException,
        "SalesforceReportSrc validation failed",
    )  # SalesforceReportSrc
    SALESFORCE_6 = ErrorDef(
        ConnInitException,
        "Salesforce API name must be 1-80 chars (got {length}): {value!r}",
    )  # _validate_api_name: bad length
    SALESFORCE_7 = ErrorDef(
        ConnInitException,
        "Salesforce instance URL must be an https:// URL on"
        " salesforce.com / force.com: {value!r}",
    )  # _validate_instance_url
    SALESFORCE_8 = ErrorDef(
        ConnInitException,
        "Salesforce API version must match '<major>.<minor>' with 1-3 digit"
        " major and 1-2 digit minor (e.g. '60.0'): {value!r}",
    )  # _validate_api_version
    SALESFORCE_9 = ErrorDef(
        ConnInitException,
        "Salesforce API name must start with a letter, contain only"
        " letters/digits/underscores with no consecutive underscores,"
        " and may end with '__c' or '__r': {value!r}",
    )  # _validate_api_name: bad shape
