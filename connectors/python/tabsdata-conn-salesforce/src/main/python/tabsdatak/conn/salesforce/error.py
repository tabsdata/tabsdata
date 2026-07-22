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


class SalesforceValidateException(ConnException):
    """Cross-field validation failure for the Salesforce connector.

    :categories: error
    """


class SalesforceRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Salesforce connector.

    :categories: error
    """


class SalesforceErrorCode(ErrorCode):
    """Error codes for the Salesforce connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `SalesforceErrorCode.SALESFORCE_N.exception`.
    """

    SALESFORCE_1 = ErrorDef(
        ConnInitException,
        "SalesforceTokenCredentials validation failed",
    )  # SalesforceTokenCredentials
    SALESFORCE_2 = ErrorDef(
        ConnInitException,
        "SalesforceSrcConn validation failed",
    )  # SalesforceSrcConn
    SALESFORCE_3 = ErrorDef(
        ConnInitException,
        "SalesforceSrc validation failed",
    )  # SalesforceSrc
    SALESFORCE_4 = ErrorDef(
        ConnInitException,
        "SalesforceReportSrc validation failed",
    )  # SalesforceReportSrc
    SALESFORCE_5 = ErrorDef(
        ConnInitException,
        "Salesforce API name must be 1-80 chars (got {length}): {value!r}",
    )  # _validate_api_name: bad length
    SALESFORCE_6 = ErrorDef(
        ConnInitException,
        "Salesforce instance URL must be an https:// URL on"
        " salesforce.com / force.com: {value!r}",
    )  # _validate_instance_url
    SALESFORCE_7 = ErrorDef(
        ConnInitException,
        "Salesforce API version must match '<major>.<minor>' with 1-3 digit"
        " major and 1-2 digit minor (e.g. '60.0'): {value!r}",
    )  # _validate_api_version
    SALESFORCE_8 = ErrorDef(
        ConnInitException,
        "Salesforce API name must start with a letter, contain only"
        " letters/digits/underscores with no consecutive underscores,"
        " and may end with '__c' or '__r': {value!r}",
    )  # _validate_api_name: bad shape

    # --- Runtime: SalesforceSrc (SOQL) ----------------------------------
    SALESFORCE_9 = ErrorDef(
        SalesforceRuntimeException,
        "Salesforce login failed (instance_url={instance_url!r})",
    )  # _login: simple-salesforce auth / connect failure
    SALESFORCE_10 = ErrorDef(
        SalesforceRuntimeException,
        "Salesforce SOQL query failed (query_index={query_index})",
    )  # _SalesforceSrcPlugin.read_in: SOQL execution failure
    SALESFORCE_11 = ErrorDef(
        SalesforceRuntimeException,
        "Salesforce SOQL query at index {query_index} contains"
        " '$lastModified' token but no initial_last_modified was provided",
    )  # _SalesforceSrcPlugin.read_in: missing initial_last_modified
    SALESFORCE_12 = ErrorDef(
        SalesforceRuntimeException,
        "Unsupported Salesforce credentials type: {kind}",
    )  # _login: bad credentials shape

    # --- Runtime: SalesforceReportSrc -----------------------------------
    SALESFORCE_13 = ErrorDef(
        SalesforceRuntimeException,
        "Salesforce report not found ({find_by}={report!r})",
    )  # _SalesforceReportSrcPlugin: report id resolution
    SALESFORCE_14 = ErrorDef(
        SalesforceRuntimeException,
        "Salesforce report '{report}' is ambiguous: found by both id and"
        " name. Set find_report_by to disambiguate.",
    )  # _SalesforceReportSrcPlugin: ambiguous resolution
    SALESFORCE_15 = ErrorDef(
        SalesforceRuntimeException,
        "Salesforce report '{report}' has format {format!r}; only"
        " 'Tabular' is supported",
    )  # _SalesforceReportSrcPlugin: unsupported report format
    SALESFORCE_16 = ErrorDef(
        SalesforceRuntimeException,
        "Salesforce report execution failed (report_index={report_index})",
    )  # _SalesforceReportSrcPlugin.read_in: report run failure
    SALESFORCE_17 = ErrorDef(
        SalesforceValidateException,
        "SalesforceReportSrc: last_modified_column requires"
        " initial_last_modified (and vice versa)",
    )  # SalesforceReportSrc.validate()
    SALESFORCE_18 = ErrorDef(
        SalesforceValidateException, "Cannot create SalesforceReportingEntity"
    )
