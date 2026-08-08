#
# Copyright 2026 Tabsdata Inc.
#

"""Salesforce connector -- public API.

Imported from `tabsdatak.conn.salesforce`:

* `SalesforceSrcConn` -- the connection:
  `SalesforceTokenCredentials` plus an optional instance URL and
  entity-discovery hints. Registered on a collection, not passed to a
  decorator.
* `SalesforceSrc` -- a `@publisher` SOQL source (one SOQL query).
* `SalesforceReportSrc` -- a `@publisher` Analytics Report source
  (Tabular reports only).
* `SalesforceTokenCredentials` -- username / password /
  security-token credentials set on the connection.

There is no Salesforce destination today; the connector uses
`simple-salesforce` (REST + Bulk API 2.0). The connection (with
credentials) is resolved from the function's collection at run time; a
flow function only references the source.

Examples:
    .. code-block:: python

        @publisher(
            source=SalesforceSrc(queries=["SELECT Id, Name FROM Account"]),
            output_tables=["accounts"],
        )
        def ingest(accounts: TableFrameSpec) -> TableFrameSpec:
            return accounts
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import Field

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import Secret, Src, StrOrSecretSpec

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import (
    CfgSpec,
    Credentials,
    EntityWhitelistSpec,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.salesforce import _validation
from tabsdatak.conn.salesforce.error import SalesforceErrorCode
from tabsdatak.spi import Conn

# --- Credentials -----------------------------------------------------


class SalesforceCredentials(Credentials, ABC):
    """Abstract base for Salesforce credential shapes.

    :categories: credentials

    Today only `SalesforceTokenCredentials` (username + password +
    security token) is supported; sibling subclasses can add OAuth /
    JWT auth without changing the connector surface.
    """


@_api.dataclass(SalesforceErrorCode.SALESFORCE_1, kw_only=True)
class SalesforceTokenCredentials(SalesforceCredentials):
    """Salesforce username / password / security-token credentials.

    :categories: credentials

    The standard SOAP / REST credential triple. Each field accepts a
    plain string or a `Secret`; the plugin combines `password` and
    `security_token` in Salesforce's concatenated-secret format at
    login.

    Args:
        username: Salesforce login username, e.g.
            `"user@example.com"`.
        password: Salesforce login password.
        security_token: User-specific security token (reset from
            Salesforce setup > Reset My Security Token).
    """

    __kind__ = "salesforceTokenCredentials"
    username: Secret
    password: Secret
    security_token: Secret


# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = SalesforceTokenCredentials
ApiNameSpec: TypeAlias = Annotated[str, _validation.API_NAME_VALIDATION]
ApiVersionSpec: TypeAlias = Annotated[str, _validation.API_VERSION_VALIDATION]
ObjectsNameSpec: TypeAlias = Annotated[list[ApiNameSpec], _api.NON_EMPTY_LIST_FIELD]

InstanceUrlSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.INSTANCE_URL_VALIDATION,
]
# Salesforce SOQL query list. Today exactly one query (one input
# slot) -- the list shape leaves room for future multi-query
# support without changing the field name.
QueriesSpec: TypeAlias = Annotated[list[str], Field(min_length=1, max_length=1)]

# Salesforce report ids / names. Non-empty list -- one output slot
# per element.
ReportsSpec: TypeAlias = Annotated[list[str], Field(min_length=1)]

# Salesforce Analytics REST API report-filter operator. Set on the
# middle element of every filter tuple. See
# https://developer.salesforce.com/docs/atlas.en-us.api_analytics.meta/api_analytics/sforce_analytics_rest_api_concepts_filter_operators.htm
OperatorSpec: TypeAlias = Literal[
    "equals",
    "notEqual",
    "lessThan",
    "greaterThan",
    "lessOrEqual",
    "greaterOrEqual",
    "contains",
    "notContain",
    "startsWith",
    "includes",
    "excludes",
    "within",
    "between",
]

# Each entry is `(column, operator, value)` -- a single condition
# on a Salesforce report. The middle element is constrained by
# `OperatorSpec`.
FilterSpec: TypeAlias = list[tuple[str, OperatorSpec, str]]


# --- SalesforceSrc / SalesforceReportSrc.src_cfg keys ---------------
SrcCfgKey: TypeAlias = Literal[
    "salesforce.logging_level",
    "tabsdata.src_metadata.drop",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- Connection ------------------------------------------------------


@_api.dataclass(SalesforceErrorCode.SALESFORCE_2, kw_only=True)
class SalesforceSrcConn(Conn):
    """Salesforce org connection used by `SalesforceSrc` /
    `SalesforceReportSrc`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the source at run time.

    Args:
        credentials: `SalesforceTokenCredentials` (username +
            password + security token).
        instance_url: Optional explicit instance URL; must be an
            `https://` URL on a Salesforce domain (`salesforce.com`
            / `force.com` / `cloudforce.com` /
            `salesforce-setup.com`). Accepts a plain string or a
            `Secret`.
        category: Optional category hint for entity discovery; mutually
            exclusive with `objects`.
        objects: Optional non-empty list of explicit Salesforce object
            API names (each 1-80 chars, starting with a letter, then
            letters / digits / single underscores, optionally ending
            `__c` or `__r`). Mutually exclusive with `category`.
        entity_whitelist: Optional list of regex patterns narrowing
            which objects the source considers.
        conn_cfg: Optional connector config dict (see `CfgSpec`).
    """

    credentials: CredentialsSpec
    instance_url: InstanceUrlSpec | None = None
    category: str | None = None
    objects: ObjectsNameSpec | None = None
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _instance_url(self) -> str:
        return _convert(self.instance_url, str)


# --- Source: SOQL queries --------------------------------------------


@_api.dataclass(SalesforceErrorCode.SALESFORCE_3, kw_only=True)
class SalesforceSrc(Src):
    """Salesforce SOQL source for `@publisher` -- one query.

    :categories: source

    Every table this source publishes automatically carries two
    metadata columns: `@td.salesforce.soql`, the query that produced
    the row as it was sent to Salesforce (any `$lastModified` token
    already substituted), and `@td.salesforce.last_modified`, the
    `initial_last_modified` cutoff it ran with (empty when unset).
    Setting the `src_cfg` key `tabsdata.src_metadata.drop` to `True`
    leaves them off.

    Args:
        queries: SOQL query list; today exactly one entry (one output
            slot). The list shape leaves room for future multi-query
            support.
        include_deleted: When true, also scan deleted rows (queryAll).
        initial_last_modified: Optional cutoff substituted for any
            `$lastModified` token in the SOQL string.
        api_version: Optional API-version override, e.g. `"60.0"`
            (`<major>.<minor>`, 1-3 digit major, 1-2 digit minor).
        src_cfg: Optional connector config over the keys
            `salesforce.logging_level` and
            `tabsdata.src_metadata.drop`.

    Examples:
        Publish an SObject query, including deleted rows (queryAll):

        .. code-block:: python

            @publisher(
                source=SalesforceSrc(
                    queries=["SELECT Id, Name FROM Account"],
                    include_deleted=True,
                ),
                output_tables=["accounts"],
            )
            def ingest(accounts: TableFrameSpec) -> TableFrameSpec:
                return accounts
    """

    queries: QueriesSpec
    include_deleted: bool = False
    initial_last_modified: datetime | None = None
    api_version: ApiVersionSpec | None = None
    src_cfg: SrcCfgSpec | None = None


# --- Source: reports --------------------------------------------------


@_api.dataclass(SalesforceErrorCode.SALESFORCE_4, kw_only=True)
class SalesforceReportSrc(Src):
    """Salesforce report source for `@publisher` -- Tabular reports
    only.

    :categories: source

    Every table this source publishes automatically carries two
    metadata columns: `@td.salesforce.reportId`, the resolved report
    id that produced the row (even when `reports` named the report by
    name), and `@td.salesforce.last_modified`, the source's
    `initial_last_modified` cutoff (empty when unset). Setting the
    `src_cfg` key `tabsdata.src_metadata.drop` to `True` leaves them
    off.

    Args:
        reports: Report ids or names, one output slot per entry
            (non-empty list).
        column_name_strategy: `"columnName"` (default, API names) or
            `"label"` (display names).
        find_report_by: Lookup strategy for a `reports` entry that
            could be either a report id (15- or 18-char alphanumeric)
            or a report name -- `"id"` forces id lookup, `"name"`
            forces name lookup. Leave unset to auto-detect; set it to
            break the tie when both forms resolve.
        filter: Optional list of `(column, operator, value)` triples
            applied on the server before download. `operator` is one
            of `equals`, `notEqual`, `lessThan`, `greaterThan`,
            `lessOrEqual`, `greaterOrEqual`, `contains`,
            `notContain`, `startsWith`, `includes`, `excludes`,
            `within` or `between`.
        filter_logic: Optional boolean expression over the `filter`
            entries, e.g. `"(1 AND 2) OR 3"`.
        last_modified_column: Optional column for incremental
            last-modified filtering; requires `initial_last_modified`
            (and vice versa).
        initial_last_modified: Cutoff datetime; requires
            `last_modified_column`.
        src_cfg: Optional connector config over the keys
            `salesforce.logging_level` and
            `tabsdata.src_metadata.drop`.

    Examples:
        Publish a Tabular report by id, filtered server-side before
        download:

        .. code-block:: python

            @publisher(
                source=SalesforceReportSrc(
                    reports=["00O000000000001"],
                    filter=[("Type", "equals", "Customer")],
                ),
                output_tables=["customers"],
            )
            def ingest(customers: TableFrameSpec) -> TableFrameSpec:
                return customers
    """

    reports: ReportsSpec
    column_name_strategy: Literal["columnName", "label"] = "columnName"
    find_report_by: Literal["id", "name"] | None = None
    filter: FilterSpec | None = None
    filter_logic: str | None = None
    last_modified_column: str | None = None
    initial_last_modified: datetime | None = None
    src_cfg: SrcCfgSpec | None = None

    def validate(self) -> None:
        """Validate the last-modified column and cutoff are set together.

        Called by the framework; raises `SalesforceValidateException`.
        """
        if bool(self.last_modified_column) != bool(self.initial_last_modified):
            # noinspection PyProtectedMember
            raise SalesforceErrorCode.SALESFORCE_17.exception()
