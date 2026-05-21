"""Salesforce connector — public API.

* :class:`SalesforceConn` -- per-org connection (token credentials,
  optional instance URL, optional ``objects`` / ``category`` API
  hints, optional ``entity_whitelist``).
* :class:`SalesforceSrc` -- SOQL source (1 SOQL query per call).
* :class:`SalesforceReportSrc` -- Analytics Report source
  (Tabular reports only).

No ``SalesforceDest`` today. Uses ``simple-salesforce`` (REST +
Bulk API 2.0).
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias, Union

from pydantic import Field

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import Conn, Env, NamedConn, Secret, SecretOrEnvSpec, Src

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import CfgSpec, Credentials, EntityWhitelistSpec

# noinspection PyProtectedMember
from tabsdatak.conn.salesforce import _validation
from tabsdatak.conn.salesforce.error import SalesforceErrorCode

# --- Credentials -----------------------------------------------------


class SalesforceCredentials(Credentials, ABC):
    """Abstract base for Salesforce credential shapes. Today only
    :class:`SalesforceTokenCredentials` (username + password +
    security token) is supported; sibling subclasses can add
    OAuth / JWT auth without changing the connector surface."""


@_api.dataclass(SalesforceErrorCode.SALESFORCE_1, kw_only=True)
class SalesforceTokenCredentials(SalesforceCredentials):
    """Salesforce username + password + security-token credentials,
    the standard SOAP / REST credential triple. All three fields
    accept ``Env`` / ``Secret`` references; the plugin combines
    ``password`` and ``security_token`` per Salesforce's expected
    concatenated-secret format at login time.

    :param username: Salesforce login username
        (e.g. ``"user@example.com"``).
    :param password: Salesforce login password.
    :param security_token: User-specific security token (reset
        from Salesforce setup > Reset My Security Token).
    """

    __kind__ = "salesforceTokenCredentials"
    username: SecretOrEnvSpec
    password: SecretOrEnvSpec
    security_token: SecretOrEnvSpec


# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = SalesforceTokenCredentials
ApiNameSpec: TypeAlias = Annotated[str, _validation.API_NAME_VALIDATOR]
ApiVersionSpec: TypeAlias = Annotated[str, _validation.API_VERSION_VALIDATOR]
ObjectsNameSpec: TypeAlias = Annotated[
    list[ApiNameSpec], _api.NON_EMPTY_LIST_CONSTRAINTS
]

InstanceUrlSpec: TypeAlias = Union[
    Annotated[
        Secret, _api.SecretValueConstraints(pattern=_validation.INSTANCE_URL_PATTERN)
    ],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.INSTANCE_URL_PATTERN)],
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

# Each entry is ``(column, operator, value)`` -- a single condition
# on a Salesforce report. The middle element is constrained by
# :data:`OperatorSpec`.
FilterSpec: TypeAlias = list[tuple[str, OperatorSpec, str]]


# --- SalesforceSrc / SalesforceReportSrc.src_cfg keys ---------------
SrcCfgKey: TypeAlias = Literal["salesforce.logging_level",]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(SalesforceErrorCode.SALESFORCE_2, kw_only=True)
class SalesforceConn(Conn):
    """Salesforce org connection.

    :param credentials: :class:`SalesforceTokenCredentials`
        (username + password + security token).
    :param instance_url: Optional explicit instance URL.
    :param category: Optional category hint for entity
        discovery; mutually exclusive with ``objects``.
    :param objects: Optional explicit list of Salesforce object
        API names. Mutually exclusive with ``category``.
    :param entity_whitelist: Optional regex-based filter.
    :param conn_cfg: Optional driver-side config dict.
    """

    credentials: CredentialsSpec
    instance_url: InstanceUrlSpec | None = None
    category: str | None = None
    objects: ObjectsNameSpec | None = None
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, SalesforceConn]


# --- Source: SOQL queries --------------------------------------------


@_api.dataclass(SalesforceErrorCode.SALESFORCE_3, kw_only=True)
class SalesforceSrc(Src):
    """Salesforce SOQL source — exactly one SOQL query.

    :param conn: :class:`SalesforceConn` (or :class:`NamedConn`).
    :param queries: SOQL query list (today exactly one; the
        list shape leaves room for future multi-query support).
    :param include_deleted: When ``True``, scan deleted rows
        (queryAll).
    :param initial_last_modified: Optional cutoff; substituted
        for ``$lastModified`` tokens in the SOQL string.
    :param api_version: Optional API version override
        (e.g. ``"60.0"``).
    :param src_cfg: Optional ``Mapping`` over
        ``salesforce.logging_level``.
    """

    conn: ConnSpec
    queries: QueriesSpec
    include_deleted: bool = False
    initial_last_modified: datetime | None = None
    api_version: ApiVersionSpec | None = None
    src_cfg: SrcCfgSpec | None = None


# --- Source: reports --------------------------------------------------


@_api.dataclass(SalesforceErrorCode.SALESFORCE_4, kw_only=True)
class SalesforceReportSrc(Src):
    """Salesforce report source — Tabular reports only.

    :param conn: :class:`SalesforceConn` (or :class:`NamedConn`).
    :param reports: Report ids or names; one slot per entry.
    :param column_name_strategy: ``"columnName"`` (default,
        API names) or ``"label"`` (display names).
    :param find_report_by: Lookup strategy for entries in
        ``reports`` that could be interpreted as either a Salesforce
        report id (15- or 18-char alphanumeric) or a report name.
        ``"id"`` forces id lookup, ``"name"`` forces name lookup.
        Leave unset to auto-detect; set explicitly when both forms
        resolve to a hit and you need to break the tie.
    :param filter: Optional list of
        ``(column, operator, value)`` triples applied to the
        report on the server before download.
    :param filter_logic: Optional boolean expression over the
        ``filter`` entries (e.g. ``"(1 AND 2) OR 3"``).
    :param last_modified_column: Optional column for incremental
        last-modified filtering. Requires ``initial_last_modified``.
    :param initial_last_modified: Cutoff datetime. Requires
        ``last_modified_column``.
    :param src_cfg: Optional ``Mapping`` over
        ``salesforce.logging_level``.
    """

    conn: ConnSpec
    reports: ReportsSpec
    column_name_strategy: Literal["columnName", "label"] = "columnName"
    find_report_by: Literal["id", "name"] | None = None
    filter: FilterSpec | None = None
    filter_logic: str | None = None
    last_modified_column: str | None = None
    initial_last_modified: datetime | None = None
    src_cfg: SrcCfgSpec | None = None

    def validate(self) -> None:
        if bool(self.last_modified_column) != bool(self.initial_last_modified):
            # noinspection PyProtectedMember
            raise SalesforceErrorCode.SALESFORCE_17.exception()
