from __future__ import annotations

from abc import ABC
from datetime import datetime
from typing import Annotated, Literal, TypeAlias, Union

from pydantic import Field

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import Conn, Env, NamedConn, Secret, SecretOrEnvSpec, Src

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import CfgSpec, Credentials, EntityWhitelistSpec

# noinspection PyProtectedMember
from tabsdatak.conn.salesforce import _validation
from tabsdatak.conn.salesforce.error import SalesforceErrorCode

# --- Credentials -----------------------------------------------------


class SalesforceCredentials(Credentials, ABC):
    pass


@_api.dataclass(SalesforceErrorCode.SALESFORCE_1, kw_only=True)
class SalesforceTokenCredentials(SalesforceCredentials):
    __kind__ = "salesforceTokenCredentials"
    username: SecretOrEnvSpec
    password: SecretOrEnvSpec
    security_token: SecretOrEnvSpec


# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = SalesforceCredentials
ApiNameSpec: TypeAlias = Annotated[str, _validation.API_NAME_VALIDATOR]
ApiVersionSpec: TypeAlias = Annotated[str, _validation.API_VERSION_VALIDATOR]


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


# --- Connection ------------------------------------------------------


@_api.dataclass(SalesforceErrorCode.SALESFORCE_2, kw_only=True)
class SalesforceConn(Conn):
    credentials: CredentialsSpec
    instance_url: InstanceUrlSpec | None = None
    category: str | None = None
    objects: ApiNameSpec | list[ApiNameSpec] | None = None
    conn_cfg: CfgSpec | None = None

    def validate(self) -> None:
        if self.category and self.objects:
            # noinspection PyProtectedMember
            raise SalesforceErrorCode.SALESFORCE_3.exception(
                category=self.category,
                objects=self.objects,
            )


ConnSpec: TypeAlias = Union[NamedConn, SalesforceConn]


# --- Source: SOQL queries --------------------------------------------


@_api.dataclass(SalesforceErrorCode.SALESFORCE_4, kw_only=True)
class SalesforceSrc(Src):
    conn: ConnSpec
    queries: QueriesSpec
    include_deleted: bool = False
    initial_last_modified: datetime | None = None
    api_version: ApiVersionSpec | None = None
    entity_whitelist: EntityWhitelistSpec | None = None
    # TODO: document valid keys for this cfg in the docstring above
    # (see MySQLSrc src_cfg for the pattern).
    src_cfg: CfgSpec | None = None


# --- Source: reports --------------------------------------------------


@_api.dataclass(SalesforceErrorCode.SALESFORCE_5, kw_only=True)
class SalesforceReportSrc(Src):
    conn: ConnSpec
    reports: ReportsSpec
    column_name_strategy: Literal["columnName", "label"] = "columnName"
    find_report_by: Literal["id", "name"] | None = None
    filter: FilterSpec | None = None
    filter_logic: str | None = None
    last_modified_column: str | None = None
    initial_last_modified: datetime | None = None
    entity_whitelist: EntityWhitelistSpec | None = None
    # TODO: document valid keys for this cfg in the docstring above
    # (see MySQLSrc src_cfg for the pattern).
    src_cfg: CfgSpec | None = None
