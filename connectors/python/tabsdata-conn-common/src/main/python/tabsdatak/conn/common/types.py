"""Cross-connector type shapes shared by every ``tabsdatak.conn.*``
package.

Lives under ``tabsdata-conn-common`` so each connector imports the
same field aliases (``UserPassword``, ``AwsAccessSecretKey``,
``GCPJson``, ``EntityWhitelistSpec``, ``CfgSpec``, etc.) instead
of redefining them per package. ``Secret`` references
are honoured uniformly: a bare string passed to one of these
fields is auto-wrapped as ``Secret(...)`` at field-validation
time.
"""

from __future__ import annotations

from abc import ABC
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Type, TypeAlias, Union

from pydantic import StringConstraints, TypeAdapter

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
import tabsdatak.conn.common._validation as _validation
from tabsdatak.api import Secret
from tabsdatak.conn.common.error import ConnErrorCode

# --- Credentials -----------------------------------------------------


class Credentials(ABC, _api.DescriptorBase):
    __kind__ = "credentials"


# --- User / password -------------------------------------------------


@_api.dataclass(ConnErrorCode.CONN_1, kw_only=True)
class UserPassword(Credentials):
    __kind__ = "userPasswordCredentials"
    user: Secret
    password: Secret


# --- GCP -------------------------------------------------------------


class GCPCredentials(Credentials, ABC):
    pass


@_api.dataclass(ConnErrorCode.CONN_2, kw_only=True)
class GCPJson(GCPCredentials):
    __kind__ = "gcpJsonCredentials"
    json: Secret


@_api.dataclass(ConnErrorCode.CONN_10)
class GCPHmac(GCPCredentials):
    """HMAC access key / secret for GCS's S3-compatible endpoint.

    Used by connectors that read or write GCS via the S3 protocol
    (e.g. ClickHouse's ``s3()`` table function pointed at
    ``https://storage.googleapis.com``).
    """

    __kind__ = "gcpHmacCredentials"
    access_key: Secret
    secret_key: Secret


# --- AWS -------------------------------------------------------------


@_api.dataclass(ConnErrorCode.CONN_3, kw_only=True)
class AwsAccessSecretKey(Credentials):
    __kind__ = "awsAccessSecretKeyCredentials"
    access_key_id: Secret
    secret_access_key: Secret


# --- Azure -----------------------------------------------------------


class AzureCredentials(Credentials, ABC):
    pass


@_api.dataclass(ConnErrorCode.CONN_4, kw_only=True)
class AzureAccountKey(AzureCredentials):
    __kind__ = "azureAccountKeyCredentials"
    account_name: Secret
    account_key: Secret


# --- Data types (placeholder) ----------------------------------------


class DataType(Enum):  # PLACEHOLDER
    BOOL = "bool"
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    BYTES = "bytes"
    DATE = "date"
    TIME = "time"
    DATETIME = "datetime"


# --- Cloud-location field specs --------------------------------------
#
# Each spec is a ``Union[Annotated[str, StringConstraints(<regex>)],
# Secret]``: a plain string literal is validated against the location's
# regex, while a ``Secret`` carries the value opaquely (the regex does
# not apply to secret values).


# --- BasePath -------------------------------------------------------

# ``base_path`` is a directory path rooted at the connector's natural
# root (``/`` for localfile, the bucket / container root for the cloud
# connectors). The stored value is whatever the user provided -- the
# location / Conn classes expose a ``full_path(path)`` method that
# joins ``base_path`` with a relative path and strips leading /
# trailing ``/`` from the result. The empty string represents the
# root. Backslash and NUL are forbidden.
_BASE_PATH_RE_STR = r"[^\\\x00]*"

BasePathSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_BASE_PATH_RE_STR)],
    Secret,
]


def full_path(base_path: BasePathSpec, path: str) -> str:
    """Join ``base_path`` with ``path``, stripping leading and
    trailing ``/`` from each side and from the result. ``base_path``
    may be a ``str`` or a ``Secret`` whose ``.value()``
    resolves to the path string.
    """
    resolved = _convert(base_path, str)
    return (resolved.strip("/") + "/" + path.strip("/")).strip("/")


# --- S3 bucket ------------------------------------------------------

# S3 bucket: 3-63 chars, lowercase letters / digits / hyphens, start
# and end alphanumeric. (Doesn't capture the imperative reserved-prefix
# / reserved-suffix / IPv4 checks the cloud provider enforces server-
# side anyway.)
_S3_BUCKET_RE_STR = r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]"

S3BucketSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_S3_BUCKET_RE_STR)],
    Secret,
]

# --- AWS region ----------------------------------------------------

# AWS region: ``us-east-1`` / ``us-gov-west-1`` style.
_AWS_REGION_RE_STR = r"[a-z]{2}(?:-[a-z]+)+-\d+"

AwsRegionSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_AWS_REGION_RE_STR)],
    Secret,
]

# --- GCS bucket -----------------------------------------------------

# GCS bucket: same length / charset rules as S3 plus underscore.
# Doesn't capture the imperative ``goog`` prefix / Google-misspelling
# reserved-substring checks.
_GCS_BUCKET_RE_STR = r"[a-z0-9][a-z0-9_-]{1,61}[a-z0-9]"

GCSBucketSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_GCS_BUCKET_RE_STR)],
    Secret,
]


# --- Azure Blob container ------------------------------------------

# Azure container: lowercase alphanumeric / single hyphens (no
# leading, trailing, or consecutive hyphens), 3-63 chars. Encoded
# without lookahead (pydantic_core's regex engine doesn't support it):
# every char after the first is either alphanumeric, or a single hyphen
# *followed by* an alphanumeric -- so consecutive / trailing hyphens
# never match.
_AZ_CONTAINER_RE_STR = r"[a-z0-9](?:-?[a-z0-9]){2,62}"

AzContainerSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_AZ_CONTAINER_RE_STR)],
    Secret,
]


# --- Connection cfg --------------------------------------------------

# Type for ``conn_cfg`` fields on every connector ``Conn`` class.
#
# A ``conn_cfg`` is a free-form dict whose keys carry routing
# information so the connector implementation can forward
# 3rd-party-specific configuration to the right downstream library.
#
# Convention -- key prefixes:
#   Keys whose value targets a specific 3rd-party dependency must be
#   prefixed with the dependency's package name followed by ``.``.
#   Example: ``"sqlalchemy.connect_args"`` carries options for
#   SQLAlchemy; ``"mysql.charset"`` carries options for the MySQL
#   client. The connector implementation strips the prefix before
#   forwarding -- so the SQLAlchemy code receives ``connect_args``
#   and the MySQL client receives ``charset``.
#
# Convention -- value shapes:
#   Values must be basic Python types or compositions thereof:
#   ``str`` / ``int`` / ``float`` / ``bool`` / ``None``, plus
#   ``dict`` (str-keyed) and ``list``. Anything else is rejected at
#   construction time. Dict values are validated recursively.
CfgSpec: TypeAlias = Annotated[dict, _validation.CONN_CFG_VALIDATOR]


# --- entity_whitelist ------------------------------------------------
#
# Per-source list of regex patterns the runtime applies to the
# external system's metadata names (tables / collections / objects /
# datasets / ...) to narrow the set of entities considered by the
# source. ``None`` (the default) means "no filtering -- consider every
# entity". Each entry must be a Python-compatible regex; an invalid
# pattern raises a ``ValueError`` at construction time, which the
# enclosing dataclass tag wraps as the appropriate connector exception.
EntityWhitelistSpec: TypeAlias = Annotated[
    list[str], _validation.ENTITY_WHITELIST_VALIDATOR
]


_CoercionTypeSpec: TypeAlias = Union[int, float, str, bool, Decimal, bytes, datetime]


def _convert[T: _CoercionTypeSpec](spec: T | Secret, type_: Type[T]) -> T:
    """Return the T form of a ``T`` / ``Secret`` spec."""
    if isinstance(spec, type_):
        return spec
    else:
        # Pydantic coercion to
        return TypeAdapter(type_).validate_python(spec.value())
