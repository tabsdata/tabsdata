#
# Copyright 2026 Tabsdata Inc.
#

"""Cross-connector type shapes shared by every `tabsdatak.conn.*`
package.

Lives under `tabsdata-conn-common` so each connector imports the
same field aliases (`UserPassword`, `AwsAccessSecretKey`,
`GCPJson`, `EntityWhitelistSpec`, `CfgSpec`, etc.) instead
of redefining them per package. `Secret` references
are honoured uniformly: a bare string passed to one of these
fields is auto-wrapped as `Secret(...)` at field-validation
time.
"""

from __future__ import annotations

import re
from abc import ABC
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, Type, TypeAlias, Union

from pydantic import GetCoreSchemaHandler, TypeAdapter
from pydantic_core import core_schema

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
import tabsdatak.conn.common._validation as _validation
from tabsdatak.api import Secret, StrOrSecretSpec
from tabsdatak.conn.common.error import ConnErrorCode

# --- Credentials -----------------------------------------------------


class Credentials(ABC, _api.DescriptorBase):
    """Abstract base for every credential type.

    :categories: credentials

    Concrete credentials are set on a connection's `credentials`
    field. Each field is a `Secret`; a bare string is auto-wrapped as
    `Secret(...)` at validation time.
    """

    __kind__ = "credentials"


# --- User / password -------------------------------------------------


@_api.dataclass(ConnErrorCode.CONN_1, kw_only=True)
class UserPassword(Credentials):
    """Username / password credentials.

    :categories: credentials

    Args:
        user: The username.
        password: The password.
    """

    __kind__ = "userPasswordCredentials"
    user: Secret
    password: Secret


# --- GCP -------------------------------------------------------------


class GCPCredentials(Credentials, ABC):
    """Abstract base for Google Cloud credential types.

    :categories: credentials
    """

    pass


@_api.dataclass(ConnErrorCode.CONN_2, kw_only=True)
class GCPJson(GCPCredentials):
    """GCP service-account JSON key credentials.

    :categories: credentials

    Args:
        json: The service-account key as a JSON document.
    """

    __kind__ = "gcpJsonCredentials"
    json: Secret


@_api.dataclass(ConnErrorCode.CONN_10)
class GCPHmac(GCPCredentials):
    """HMAC access key / secret for GCS's S3-compatible endpoint.

    :categories: credentials

    Used by connectors that read or write GCS via the S3 protocol (e.g.
    ClickHouse's `s3()` table function pointed at
    `https://storage.googleapis.com`).

    Args:
        access_key: The HMAC access key.
        secret_key: The HMAC secret.
    """

    __kind__ = "gcpHmacCredentials"
    access_key: Secret
    secret_key: Secret


# --- AWS -------------------------------------------------------------


@_api.dataclass(ConnErrorCode.CONN_3, kw_only=True)
class AwsAccessSecretKey(Credentials):
    """AWS access key ID / secret access key credentials.

    :categories: credentials

    Used by the S3 connector (`S3SrcConn` / `S3DestConn`).

    Args:
        access_key_id: The AWS access key ID.
        secret_access_key: The AWS secret access key.
    """

    __kind__ = "awsAccessSecretKeyCredentials"
    access_key_id: Secret
    secret_access_key: Secret


# --- Azure -----------------------------------------------------------


class AzureCredentials(Credentials, ABC):
    """Abstract base for Azure credential types.

    :categories: credentials
    """

    pass


@_api.dataclass(ConnErrorCode.CONN_4, kw_only=True)
class AzureAccountKey(AzureCredentials):
    """Azure storage account name / key credentials.

    :categories: credentials

    Args:
        account_name: The Azure storage account name.
        account_key: The Azure storage account key.
    """

    __kind__ = "azureAccountKeyCredentials"
    account_name: Secret
    account_key: Secret


# --- Data types (placeholder) ----------------------------------------


class DataType(Enum):  # PLACEHOLDER
    """Placeholder enumeration of basic data types.

    Not yet part of the stable public surface; subject to change.
    """

    BOOL = "bool"
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    BYTES = "bytes"
    DATE = "date"
    TIME = "time"
    DATETIME = "datetime"


# --- str-or-secret regex constraint ----------------------------------


class StrOrSecretConstraints:
    """Field-site regex gating both arms of a `StrOrSecretSpec`.

    :categories: credentials

    Applied as
    `Annotated[StrOrSecretSpec, StrOrSecretConstraints(pattern=<re>)]`.
    The pattern is enforced as a full match on a plain-string value and
    on a `Secret`'s hydrated value -- at construction and again
    whenever the runtime re-hydrates the value. A dehydrated `Secret`
    carries no value and is left unchecked. One `pattern` covers both
    arms.

    Args:
        pattern: The regular expression each arm's value must fully
            match.
    """

    def __init__(self, *, pattern: str):
        if not isinstance(pattern, str):
            raise TypeError(
                f"StrOrSecretConstraints expects a regex string, got {type(pattern)!r}"
            )
        self.pattern = pattern

    def __repr__(self) -> str:
        return f"StrOrSecretConstraints(pattern={self.pattern!r})"

    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        pattern = self.pattern
        base = handler(source_type)

        def check(value: Any) -> Any:
            if isinstance(value, Secret):
                resolved = value._value
            elif isinstance(value, str):
                resolved = value
            else:
                return value
            if resolved is not None and re.fullmatch(pattern, resolved) is None:
                raise ValueError(
                    f"value {resolved!r} does not match pattern {pattern!r}"
                )
            return value

        return core_schema.no_info_after_validator_function(check, base)


# --- Cloud-location field specs --------------------------------------
#
# Each spec is a `Annotated[StrOrSecretSpec,
# StrOrSecretConstraints(<regex>)]`. The same location regex is enforced
# on a plain-string value (spelled `str:<value>` on the wire) and on a
# `Secret`'s hydrated value (at construction and whenever the runtime
# re-hydrates the secret). A dehydrated `Secret` carries no value and
# is left unchecked.


# --- FullPath -------------------------------------------------------

# `base_path` is always spelled as a full, absolute-looking path: it
# starts with `/`, never ends with one (other than the bare root `/`
# itself), and has no empty segments (no `//`). This gives one
# consistent shape across every connector -- local filesystem and cloud
# object store alike. For the local-file connector the leading `/` is
# the real absolute path. For the cloud connectors it is a UX
# convention: the value denotes a "folder" (object-key prefix) from the
# container / bucket root, and the leading `/` is trimmed when composing
# the actual object key (`full_path` / `normalize_dir_path` do the
# trimming). Backslash and NUL are forbidden. pydantic_core's regex
# engine has no lookaround, so the rule is spelled out structurally: a
# leading `/` followed by zero or more `/`-separated segments, each a run
# of one or more chars that are not `/`, `\` or NUL; the bare `/` root is
# allowed. Defaults to `/`.
_FULL_PATH_RE_STR = r"^/(?:[^/\\\x00]+(?:/[^/\\\x00]+)*)?$"
FullPathSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_FULL_PATH_RE_STR)
]
"""A full path (object-key prefix) from the container/bucket root.

Starts with `/`, has no trailing `/` (except the bare root `/`) and no
empty segments (no `//`); backslash and NUL are forbidden. Accepts a
plain string or a `Secret`.
"""


def full_path(base_path: FullPathSpec, path: str) -> str:
    """Join `base_path` with `path`, stripping leading and
    trailing `/` from each side and from the result. `base_path`
    may be a `str` or a `Secret` whose `.value()`
    resolves to the path string.
    """
    resolved = _convert(base_path, str)
    return (resolved.strip("/") + "/" + path.strip("/")).strip("/")


def normalize_dir_path(path: str, lead_separator: bool, back_separator: bool) -> str:
    """Render `path` as a directory-style prefix with a leading and/or
    trailing `/` forced on per the flags -- for an external driver that
    expects a specific spelling of the prefix. `path` is first reduced
    to its slash-stripped core; an empty core yields `""` regardless of
    the flags (there is no directory to delimit).
    """
    core = path.strip("/")
    if not core:
        return ""
    lead = "/" if lead_separator else ""
    back = "/" if back_separator else ""
    return f"{lead}{core}{back}"


# --- S3 bucket ------------------------------------------------------

# S3 bucket: 3-63 chars, lowercase letters / digits / hyphens, start
# and end alphanumeric. (Doesn't capture the imperative reserved-prefix
# / reserved-suffix / IPv4 checks the cloud provider enforces server-
# side anyway.)
_S3_BUCKET_RE_STR = r"^(?:[a-z0-9][a-z0-9-]{1,61}[a-z0-9])$"
S3BucketSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_S3_BUCKET_RE_STR)
]
"""A valid S3 bucket name: 3-63 characters, lowercase letters, digits
and hyphens, starting and ending with a letter or digit. Accepts a
plain string or a `Secret`."""

# --- AWS region ----------------------------------------------------

# AWS region: `us-east-1` / `us-gov-west-1` style.
_AWS_REGION_RE_STR = r"^(?:[a-z]{2}(?:-[a-z]+)+-\d+)$"
AwsRegionSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_AWS_REGION_RE_STR)
]
"""A valid AWS region name, e.g. `us-east-1` or `us-gov-west-1`.
Accepts a plain string or a `Secret`."""

# --- GCS bucket -----------------------------------------------------

# GCS bucket: same length / charset rules as S3 plus underscore.
# Doesn't capture the imperative `goog` prefix / Google-misspelling
# reserved-substring checks.
_GCS_BUCKET_RE_STR = r"^(?:[a-z0-9][a-z0-9_-]{1,61}[a-z0-9])$"
GCSBucketSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_GCS_BUCKET_RE_STR)
]
"""A valid GCS bucket name: 3-63 characters, lowercase letters, digits,
hyphens and underscores, starting and ending with a letter or digit.
Accepts a plain string or a `Secret`."""


# --- Azure Blob container ------------------------------------------

# Azure container: lowercase alphanumeric / single hyphens (no
# leading, trailing, or consecutive hyphens), 3-63 chars. Encoded
# without lookahead (pydantic_core's regex engine doesn't support it):
# every char after the first is either alphanumeric, or a single hyphen
# *followed by* an alphanumeric -- so consecutive / trailing hyphens
# never match.
_AZ_CONTAINER_RE_STR = r"^(?:[a-z0-9](?:-?[a-z0-9]){2,62})$"
AzContainerSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_AZ_CONTAINER_RE_STR)
]
"""A valid Azure Blob container name: 3-63 characters, lowercase
letters and digits with single (non-leading, non-trailing) hyphens.
Accepts a plain string or a `Secret`."""


# --- Host --------------------------------------------------------------

# A DNS hostname or IPv4 literal: dot-separated labels of alphanumerics with
# internal (not leading / trailing) hyphens. Covers `localhost`,
# `db.example.com`, `10.0.0.1`. Read the value with `_convert(x, str)`.
_HOST_RE_STR = (
    r"^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?"
    r"(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*$"
)
HostSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_HOST_RE_STR)
]
"""A DNS host name or IPv4 literal (e.g. `localhost`,
`db.example.com`, `10.0.0.1`). Accepts a plain string or a
`Secret`."""


# --- Port --------------------------------------------------------------

# A TCP port, 1-65535 (spelled out per digit since the value never carries a
# type on the wire -- it is a `str` or a secret). Read the parsed int with
# `_convert(x, int)`.
_PORT_RE_STR = (
    r"^([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}"
    r"|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$"
)
PortSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_PORT_RE_STR)
]
"""A TCP port number, 1-65535. Accepts a plain string or a `Secret`;
read the parsed int with `_convert(x, int)`."""


# --- Bool --------------------------------------------------------------

# A boolean flag as text (the value is a `str` or a secret on the wire).
# Accepts the tokens pydantic's bool coercion understands, case-insensitively.
# Read the parsed bool with `_convert(x, bool)`.
_BOOL_RE_STR = r"(?i)^(?:true|false|t|f|yes|no|y|n|on|off|1|0)$"
BoolSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_BOOL_RE_STR)
]
"""A boolean flag as text: one of `true`/`false`, `t`/`f`,
`yes`/`no`, `y`/`n`, `on`/`off`, `1`/`0`
(case-insensitive). Accepts a plain string or a `Secret`; read the
parsed bool with `_convert(x, bool)`."""


# --- Connection cfg --------------------------------------------------

# Type for `conn_cfg` fields on every connector `Conn` class.
#
# A `conn_cfg` is a free-form dict whose keys carry routing
# information so the connector implementation can forward
# 3rd-party-specific configuration to the right downstream library.
#
# Convention -- key prefixes:
#   Keys whose value targets a specific 3rd-party dependency must be
#   prefixed with the dependency's package name followed by `.`.
#   Example: `"sqlalchemy.connect_args"` carries options for
#   SQLAlchemy; `"mysql.charset"` carries options for the MySQL
#   client. The connector implementation strips the prefix before
#   forwarding -- so the SQLAlchemy code receives `connect_args`
#   and the MySQL client receives `charset`.
#
# Convention -- value shapes:
#   Values must be basic Python types or compositions thereof:
#   `str` / `int` / `float` / `bool` / `None`, plus
#   `dict` (str-keyed) and `list`. Anything else is rejected at
#   construction time. Dict values are validated recursively.
CfgSpec: TypeAlias = Annotated[dict, _validation.CONN_CFG_VALIDATOR]
"""Type for a connector `conn_cfg` field: a free-form dict forwarding
3rd-party-specific configuration.

Keys targeting a dependency are prefixed with its package name plus
`.` (e.g. `"sqlalchemy.connect_args"`, `"mysql.charset"`); the
connector strips the prefix before forwarding. Values must be
JSON-like: `str` / `int` / `float` / `bool` / `None`, or a
(recursively validated) str-keyed `dict` or `list`.
"""


# --- entity_whitelist ------------------------------------------------
#
# Per-source list of regex patterns the runtime applies to the
# external system's metadata names (tables / collections / objects /
# datasets / ...) to narrow the set of entities considered by the
# source. `None` (the default) means "no filtering -- consider every
# entity". Each entry must be a Python-compatible regex; an invalid
# pattern raises a `ValueError` at construction time, which the
# enclosing dataclass tag wraps as the appropriate connector exception.
EntityWhitelistSpec: TypeAlias = Annotated[
    list[str], _validation.ENTITY_WHITELIST_VALIDATOR
]
"""Optional list of regex patterns narrowing which of the external
system's entities (tables / collections / objects / datasets / ...) a
source considers.

`None` (the default) means no filtering. Each entry must be a valid
Python regular expression, matched against entity metadata names.
"""


_CoercionTypeSpec: TypeAlias = Union[int, float, str, bool, Decimal, bytes, datetime]


def _convert[T: _CoercionTypeSpec](spec: T | str | Secret, type_: Type[T]) -> T:
    """Resolve a string-or-secret `spec` to a value of `type_`.

    A `StrOrSecretSpec` resolves to a `str` (the plain
    arm verbatim, or a secret's hydrated value); that text is then coerced to
    `type_` -- `str` passes through, `int` / `bool` / ... are parsed by
    pydantic. An already-typed value (e.g. an unvalidated field default) is
    returned as-is.
    """
    raw = spec.value() if isinstance(spec, Secret) else spec
    if isinstance(raw, type_):
        return raw
    return TypeAdapter(type_).validate_python(raw)
