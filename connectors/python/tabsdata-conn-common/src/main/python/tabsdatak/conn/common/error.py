#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)


class ConnException(TabsdataException):
    """Top-level base exception for the connector hierarchy.

    :categories: error

    Each connector package defines its own subclasses for the
    categories it needs (init / validate / runtime).
    """

    pass


class ConnInitException(ConnException):
    """A connector-layer object could not be created (attribute type /
    value issue at construction time).

    :categories: error
    """

    pass


class ConnStreamException(ConnException):
    """A stream source broke a contract of the shared stream machinery
    while it ran.

    :categories: error
    """

    pass


class ConnErrorCode(ErrorCode):
    """Error codes shared across the connector-common layer.

    :categories: error

    One variant per validation site (credentials, cloud locations,
    explorer/metadata). Raise via `ConnErrorCode.CONN_N.exception`.
    """

    # --- Credentials (tabsdatak.conn.common.typing_) ----------------
    CONN_1 = ErrorDef(ConnInitException, "Cannot create UserPassword")  # UserPassword
    CONN_2 = ErrorDef(ConnInitException, "Cannot create GCPJson")  # GCPJson
    CONN_3 = ErrorDef(
        ConnInitException, "Cannot create AwsAccessSecretKey"
    )  # AwsAccessSecretKey
    CONN_4 = ErrorDef(
        ConnInitException, "Cannot create AzureAccountKey"
    )  # AzureAccountKey

    # --- Misc -------------------------------------------------------
    # CONN_5 / CONN_6 / CONN_7 (S3Location / AzBlobLocation / GCSLocation)
    # were moved into the per-connector packages along with the
    # CloudLocation hierarchy; the numbering gap is intentional --
    # callers that still imported the conn-common variants now see
    # AttributeError rather than silently picking up an unrelated code.
    CONN_8 = ErrorDef(
        ConnInitException,
        "Configuration '{name}', invalid value '{value}'. Valid values are: {values}",
    )  # GCSLocation

    # --- FQN syntax (tabsdatak.conn.common._validation) -------------
    CONN_9 = ErrorDef(ConnInitException, "Cannot create FqnDef")  # FqnDef

    # --- Credentials (later additions) ------------------------------
    CONN_10 = ErrorDef(ConnInitException, "Cannot create GCPHmac")  # GCPHmac

    # --- Explorer / metadata (tabsdatak.conn.common._explorer) -----------
    CONN_11 = ErrorDef(ConnInitException, "Cannot create Column")

    # --- Entity (tabsdatak.conn.common._explorer) -----------
    CONN_12 = ErrorDef(ConnInitException, "Cannot create ReportingEntity")
    CONN_13 = ErrorDef(ConnInitException, "Cannot create ObjectsEntity")

    # --- ReportingConnection Meta Model (tabsdatak.conn.common._explorer) -----------
    CONN_14 = ErrorDef(ConnInitException, "Cannot create ReportingConnMetaModel")
    CONN_15 = ErrorDef(ConnInitException, "Cannot create ObjectsConnMetaModel")

    # --- Property (tabsdatak.conn.common._explorer) -----------
    CONN_16 = ErrorDef(ConnInitException, "Cannot create Property")

    # --- Field-value rules (tabsdatak.conn.common.types) ------------
    # One code per shared field spec, raised by the spec's
    # `ValueConstraints` rules. The message states the rule the value
    # broke, never the regex that enforced it.
    CONN_17 = ErrorDef(
        ConnInitException,
        "The value is not a cloud object-key prefix (it must start with '/', "
        "have no trailing '/' except the bare root, and no empty segments, "
        "backslash or NUL): {value!r}",
    )  # CloudRelativePathSpec
    CONN_18 = ErrorDef(
        ConnInitException,
        "The value is not an absolute local path (POSIX '/a/b' or Windows "
        "'C:/a/b'; UNC and relative paths are rejected, as are empty segments "
        "and NUL): {value!r}",
    )  # LocalAbsolutePathSpec
    CONN_19 = ErrorDef(
        ConnInitException,
        "The value is not a valid S3 bucket name (3-63 characters of lowercase "
        "letters, digits and hyphens, starting and ending alphanumeric): "
        "{value!r}",
    )  # S3BucketSpec
    CONN_20 = ErrorDef(
        ConnInitException,
        "The value is not a valid AWS region name (for example 'us-east-1' or "
        "'us-gov-west-1'): {value!r}",
    )  # AwsRegionSpec
    CONN_21 = ErrorDef(
        ConnInitException,
        "The value is not a valid GCS bucket name (3-63 characters of lowercase "
        "letters, digits, hyphens and underscores, starting and ending "
        "alphanumeric): {value!r}",
    )  # GCSBucketSpec
    CONN_22 = ErrorDef(
        ConnInitException,
        "The value is not a valid Azure Blob container name (3-63 characters of "
        "lowercase letters and digits, with single hyphens that neither lead nor "
        "trail): {value!r}",
    )  # AzContainerSpec
    CONN_23 = ErrorDef(
        ConnInitException,
        "The value is not a DNS host name or an IPv4 literal (for example "
        "'localhost', 'db.example.com' or '10.0.0.1'): {value!r}",
    )  # HostSpec
    CONN_24 = ErrorDef(
        ConnInitException,
        "The value is not a TCP port number (1-65535, written without leading "
        "zeros, sign or spaces): {value!r}",
    )  # PortSpec
    CONN_25 = ErrorDef(
        ConnInitException,
        "The value is not a boolean flag (one of true/false, t/f, yes/no, y/n, "
        "on/off or 1/0, in any case): {value!r}",
    )  # BoolSpec
    CONN_26 = ErrorDef(
        ConnInitException, "The FqnDef separator must be non-empty"
    )  # SeparatorSpec
    CONN_27 = ErrorDef(
        ConnInitException, "The FqnDef quote, when set, must be non-empty"
    )  # QuoteSpec
    CONN_28 = ErrorDef(
        ConnInitException,
        "The entity_whitelist entry at index {index} is not a valid regular "
        "expression: {value!r}: {reason}",
    )  # EntityWhitelistSpec
    CONN_29 = ErrorDef(
        ConnInitException,
        "The name has {parts} parts, which exceeds the maximum of {max_parts}: "
        "{value!r}",
    )  # name_validate -- too many FQN parts
    CONN_30 = ErrorDef(
        ConnInitException,
        "The value is not a valid {syntax} qualified name: {value!r}: {reason}",
    )  # name_validate -- FQN syntax error
    # --- cfg shape (tabsdatak.conn.common._validation) --------------
    # `conn_cfg` / `src_cfg` / `dest_cfg` accept only JSON-like values;
    # `check_basic` reports which way a tree broke that rule.
    CONN_31 = ErrorDef(
        ConnInitException,
        "The configuration key at {path} must be a string; its type is {type}",
    )  # check_basic -- non-str dict key
    CONN_32 = ErrorDef(
        ConnInitException,
        "The configuration value at {path} must be a string, number, boolean, "
        "null, list or dict; its type is {type}",
    )  # check_basic -- non-basic value
    CONN_33 = ErrorDef(
        ConnStreamException,
        "The buffer sentinel has no base offset; the ingester must provide one "
        "at initialization",
    )
    CONN_34 = ErrorDef(
        ConnStreamException,
        "The ingester {ingester} reported a position to record without staging, "
        "but gave "
        "{offset!r}: it must report the offset the stream resumes from, and "
        "keep that offset in Tabsdata stream state",
    )
