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
        "not a cloud object-key prefix (must start with '/', no trailing '/' "
        "except the bare root, no empty segments, no backslash or NUL): {value!r}",
    )  # CloudRelativePathSpec
    CONN_18 = ErrorDef(
        ConnInitException,
        "not an absolute local path (POSIX '/a/b' or Windows 'C:/a/b'; UNC and "
        "relative paths are rejected, no empty segments or NUL): {value!r}",
    )  # LocalAbsolutePathSpec
    CONN_19 = ErrorDef(
        ConnInitException,
        "not a valid S3 bucket name (3-63 chars, lowercase letters, digits and "
        "hyphens, starting and ending alphanumeric): {value!r}",
    )  # S3BucketSpec
    CONN_20 = ErrorDef(
        ConnInitException,
        "not a valid AWS region name (e.g. 'us-east-1', 'us-gov-west-1'): {value!r}",
    )  # AwsRegionSpec
    CONN_21 = ErrorDef(
        ConnInitException,
        "not a valid GCS bucket name (3-63 chars, lowercase letters, digits, "
        "hyphens and underscores, starting and ending alphanumeric): {value!r}",
    )  # GCSBucketSpec
    CONN_22 = ErrorDef(
        ConnInitException,
        "not a valid Azure Blob container name (3-63 chars, lowercase letters and "
        "digits with single non-leading, non-trailing hyphens): {value!r}",
    )  # AzContainerSpec
    CONN_23 = ErrorDef(
        ConnInitException,
        "not a DNS host name or IPv4 literal (e.g. 'localhost', 'db.example.com', "
        "'10.0.0.1'): {value!r}",
    )  # HostSpec
    CONN_24 = ErrorDef(
        ConnInitException,
        "not a TCP port number (1-65535, written without leading zeros, sign or "
        "spaces): {value!r}",
    )  # PortSpec
    CONN_25 = ErrorDef(
        ConnInitException,
        "not a boolean flag (one of true/false, t/f, yes/no, y/n, on/off, 1/0, "
        "case-insensitive): {value!r}",
    )  # BoolSpec
    CONN_26 = ErrorDef(
        ConnInitException, "FqnDef.separator must be non-empty"
    )  # SeparatorSpec
    CONN_27 = ErrorDef(
        ConnInitException, "FqnDef.quote, when set, must be non-empty"
    )  # QuoteSpec
    CONN_28 = ErrorDef(
        ConnInitException,
        "entity_whitelist entry at index {index} is not a valid regular "
        "expression: {value!r}: {reason}",
    )  # EntityWhitelistSpec
    CONN_29 = ErrorDef(
        ConnInitException,
        "name has {parts} parts, exceeds the maximum of {max_parts}: {value!r}",
    )  # name_validate -- too many FQN parts
    CONN_30 = ErrorDef(
        ConnInitException,
        "not a valid {syntax} qualified name: {value!r}: {reason}",
    )  # name_validate -- FQN syntax error
    # --- cfg shape (tabsdatak.conn.common._validation) --------------
    # `conn_cfg` / `src_cfg` / `dest_cfg` accept only JSON-like values;
    # `check_basic` reports which way a tree broke that rule.
    CONN_31 = ErrorDef(
        ConnInitException,
        "cfg key at {path} must be a string, got {type}",
    )  # check_basic -- non-str dict key
    CONN_32 = ErrorDef(
        ConnInitException,
        "cfg value at {path} must be a string, number, boolean, null, list or "
        "dict, got {type}",
    )  # check_basic -- non-basic value
