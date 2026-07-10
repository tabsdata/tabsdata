from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)


class ConnException(TabsdataException):
    """Top-level base exception for the connector hierarchy. Each
    connector package defines its own subclasses for the categories
    it needs (init / validate / runtime)."""

    pass


class ConnInitException(ConnException):
    """An object in the connector layer could not be created because
    of attribute type / value issues at construction time."""

    pass


class ConnErrorCode(ErrorCode):
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
