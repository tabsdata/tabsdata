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

    # --- Cloud locations (tabsdatak.conn.common.typing_) ------------
    CONN_5 = ErrorDef(ConnInitException, "Cannot create S3Location")  # S3Location
    CONN_6 = ErrorDef(
        ConnInitException, "Cannot create AzBlobLocation"
    )  # AzBlobLocation
    CONN_7 = ErrorDef(ConnInitException, "Cannot create GCSLocation")  # GCSLocation
    CONN_8 = ErrorDef(
        ConnInitException,
        "Configuration '{name}', invalid value '{value}'. Valid values are: {values}",
    )  # GCSLocation

    # --- FQN syntax (tabsdatak.conn.common._validation) -------------
    CONN_9 = ErrorDef(ConnInitException, "Cannot create FqnDef")  # FqnDef

    # --- Credentials (later additions) ------------------------------
    CONN_10 = ErrorDef(ConnInitException, "Cannot create GCPHmac")  # GCPHmac
