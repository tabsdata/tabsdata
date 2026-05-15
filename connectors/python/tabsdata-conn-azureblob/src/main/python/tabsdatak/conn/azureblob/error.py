from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class AzureBlobValidateException(ConnException):
    """Cross-field validation failure for the AzureBlob connector."""


class AzureBlobRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the AzureBlob connector."""


class AzureBlobErrorCode(ErrorCode):
    AZURE_BLOB_1 = ErrorDef(
        ConnInitException,
        "AzureBlobConn validation failed",
    )  # AzureBlobConn
    AZURE_BLOB_2 = ErrorDef(
        ConnInitException,
        "AzureBlobSrc validation failed",
    )  # AzureBlobSrc
    AZURE_BLOB_3 = ErrorDef(
        AzureBlobValidateException,
        "AzureBlobSrc.format_cfg format configuration validation failed",
    )  # AzureBlobSrc.validate()
    AZURE_BLOB_4 = ErrorDef(
        ConnInitException,
        "AzureBlobDest validation failed",
    )  # AzureBlobDest
    AZURE_BLOB_5 = ErrorDef(
        AzureBlobValidateException,
        "AzureBlobDest.format_cfg format configuration validation failed",
    )  # AzureBlobDest.validate()
    AZURE_BLOB_6 = ErrorDef(
        AzureBlobRuntimeException,
        "td_transporter binary not found (set $TD_TRANSPORTER_BIN or install on PATH)",
    )  # _AzureBlobSrcPlugin / _AzureBlobDestPlugin: binary lookup
    AZURE_BLOB_7 = ErrorDef(
        AzureBlobRuntimeException,
        "AzureBlob read failed (path={path})",
    )  # _AzureBlobSrcPlugin: import_one failure
    AZURE_BLOB_8 = ErrorDef(
        AzureBlobRuntimeException,
        "AzureBlob write failed (target={target})",
    )  # _AzureBlobDestPlugin: copy_one failure
    AZURE_BLOB_9 = ErrorDef(
        AzureBlobRuntimeException,
        "AzureBlob transporter report invalid (path={path})",
    )  # _AzureBlobSrcPlugin / _AzureBlobDestPlugin: bad report YAML
    AZURE_BLOB_10 = ErrorDef(
        AzureBlobRuntimeException,
        "AzureBlob credential resolution failed: {reason}",
    )  # _AzureBlobSrcPlugin / _AzureBlobDestPlugin: Secret-backed creds w/o resolver
