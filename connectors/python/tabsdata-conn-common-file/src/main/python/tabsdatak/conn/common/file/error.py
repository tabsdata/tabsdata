#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class FileValidateException(ConnException):
    """Cross-field validation failure shared by the cloud-file connectors.

    :categories: error
    """


class FileRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) shared by the cloud-file connectors.

    :categories: error
    """


class FileCommonErrorCode(ErrorCode):
    """Error codes shared by the cloud-file connectors.

    :categories: error

    One variant per path / format-cfg / runtime validation site, reused
    by `azureblob`, `gcs`, `localfile` and `s3`. Raise via
    `FileCommonErrorCode.FILE_COMMON_N.exception`.
    """

    FILE_COMMON_1 = ErrorDef(
        ConnInitException,
        "Source path cannot be empty",
    )  # _validate_src_path
    FILE_COMMON_2 = ErrorDef(
        ConnInitException,
        "Source path '{path}' cannot contain '\\x00' (shown as '#')",
    )  # _validate_src_path
    FILE_COMMON_3 = ErrorDef(
        ConnInitException,
        "Source path '{path}' cannot contain '.' or '..' segments",
    )  # _validate_src_path
    FILE_COMMON_4 = ErrorDef(
        ConnInitException,
        "Source path '{path}' cannot contain '${{...}}' tokens (destination-only)",
    )  # _validate_src_path
    FILE_COMMON_5 = ErrorDef(
        ConnInitException,
        "Source path '{path}' can use '*' only in the basename (last path segment)",
    )  # _validate_src_path
    FILE_COMMON_6 = ErrorDef(
        ConnInitException,
        "Destination path cannot be empty",
    )  # _validate_dest_path
    FILE_COMMON_7 = ErrorDef(
        ConnInitException,
        "Destination path '{path}' cannot contain '\\x00' (shown as '#')",
    )  # _validate_dest_path
    FILE_COMMON_8 = ErrorDef(
        ConnInitException,
        "Destination path '{path}' cannot contain '.' or '..' segments",
    )  # _validate_dest_path
    FILE_COMMON_9 = ErrorDef(
        ConnInitException,
        "Destination path '{path}' cannot contain '*' globs",
    )  # _validate_dest_path
    FILE_COMMON_10 = ErrorDef(
        ConnInitException,
        "Destination path '{path}' has unknown token '${{{token}}}';"
        " must be one of {allowed}",
    )  # _validate_dest_path
    FILE_COMMON_11 = ErrorDef(
        ConnInitException,
        "format_cfg[{format}] has unknown keys {unknown}; "
        "allowed for {format} ({side}): {allowed}",
    )  # validate_format_cfg -- unknown-key branch
    FILE_COMMON_12 = ErrorDef(
        ConnInitException,
        "format_cfg[{format}] must be a dict, got {got_type}",
    )  # validate_format_cfg -- non-dict branch
    FILE_COMMON_13 = ErrorDef(
        ConnInitException,
        "format_cfg side must be 'src' or 'dest', got '{side}'",
    )  # validate_format_cfg -- invalid side
    FILE_COMMON_16 = ErrorDef(
        FileRuntimeException,
        "Slot/path count mismatch: {tables} parquet files vs {paths} target paths",
    )  # run_write_out -- pre-flight check
    FILE_COMMON_17 = ErrorDef(
        ConnInitException,
        "Source format=AUTO could not be inferred from paths {paths}: {reason}. "
        "Set `format=SrcFileFormat.<...>` explicitly.",
    )  # resolve_auto_src_format -- AUTO inference failure
    FILE_COMMON_18 = ErrorDef(
        ConnInitException,
        "Destination format=AUTO could not be inferred from paths {paths}: {reason}. "
        "Set `format=DestFileFormat.<...>` explicitly.",
    )  # resolve_auto_dest_format -- AUTO inference failure
    FILE_COMMON_19 = ErrorDef(
        ConnInitException, "Cannot create FileStorageConnMetaModel"
    )
    FILE_COMMON_20 = ErrorDef(
        ConnInitException,
        "Cannot create FileStorageEntity",
    )
    FILE_COMMON_21 = ErrorDef(
        ConnInitException,
        "Cannot create FilePattern",
    )
    FILE_COMMON_22 = ErrorDef(
        ConnInitException,
        "Source path '{path}' must be relative (cannot start with '/')",
    )  # validate_src_path
    FILE_COMMON_23 = ErrorDef(
        ConnInitException,
        "Source path '{path}' cannot end with '/'",
    )  # validate_src_path
    FILE_COMMON_24 = ErrorDef(
        ConnInitException,
        "Source path '{path}' cannot contain empty segments ('//')",
    )  # validate_src_path
    FILE_COMMON_25 = ErrorDef(
        ConnInitException,
        "Destination path '{path}' must be relative (cannot start with '/')",
    )  # validate_dest_path
    FILE_COMMON_26 = ErrorDef(
        ConnInitException,
        "Destination path '{path}' cannot end with '/'",
    )  # validate_dest_path
    FILE_COMMON_27 = ErrorDef(
        ConnInitException,
        "Destination path '{path}' cannot contain empty segments ('//')",
    )  # validate_dest_path
    FILE_COMMON_28 = ErrorDef(
        ConnInitException,
        "Source path '{path}' has a malformed worksheet fragment '#{fragment}': "
        "expected a decimal worksheet number, as in 'book.xlsx#2'",
    )  # split_sheet_fragment (validate_src_format)
    FILE_COMMON_29 = ErrorDef(
        ConnInitException,
        "Source path '{path}' names worksheet 0: worksheet numbers start at 1, "
        "counting every sheet the workbook declares",
    )  # split_sheet_fragment (validate_src_format)
    FILE_COMMON_30 = ErrorDef(
        ConnInitException,
        "Source path '{path}' has a '#<sheet>' worksheet fragment, which only an "
        "EXCEL source takes (format is {format})",
    )  # split_sheet_fragment (validate_src_format)
    FILE_COMMON_31 = ErrorDef(
        ConnInitException,
        "An EBCDIC source needs a copybook: set "
        "format_cfg[SrcFileFormat.EBCDIC]['copybook'] to a list of copybook "
        "sources, one for each entry in 'paths'",
    )  # validate_src_format
    FILE_COMMON_32 = ErrorDef(
        ConnInitException,
        "An EBCDIC source reads one copybook per path, so "
        "format_cfg[SrcFileFormat.EBCDIC]['copybook'] must be a list of "
        "{paths} entries, one for each entry in 'paths' (got {given})",
    )  # validate_src_format
    FILE_COMMON_34 = ErrorDef(
        ConnInitException,
        "format_cfg[SrcFileFormat.EBCDIC]['codepage'] is '{codepage}', which is "
        "not a supported EBCDIC code page. Use one of: {supported}",
    )  # validate_src_format
    FILE_COMMON_33 = ErrorDef(
        ConnInitException,
        "Source path '{path}' uses a wildcard, which an EBCDIC source cannot: "
        "each path is read against its own copybook, and a wildcard stands for "
        "an unknown number of files. Name each file on its own path",
    )  # validate_src_format
    FILE_COMMON_35 = ErrorDef(
        ConnInitException,
        "initial_last_modified is required when purge=True: purge deletes source files "
        "older than the incremental initial_last_modified cursor.",
    )  # validate_src_purge
