#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.common._error import CommonInitException
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class FileFormatErrorCode(ErrorCode):
    FILE_FORMAT_1 = ErrorDef(
        CommonInitException,
        "Unsupported source file format: '{format}'",
    )  # src_format_to_transporter -- unhandled SrcFileFormat
    FILE_FORMAT_2 = ErrorDef(
        CommonInitException,
        "Unsupported destination file format: '{format}'",
    )  # dest_format_to_transporter -- unhandled DestFileFormat
    FILE_FORMAT_3 = ErrorDef(
        CommonInitException,
        "Source format=AUTO could not be inferred from paths {paths}: {reason}. "
        "Set ``format=SrcFileFormat.<...>`` explicitly.",
    )  # resolve_auto_src_format -- AUTO inference failure
    FILE_FORMAT_4 = ErrorDef(
        CommonInitException,
        "Destination format=AUTO could not be inferred from paths {paths}: {reason}. "
        "Set ``format=DestFileFormat.<...>`` explicitly.",
    )  # resolve_auto_dest_format -- AUTO inference failure
    FILE_FORMAT_5 = ErrorDef(
        CommonInitException,
        "A '#{sheet}' worksheet fragment is only valid on an EXCEL source, but the "
        "format resolved to '{format}'",
    )  # import_request -- a sheet survived onto a non-Excel source
    FILE_FORMAT_6 = ErrorDef(
        CommonInitException,
        "An EBCDIC source needs a copybook: set format_cfg's 'copybook' to a "
        "list of copybook sources, one for each entry in 'paths'",
    )  # src_format_to_transporter -- no copybook
    FILE_FORMAT_7 = ErrorDef(
        CommonInitException,
        "An EBCDIC copybook reached the reader as a list of {count} rather than "
        "the one entry for this path; the per-path list was never narrowed",
    )  # src_format_to_transporter -- an un-narrowed copybook list
