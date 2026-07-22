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
