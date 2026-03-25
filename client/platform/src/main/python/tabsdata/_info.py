#
# Copyright 2026 Tabs Data Inc.
#

from enum import StrEnum

from tabsdata.exceptions import ErrorCode, FormatConfigurationError


class FileInfo(StrEnum):
    NAME = "name"
    URI = "uri"


def validate_file_info(value: str | FileInfo | None) -> FileInfo | None:
    if value is None:
        return None
    if isinstance(value, FileInfo):
        return value
    if isinstance(value, str):
        try:
            return FileInfo(value.lower())
        except ValueError:
            raise FormatConfigurationError(
                ErrorCode.FOCE5,
                ["name", "uri", None],
                value,
            )
    raise FormatConfigurationError(
        ErrorCode.FOCE5,
        ["name", "uri", None],
        type(value),
    )
