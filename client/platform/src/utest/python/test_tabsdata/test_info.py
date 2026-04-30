#
# Copyright 2026 Tabs Data Inc.
#

import pytest

from tabsdata._info import FileInfo, validate_file_info
from tabsdata.exceptions import FormatConfigurationError


class TestFileInfo:
    def test_name_value(self):
        assert FileInfo.NAME == "name"

    def test_uri_value(self):
        assert FileInfo.URI == "uri"

    def test_is_str_enum(self):
        assert isinstance(FileInfo.NAME, str)
        assert isinstance(FileInfo.URI, str)


class TestValidateFileInfo:
    def test_none_returns_none(self):
        assert validate_file_info(None) is None

    def test_file_info_passthrough(self):
        assert validate_file_info(FileInfo.NAME) is FileInfo.NAME
        assert validate_file_info(FileInfo.URI) is FileInfo.URI

    def test_string_name(self):
        assert validate_file_info("name") is FileInfo.NAME

    def test_string_uri(self):
        assert validate_file_info("uri") is FileInfo.URI

    def test_string_case_insensitive(self):
        assert validate_file_info("NAME") is FileInfo.NAME
        assert validate_file_info("Uri") is FileInfo.URI
        assert validate_file_info("URI") is FileInfo.URI

    def test_invalid_string_raises(self):
        with pytest.raises(FormatConfigurationError):
            validate_file_info("invalid")

    def test_invalid_type_raises(self):
        with pytest.raises(FormatConfigurationError):
            validate_file_info(42)
