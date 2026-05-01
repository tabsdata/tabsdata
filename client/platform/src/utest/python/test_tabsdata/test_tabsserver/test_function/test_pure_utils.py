#
# Copyright 2026 Tabs Data Inc.
#

"""
Unit tests for pure-logic utility functions in _tabsserver/function/.
Covers: lastmod_info_parsing, logging_utils, sql_utils, global_utils (URI/path),
        store_results_utils (schema hashing, meta info), offset_utils (update logic),
        execution_context (MountOptions).
"""

import base64
import json
import os
import sys

import polars as pl
import pytest

from tabsdata._tabsserver.function.execution_context import MountOptions
from tabsdata._tabsserver.function.global_utils import (
    convert_path_to_uri,
    convert_uri_to_path,
    uri_join,
)
from tabsdata._tabsserver.function.lastmod_info_parsing import Entry, LastModInfo
from tabsdata._tabsserver.function.logging_utils import pad_string
from tabsdata._tabsserver.function.offset_utils import (
    NEW_MODE,
    NONE_MODE,
    SAME_MODE,
    Offset,
)
from tabsdata._tabsserver.function.sql_utils import (
    MARIADB_COLLATION,
    add_mariadb_collation,
)
from tabsdata._tabsserver.function.store_results_utils import (
    _extract_index,
    arrow_schema_hash,
    get_arrow_schema,
    get_table_meta_info_from_lf,
)

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


# ---------------------------------------------------------------------------
# lastmod_info_parsing
# ---------------------------------------------------------------------------


def _encode_lastmod(data: dict) -> str:
    return base64.b64encode(json.dumps(data).encode()).decode().rstrip("=")


class TestLastModInfoParsing:
    def test_basic_parsing(self):
        raw = {"V1": {"initial_last_modified": "2026-01-01T00:00:00Z", "entries": {}}}
        info = LastModInfo(_encode_lastmod(raw))
        assert info.initial_last_modified == "2026-01-01T00:00:00Z"
        assert info.entries == []

    def test_entries_parsed(self):
        raw = {
            "V1": {
                "initial_last_modified": None,
                "entries": {
                    "table_a": ["2026-01-01T00:00:00Z", ["file1.parquet"]],
                    "table_b": [
                        "2026-02-01T00:00:00Z",
                        ["file2.parquet", "file3.parquet"],
                    ],
                },
            }
        }
        info = LastModInfo(_encode_lastmod(raw))
        entries = info.entries
        assert len(entries) == 2
        names = {e.name for e in entries}
        assert names == {"table_a", "table_b"}
        for entry in entries:
            assert isinstance(entry, Entry)
            assert isinstance(entry.files, list)

    def test_empty_v1(self):
        raw = {"V1": {}}
        info = LastModInfo(_encode_lastmod(raw))
        assert info.initial_last_modified is None
        assert info.entries == []

    def test_missing_v1_key(self):
        raw = {"V2": {"something": "else"}}
        info = LastModInfo(_encode_lastmod(raw))
        assert info.info == {}
        assert info.initial_last_modified is None
        assert info.entries == []

    def test_repr(self):
        raw = {"V1": {}}
        info = LastModInfo(_encode_lastmod(raw))
        assert "LastModInfo" in repr(info)

    def test_padding_in_base64(self):
        """Verify that base64 strings with and without padding both work."""
        raw = {"V1": {"initial_last_modified": "test"}}
        # With padding
        with_padding = base64.b64encode(json.dumps(raw).encode()).decode()
        info1 = LastModInfo(with_padding)
        # Without padding
        without_padding = with_padding.rstrip("=")
        info2 = LastModInfo(without_padding)
        assert info1.initial_last_modified == info2.initial_last_modified


# ---------------------------------------------------------------------------
# logging_utils
# ---------------------------------------------------------------------------


class TestPadString:
    def test_default_padding(self):
        result = pad_string("hello")
        assert result.startswith("hello ")
        assert len(result) == 60
        assert result.endswith("-")

    def test_custom_symbol(self):
        result = pad_string("test", symbol="*", length=20)
        assert result == "test " + "*" * 15

    def test_string_longer_than_length(self):
        result = pad_string("a" * 100, length=10)
        # ljust doesn't truncate, so result is longer than length
        assert result.startswith("a" * 100 + " ")

    def test_empty_string(self):
        result = pad_string("", length=10)
        assert result == " " + "-" * 9


# ---------------------------------------------------------------------------
# sql_utils
# ---------------------------------------------------------------------------


class TestAddMariadbCollation:
    def test_adds_collation_to_plain_uri(self):
        uri = "mariadb://host:3306/db"
        result = add_mariadb_collation(uri)
        assert result == f"mariadb://host:3306/db?collation={MARIADB_COLLATION}"

    def test_appends_collation_to_existing_params(self):
        uri = "mariadb://host:3306/db?charset=utf8mb4"
        result = add_mariadb_collation(uri)
        assert result == (
            f"mariadb://host:3306/db?charset=utf8mb4&collation={MARIADB_COLLATION}"
        )

    def test_preserves_existing_collation(self):
        uri = "mariadb://host:3306/db?collation=custom_collation"
        result = add_mariadb_collation(uri)
        assert result == uri  # unchanged

    def test_preserves_collation_in_middle_of_params(self):
        uri = "mariadb://host:3306/db?collation=custom&other=param"
        result = add_mariadb_collation(uri)
        assert result == uri  # unchanged


# ---------------------------------------------------------------------------
# global_utils — URI/path conversion
# ---------------------------------------------------------------------------


class TestUriJoin:
    def test_folder_without_trailing_slash(self):
        assert uri_join("s3://bucket/folder", "file.txt") == "s3://bucket/folder/file.txt"

    def test_folder_with_trailing_slash(self):
        assert uri_join("s3://bucket/folder/", "file.txt") == "s3://bucket/folder/file.txt"

    def test_local_path(self):
        assert uri_join("/tmp/data", "output.parquet") == "/tmp/data/output.parquet"

    def test_file_uri(self):
        assert (
            uri_join("file:///tmp/data", "out.parquet")
            == "file:///tmp/data/out.parquet"
        )

    def test_multiple_trailing_slashes(self):
        assert uri_join("gs://bucket///", "file.txt") == "gs://bucket/file.txt"


class TestConvertPathToUri:
    def test_simple_path(self, tmp_path):
        path = str(tmp_path / "test.parquet")
        uri = convert_path_to_uri(path)
        assert uri.startswith("file://")
        assert "test.parquet" in uri

    def test_already_uri(self):
        uri = "file:///tmp/test.parquet"
        assert convert_path_to_uri(uri) == uri

    def test_trailing_separator_preserved(self, tmp_path):
        path = str(tmp_path) + os.path.sep
        uri = convert_path_to_uri(path)
        assert uri.endswith("/")


class TestConvertUriToPath:
    def test_roundtrip(self, tmp_path):
        original = str(tmp_path / "test.parquet")
        uri = convert_path_to_uri(original)
        restored = convert_uri_to_path(uri)
        assert os.path.normpath(restored) == os.path.normpath(original)


# ---------------------------------------------------------------------------
# store_results_utils — schema hashing & meta info
# ---------------------------------------------------------------------------


class TestGetTableMetaInfo:
    def test_basic_meta_info(self):
        lf = pl.LazyFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        meta = get_table_meta_info_from_lf(lf)
        assert meta["column_count"] == 2
        assert meta["row_count"] == 3
        assert isinstance(meta["schema_hash"], str)
        assert len(meta["schema_hash"]) == 64  # SHA256 hex

    def test_empty_frame(self):
        lf = pl.LazyFrame({"a": [], "b": []}).cast({"a": pl.Int64, "b": pl.Utf8})
        meta = get_table_meta_info_from_lf(lf)
        assert meta["column_count"] == 2
        assert meta["row_count"] == 0
        assert isinstance(meta["schema_hash"], str)


class TestArrowSchemaHash:
    def test_same_schema_same_hash(self):
        lf1 = pl.LazyFrame({"a": [1], "b": ["x"]})
        lf2 = pl.LazyFrame({"a": [2], "b": ["y"]})
        hash1 = arrow_schema_hash(get_arrow_schema(lf1))
        hash2 = arrow_schema_hash(get_arrow_schema(lf2))
        assert hash1 == hash2

    def test_different_schema_different_hash(self):
        lf1 = pl.LazyFrame({"a": [1]})
        lf2 = pl.LazyFrame({"a": [1], "b": [2]})
        hash1 = arrow_schema_hash(get_arrow_schema(lf1))
        hash2 = arrow_schema_hash(get_arrow_schema(lf2))
        assert hash1 != hash2

    def test_column_order_independent_when_sorted(self):
        lf1 = pl.LazyFrame({"a": [1], "b": [2]})
        lf2 = pl.LazyFrame({"b": [2], "a": [1]})
        hash1 = arrow_schema_hash(get_arrow_schema(lf1), sort_schema=True)
        hash2 = arrow_schema_hash(get_arrow_schema(lf2), sort_schema=True)
        assert hash1 == hash2

    def test_column_order_matters_when_unsorted(self):
        lf1 = pl.LazyFrame({"a": [1], "b": [2]})
        lf2 = pl.LazyFrame({"b": [2], "a": [1]})
        hash1 = arrow_schema_hash(get_arrow_schema(lf1), sort_schema=False)
        hash2 = arrow_schema_hash(get_arrow_schema(lf2), sort_schema=False)
        assert hash1 != hash2


class TestExtractIndex:
    def test_valid_filename(self):
        assert _extract_index("parquet", "/path/to/data_0.parquet") == 0
        assert _extract_index("parquet", "/path/to/data_42.parquet") == 42

    def test_invalid_filename_raises(self):
        with pytest.raises(ValueError, match="does not contain an index"):
            _extract_index("parquet", "/path/to/no_index.parquet")


# ---------------------------------------------------------------------------
# offset_utils — update_new_values logic
# ---------------------------------------------------------------------------


class TestOffsetUpdateNewValues:
    def test_update_with_valid_dict(self):
        offset = Offset()
        offset.update_new_values({"key1": "value1", "key2": 42})
        assert offset.new_offset == {"key1": "value1", "key2": 42}
        assert offset.update_mode == NEW_MODE

    def test_update_with_same_mode(self):
        offset = Offset()
        offset.update_new_values(SAME_MODE)
        assert offset.update_mode == SAME_MODE
        assert offset.new_offset == {}

    def test_update_with_non_string_keys_raises(self):
        offset = Offset()
        with pytest.raises(TypeError, match="Invalid type for new offset"):
            offset.update_new_values({1: "value"})

    def test_update_with_invalid_type_raises(self):
        offset = Offset()
        with pytest.raises(TypeError, match="Invalid type or value"):
            offset.update_new_values(42)

    def test_update_with_none_raises(self):
        offset = Offset()
        with pytest.raises(TypeError, match="Invalid type or value"):
            offset.update_new_values(None)

    def test_multiple_updates_merge(self):
        offset = Offset()
        offset.update_new_values({"a": 1})
        offset.update_new_values({"b": 2})
        assert offset.new_offset == {"a": 1, "b": 2}

    def test_update_overwrites_existing_key(self):
        offset = Offset()
        offset.update_new_values({"a": 1})
        offset.update_new_values({"a": 99})
        assert offset.new_offset == {"a": 99}


class TestOffsetChangedProperty:
    def test_not_changed_initially(self):
        offset = Offset()
        assert offset.changed is False

    def test_not_changed_in_none_mode(self):
        offset = Offset()
        offset.update_mode = NONE_MODE
        offset.new_offset = {"a": 1}
        assert offset.changed is False

    def test_changed_when_new_differs_from_loaded(self):
        offset = Offset()
        offset.loaded_offset = {"a": 1}
        offset.new_offset = {"a": 2}
        offset.update_mode = NEW_MODE
        assert offset.changed is True

    def test_not_changed_when_new_equals_loaded(self):
        offset = Offset()
        offset.loaded_offset = {"a": 1}
        offset.new_offset = {"a": 1}
        offset.update_mode = NEW_MODE
        assert offset.changed is False

    def test_not_changed_in_same_mode(self):
        offset = Offset()
        offset.loaded_offset = {"a": 1}
        offset.new_offset = {"a": 2}
        offset.update_mode = SAME_MODE
        assert offset.changed is False


# ---------------------------------------------------------------------------
# execution_context — MountOptions
# ---------------------------------------------------------------------------


class TestMountOptions:
    def test_get_options_for_prefix(self):
        options = MountOptions({
            "S3_access_key": "AKIA...",
            "S3_secret_key": "secret",
            "GCS_project": "my-project",
        })
        s3_opts = options.get_options_for_prefix("S3")
        assert s3_opts == {"access_key": "AKIA...", "secret_key": "secret"}

    def test_get_options_no_match(self):
        options = MountOptions({"S3_access_key": "AKIA..."})
        gcs_opts = options.get_options_for_prefix("GCS")
        assert gcs_opts == {}

    def test_get_options_empty(self):
        options = MountOptions({})
        assert options.get_options_for_prefix("S3") == {}

    def test_getitem(self):
        options = MountOptions({"key": "value"})
        assert options["key"] == "value"

    def test_getitem_missing_raises(self):
        options = MountOptions({})
        with pytest.raises(KeyError):
            _ = options["missing"]
