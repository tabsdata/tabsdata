#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnInitException

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._select import accepts, codec, plain

pytestmark = [pytest.mark.unit]

NAMES = [
    "a.json",
    "a.json.gz",
    "a.json.ZST",
    "a.json.gz.zst",
    "a.json.csv",
    "a.jsonl",
    "x.csv.gz",
    "b.gz",
]


@pytest.mark.parametrize(
    "name, expected",
    [
        ("a.json", None),
        ("a.json.gz", ".gz"),
        ("a.json.ZST", ".zst"),
        ("a.json.zstd", ".zstd"),
        ("a.gz.json", None),
        ("a.zip", None),
        ("a.tar", None),
    ],
)
def test_the_last_extension_alone_names_the_codec(name, expected):
    assert codec(name) == expected


def test_the_plain_name_drops_one_codec_extension():
    assert plain("a.json.gz.zst") == "a.json.gz"
    assert plain("a.json") == "a.json"


@pytest.mark.parametrize(
    "pattern, include_compressed, taken",
    [
        ("*.json", False, ["a.json"]),
        ("*.json", True, ["a.json", "a.json.gz", "a.json.ZST"]),
        ("*", False, ["a.json", "a.json.csv", "a.jsonl"]),
        ("*", True, NAMES),
        ("*.json.gz", True, ["a.json.gz"]),
        ("*.json.gz", False, []),
        ("*.gz", True, ["a.json.gz", "b.gz", "x.csv.gz"]),
    ],
    ids=[
        "plain pattern",
        "plain pattern with compressed",
        "everything, plain only",
        "everything",
        "pattern naming a codec",
        "pattern naming a codec without the flag",
        "pattern naming only a codec",
    ],
)
def test_compressed_files_are_taken_only_when_asked_for(
    pattern, include_compressed, taken
):
    accept = accepts(pattern, include_compressed)
    assert sorted(name for name in NAMES if accept(name)) == sorted(taken)


@pytest.mark.parametrize("path", ["data/*.json.gz", "data/a.csv.zst", "b.xz#2"])
def test_a_path_naming_a_codec_needs_compressed_files_asked_for(path):
    with pytest.raises(ConnInitException, match="include_compressed=True"):
        _validation.validate_src_compressed(False, [path])
    _validation.validate_src_compressed(True, [path])


def test_plain_paths_need_nothing():
    _validation.validate_src_compressed(False, ["data/*.json", "a.csv"])
    _validation.validate_src_compressed(True, ["data/*.json", "a.csv"])
