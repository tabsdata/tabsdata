"""Tests for cloud helpers — path normalisation, prefix handling, walk_paths."""
from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatax.itest.tdksystems.systems._backend import normalise_prefix, walk_paths
from tabsdatax.itest.tdksystems.systems.base import _normalize_path, _validate_path
from tabsdatax.itest.tdksystems.systems.fs.system import _validate_fs_root
from tabsdatax.itest.tdksystems.errors import DatagenError


# ---- normalise_prefix (object-store key prefixes) -------------------------

def test_normalise_prefix_empty_stays_empty():
    assert normalise_prefix("") == ""


def test_normalise_prefix_strips_leading_and_appends_trailing_slash():
    assert normalise_prefix("/foo/bar") == "foo/bar/"


def test_normalise_prefix_idempotent():
    assert normalise_prefix("foo/") == "foo/"


# ---- _normalize_path (root / base path normalisation) --------------------

@pytest.mark.parametrize("p,expected", [
    ("foo/bar",       "foo/bar"),
    ("foo/bar/",      "foo/bar"),
    ("/tmp/x",        "/tmp/x"),
    ("/tmp/x/",       "/tmp/x"),
    ("foo//bar",      "foo/bar"),
    ("/tmp///x",      "/tmp/x"),
    ("",              ""),
])
def test_normalize_path(p, expected):
    assert _normalize_path(p) == expected


# ---- _validate_path -------------------------------------------------------

@pytest.mark.parametrize("good", ["foo", "foo/bar", "/tmp/x"])
def test_validate_path_accepts(good):
    _validate_path(good, "test")


def test_validate_path_rejects_dotdot():
    with pytest.raises(DatagenError, match=".."):
        _validate_path("foo/../bar", "test")


def test_validate_path_rejects_newlines():
    with pytest.raises(DatagenError, match="newlines"):
        _validate_path("foo\nbar", "test")


# ---- _validate_fs_root ----------------------------------------------------

@pytest.mark.parametrize("ok", [
    "/tmp/tdx", "/home/user/data", "/Users/me/data", "rel/path",
])
def test_validate_fs_root_accepts(ok):
    _validate_fs_root(ok)


@pytest.mark.parametrize("bad", [
    "/etc/passwd",        # absolute, not in safe roots
    "/var/log",
    "/usr/local",
    "usr/local/foo",      # relative starting with usr
    "opt/something",      # relative starting with opt
    "single",             # only 1 element
])
def test_validate_fs_root_rejects(bad):
    with pytest.raises(DatagenError):
        _validate_fs_root(bad)


def test_validate_fs_root_rejects_empty():
    with pytest.raises(DatagenError):
        _validate_fs_root("")


# ---- walk_paths -----------------------------------------------------------

def test_walk_paths_single_file(tmp_path):
    f = tmp_path / "a.txt"
    f.write_text("hi")
    pairs = list(walk_paths([str(f)]))
    assert pairs == [(f, "a.txt")]


def test_walk_paths_directory_walks_recursively(tmp_path):
    (tmp_path / "x").mkdir()
    (tmp_path / "x" / "a.txt").write_text("a")
    (tmp_path / "x" / "sub").mkdir()
    (tmp_path / "x" / "sub" / "b.txt").write_text("b")
    pairs = list(walk_paths([str(tmp_path / "x")]))
    rels = sorted(rel for _, rel in pairs)
    assert rels == ["a.txt", "sub/b.txt"]


def test_walk_paths_mixed_file_and_dir(tmp_path):
    f = tmp_path / "single.txt"
    f.write_text("s")
    d = tmp_path / "d"
    d.mkdir()
    (d / "x.txt").write_text("x")
    pairs = list(walk_paths([str(f), str(d)]))
    rels = sorted(rel for _, rel in pairs)
    assert rels == ["single.txt", "x.txt"]


def test_walk_paths_missing_path_raises(tmp_path):
    with pytest.raises(DatagenError, match="not a file or directory"):
        list(walk_paths([str(tmp_path / "nope")]))
