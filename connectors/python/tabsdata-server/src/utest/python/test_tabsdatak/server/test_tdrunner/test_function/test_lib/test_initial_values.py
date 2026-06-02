#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from tabsdatak.server._tdrunner.function._lib.initial_values import (
    InvalidInitialValuesError,
    read_initial_values,
    write_initial_values,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- round-trip happy paths ---


def test_roundtrip_single_str(tmp_path):
    p = tmp_path / "iv.parquet"
    write_initial_values({"last-modified": "2026-05-28T00:00:00Z"}, p)
    assert read_initial_values(p) == {"last-modified": "2026-05-28T00:00:00Z"}


def test_roundtrip_mixed_primitive_types(tmp_path):
    p = tmp_path / "iv.parquet"
    values = {
        "a": "x",
        "b": 42,
        "c": 3.14,
        "d-true": True,
        "d-false": False,
    }
    write_initial_values(values, p)
    assert read_initial_values(p) == values


def test_roundtrip_list_of_str(tmp_path):
    p = tmp_path / "iv.parquet"
    write_initial_values({"tags": ["alpha", "beta", "gamma"]}, p)
    assert read_initial_values(p) == {"tags": ["alpha", "beta", "gamma"]}


def test_roundtrip_dict_of_str(tmp_path):
    p = tmp_path / "iv.parquet"
    write_initial_values({"labels": {"env": "prod", "team": "data"}}, p)
    assert read_initial_values(p) == {"labels": {"env": "prod", "team": "data"}}


def test_roundtrip_empty_dict(tmp_path):
    p = tmp_path / "iv.parquet"
    write_initial_values({}, p)
    assert read_initial_values(p) == {}


def test_roundtrip_dotted_key(tmp_path):
    p = tmp_path / "iv.parquet"
    write_initial_values({"databricks.last-modified": "2026-05-28T00:00:00Z"}, p)
    assert read_initial_values(p) == {
        "databricks.last-modified": "2026-05-28T00:00:00Z"
    }


# --- read: error paths ---


def test_read_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_initial_values(tmp_path / "no-such.parquet")


def test_read_unreadable_file_raises_invalid(tmp_path):
    bogus = tmp_path / "iv.parquet"
    bogus.write_bytes(b"this is not parquet")
    with pytest.raises(InvalidInitialValuesError, match="could not read"):
        read_initial_values(bogus)


def test_read_multi_row_parquet_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    pl.DataFrame({"k": ["v1", "v2"]}).write_parquet(p)
    with pytest.raises(InvalidInitialValuesError, match="2 row"):
        read_initial_values(p)


def test_read_zero_row_parquet_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    pl.DataFrame({"k": pl.Series("k", [], dtype=pl.Utf8)}).write_parquet(p)
    with pytest.raises(InvalidInitialValuesError, match="0 row"):
        read_initial_values(p)


def test_read_bad_key_column_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    # "k!" violates the ContextKeySpec pattern (non-alphanumeric + non-dash).
    pl.DataFrame({"k!": ["v"]}).write_parquet(p)
    with pytest.raises(InvalidInitialValuesError, match="ContextKeySpec"):
        read_initial_values(p)


# --- write: validation ---


def test_write_bad_key_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    with pytest.raises(InvalidInitialValuesError, match="ContextKeySpec"):
        write_initial_values({"bad key!": "v"}, p)
    assert not p.exists()


def test_write_empty_key_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    with pytest.raises(InvalidInitialValuesError, match="ContextKeySpec"):
        write_initial_values({"": "v"}, p)


def test_write_key_too_long_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    # ContextKeySpec caps total length at 253.
    with pytest.raises(InvalidInitialValuesError, match="ContextKeySpec"):
        write_initial_values({"a" * 254: "v"}, p)


def test_write_unsupported_value_type_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    with pytest.raises(InvalidInitialValuesError, match="ContextValueSpec"):
        write_initial_values({"k": object()}, p)
    assert not p.exists()


def test_write_list_with_non_str_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    with pytest.raises(InvalidInitialValuesError, match="ContextValueSpec"):
        write_initial_values({"k": ["a", 1, "c"]}, p)


def test_write_dict_with_non_str_key_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    with pytest.raises(InvalidInitialValuesError, match="ContextValueSpec"):
        write_initial_values({"k": {1: "v"}}, p)


def test_write_dict_with_non_str_value_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    with pytest.raises(InvalidInitialValuesError, match="ContextValueSpec"):
        write_initial_values({"k": {"a": 1}}, p)


def test_write_validates_before_io(tmp_path):
    """If any key/value is invalid, no file is written -- the previous
    file (or absence) is preserved."""
    p = tmp_path / "iv.parquet"
    # First write a valid file.
    write_initial_values({"good": "v1"}, p)
    snapshot = p.read_bytes()
    # Now attempt an invalid write -- file should stay byte-identical.
    with pytest.raises(InvalidInitialValuesError):
        write_initial_values({"good": "v2", "bad key!": "v"}, p)
    assert p.read_bytes() == snapshot


def test_write_unwritable_path_raises_oserror(tmp_path):
    a_dir = tmp_path / "iam-a-dir"
    a_dir.mkdir()
    with pytest.raises(OSError):
        write_initial_values({"k": "v"}, a_dir)


# --- type discipline: bool vs int ---


def test_bool_round_trips_as_bool_not_int(tmp_path):
    p = tmp_path / "iv.parquet"
    write_initial_values({"flag": True}, p)
    out = read_initial_values(p)
    assert out == {"flag": True}
    # ``True == 1`` in Python, so equality alone isn't proof -- check
    # the type explicitly. Polars writes booleans as bool, not int.
    assert isinstance(out["flag"], bool)


def test_int_does_not_become_bool(tmp_path):
    p = tmp_path / "iv.parquet"
    write_initial_values({"count": 1}, p)
    out = read_initial_values(p)
    assert out["count"] == 1
    assert not isinstance(out["count"], bool)


# --- crash-safe write ---------------------------------------------------


def test_write_does_not_leave_tmp_file_behind(tmp_path):
    """Successful writes ``os.replace`` the tmp file; no stray
    ``<path>.tmp`` should remain afterwards."""
    p = tmp_path / "iv.parquet"
    write_initial_values({"k": "v"}, p)
    assert p.is_file()
    assert not (tmp_path / "iv.parquet.tmp").exists()


def test_write_crash_mid_write_leaves_previous_content_intact(tmp_path, monkeypatch):
    """If the underlying ``write_parquet`` call fails mid-flight,
    ``path`` keeps its previous content -- the tmp file never gets
    renamed over the destination."""
    p = tmp_path / "iv.parquet"
    write_initial_values({"k": "before"}, p)
    snapshot = p.read_bytes()

    real_write_parquet = pl.DataFrame.write_parquet

    def boom(self, target, *a, **kw):
        # Write *something* to the tmp file first so the test simulates
        # a partial write, then explode before the os.replace can run.
        Path(target).write_bytes(b"corrupt")
        raise RuntimeError("simulated mid-write crash")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", boom)
    with pytest.raises(RuntimeError, match="simulated mid-write crash"):
        write_initial_values({"k": "after"}, p)

    # Destination untouched; tmp file cleaned up.
    assert p.read_bytes() == snapshot
    assert not (tmp_path / "iv.parquet.tmp").exists()

    # Restoring the real call works for sanity (catches accidental
    # monkeypatch leakage if this test ever gets reordered).
    pl.DataFrame.write_parquet = real_write_parquet


def test_write_crash_after_partial_tmp_cleans_up_orphan(tmp_path, monkeypatch):
    """Even when the failure happens *after* a partial tmp file
    exists, the tmp gets removed so we don't accumulate orphans
    across crashes."""
    p = tmp_path / "iv.parquet"

    def boom(self, target, *a, **kw):
        Path(target).write_bytes(b"corrupt")
        raise OSError("simulated I/O failure")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", boom)
    with pytest.raises(OSError, match="simulated I/O failure"):
        write_initial_values({"k": "v"}, p)

    assert not p.exists()
    assert not (tmp_path / "iv.parquet.tmp").exists()


# --- drift-prevention regression: pydantic-backed validators ------------


def test_value_validator_uses_pydantic_against_context_value_spec():
    """If anyone replaces the pydantic-backed validator with a
    hand-rolled isinstance ladder, that ladder will diverge from
    ``ContextValueSpec`` over time. This test pins the
    pydantic-based approach in place by verifying the validator
    chains a ``pydantic.ValidationError``."""
    from pydantic import ValidationError

    from tabsdatak.server._tdrunner.function._lib import initial_values

    with pytest.raises(InvalidInitialValuesError) as ei:
        initial_values._validate_value("k", object())
    assert isinstance(ei.value.__cause__, ValidationError)


def test_key_validator_uses_pydantic_against_context_key_spec():
    from pydantic import ValidationError

    from tabsdatak.server._tdrunner.function._lib import initial_values

    with pytest.raises(InvalidInitialValuesError) as ei:
        initial_values._validate_key("bad_underscore")
    assert isinstance(ei.value.__cause__, ValidationError)
