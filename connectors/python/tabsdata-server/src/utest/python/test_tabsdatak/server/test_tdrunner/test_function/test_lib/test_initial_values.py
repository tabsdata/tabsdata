#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from tabsdatak.server._tdrunner.function._lib.initial_values import (
    USER_NAMESPACE_COLUMN,
    InvalidInitialValuesError,
    read_initial_values,
    write_initial_values,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- round-trip happy paths ---


def test_roundtrip_single_str(tmp_path):
    p = tmp_path / "iv.parquet"
    write_initial_values({"last_modified": "2026-05-28T00:00:00Z"}, p)
    assert read_initial_values(p) == {"last_modified": "2026-05-28T00:00:00Z"}


def test_roundtrip_mixed_primitive_types(tmp_path):
    p = tmp_path / "iv.parquet"
    values = {
        "a": "x",
        "b": 42,
        "c": 3.14,
        "d_true": True,
        "d_false": False,
    }
    write_initial_values(values, p)
    assert read_initial_values(p) == values


def test_roundtrip_heterogeneous_dict(tmp_path):
    """The actual SQL-incremental-ingest use case: a bind dict whose
    values are a mix of primitives. Previously broken under the
    ``dict[str, str]`` codec; now round-trips end-to-end."""
    p = tmp_path / "iv.parquet"
    values = {"last_modified": "2026-01-15T00:00:00Z", "min_id": 1234}
    write_initial_values(values, p)
    assert read_initial_values(p) == values


def test_write_list_value_rejected(tmp_path):
    """A ``list`` value is not a ``BasicTypeSpec`` (scalar only) -- a
    connector that needs a sequence serialises it to a ``str`` itself."""
    p = tmp_path / "iv.parquet"
    with pytest.raises(InvalidInitialValuesError, match="BasicTypeSpec"):
        write_initial_values({"tags": ["alpha", "beta"]}, p)
    assert not p.exists()


def test_write_dict_value_rejected(tmp_path):
    """A ``dict`` value is not a ``BasicTypeSpec`` (scalar only)."""
    p = tmp_path / "iv.parquet"
    with pytest.raises(InvalidInitialValuesError, match="BasicTypeSpec"):
        write_initial_values({"labels": {"env": "prod"}}, p)
    assert not p.exists()


def test_roundtrip_empty_dict(tmp_path):
    p = tmp_path / "iv.parquet"
    write_initial_values({}, p)
    assert read_initial_values(p) == {}


def test_roundtrip_underscored_keys_allowed(tmp_path):
    """Plain ``str`` keys -- underscores, dashes, dots, mixed, all
    legal. The old DNS-1123 regex is gone."""
    p = tmp_path / "iv.parquet"
    values = {
        "last_modified": "2026-01-01T00:00:00Z",
        "min-id": 1,
        "snake_case_key": "v",
        "kebab-case-key": "v",
        "dotted.namespace": "v",
    }
    write_initial_values(values, p)
    assert read_initial_values(p) == values


# --- read: error paths ---


def test_read_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_initial_values(tmp_path / "no-such.parquet")


def test_read_unreadable_file_raises_invalid(tmp_path):
    bogus = tmp_path / "iv.parquet"
    bogus.write_bytes(b"this is not parquet")
    with pytest.raises(InvalidInitialValuesError, match="could not read"):
        read_initial_values(bogus)


def test_read_multi_column_parquet_raises(tmp_path):
    """The codec expects exactly one ``user_namespace`` column.
    Anything else (e.g. legacy per-key columns) is rejected."""
    p = tmp_path / "iv.parquet"
    pl.DataFrame({"a": ["v"], "b": ["w"]}).write_parquet(p)
    with pytest.raises(InvalidInitialValuesError, match="user_namespace"):
        read_initial_values(p)


def test_read_wrong_column_name_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    pl.DataFrame({"some_other_column": [{"k": "v"}]}).write_parquet(p)
    with pytest.raises(InvalidInitialValuesError, match="user_namespace"):
        read_initial_values(p)


def test_read_multi_row_parquet_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    pl.DataFrame({USER_NAMESPACE_COLUMN: [{"k": "v1"}, {"k": "v2"}]}).write_parquet(p)
    with pytest.raises(InvalidInitialValuesError, match="2 row"):
        read_initial_values(p)


def test_read_zero_row_parquet_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    pl.DataFrame({
        USER_NAMESPACE_COLUMN: pl.Series([], dtype=pl.Struct({"k": pl.Utf8}))
    }).write_parquet(p)
    with pytest.raises(InvalidInitialValuesError, match="0 row"):
        read_initial_values(p)


# --- write: validation ---


def test_write_non_str_key_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    with pytest.raises(InvalidInitialValuesError, match="ContextKeySpec"):
        write_initial_values({42: "v"}, p)
    assert not p.exists()


def test_write_unsupported_value_type_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    with pytest.raises(InvalidInitialValuesError, match="BasicTypeSpec"):
        write_initial_values({"k": object()}, p)
    assert not p.exists()


def test_write_validates_before_io(tmp_path):
    """If any key/value is invalid, no file is written -- the previous
    file (or absence) is preserved."""
    p = tmp_path / "iv.parquet"
    write_initial_values({"good": "v1"}, p)
    snapshot = p.read_bytes()
    with pytest.raises(InvalidInitialValuesError):
        write_initial_values({"good": "v2", "bad": object()}, p)
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
        Path(target).write_bytes(b"corrupt")
        raise RuntimeError("simulated mid-write crash")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", boom)
    with pytest.raises(RuntimeError, match="simulated mid-write crash"):
        write_initial_values({"k": "after"}, p)

    assert p.read_bytes() == snapshot
    assert not (tmp_path / "iv.parquet.tmp").exists()

    pl.DataFrame.write_parquet = real_write_parquet


def test_write_crash_after_partial_tmp_cleans_up_orphan(tmp_path, monkeypatch):
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


def test_value_validator_uses_pydantic_against_basic_type_spec():
    """If anyone replaces the pydantic-backed validator with a
    hand-rolled isinstance ladder, that ladder will diverge from
    ``BasicTypeSpec`` over time. This test pins the
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
        initial_values._validate_key(123)  # non-str key
    assert isinstance(ei.value.__cause__, ValidationError)
