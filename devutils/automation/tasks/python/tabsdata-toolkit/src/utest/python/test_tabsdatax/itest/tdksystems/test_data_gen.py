"""Tests for tabsdatax.itest.tdksystems.data_gen — bundled-model resolution + sizeable check."""
from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatax.itest.tdksystems import data_gen
from tabsdatax.itest.tdksystems.errors import DatagenError


# ---- list_models / model_dir ----------------------------------------------

def test_list_models_returns_known_bundled():
    models = data_gen.list_models()
    expected_subset = {
        "airportdb", "courierlogistics", "employees", "hospital",
        "restaurant", "sakila", "schoolgrades", "supermarket", "world",
    }
    assert expected_subset.issubset(set(models))


def test_list_models_is_sorted():
    models = data_gen.list_models()
    assert models == sorted(models)


def test_model_dir_returns_existing_directory():
    p = data_gen.model_dir("courierlogistics")
    assert p.is_dir()
    assert (p / "datagen.py").is_file()
    assert (p / "sizing.json").is_file()


def test_model_dir_unknown_raises_with_available_list():
    with pytest.raises(DatagenError, match="unknown model"):
        data_gen.model_dir("nope-not-a-model")


# ---- is_sizeable ----------------------------------------------------------

def test_is_sizeable_true_for_courierlogistics():
    assert data_gen.is_sizeable("courierlogistics") is True


def test_is_sizeable_unknown_model_raises():
    with pytest.raises(DatagenError):
        data_gen.is_sizeable("nope-not-a-model")


def test_is_sizeable_false_when_sizing_lacks_prolific(tmp_path, monkeypatch):
    """Synthesise a fixed-data model and verify is_sizeable returns False."""
    # Point models root at a tmp dir with a fixed model.
    m = tmp_path / "fixed"
    m.mkdir()
    (m / "datagen.py").write_text("def main(): pass\n")
    (m / "sizing.json").write_text("{}\n")
    monkeypatch.setattr(data_gen, "_models_root", lambda: tmp_path)
    assert data_gen.list_models() == ["fixed"]
    assert data_gen.is_sizeable("fixed") is False


def test_is_sizeable_handles_invalid_sizing_json(tmp_path, monkeypatch):
    m = tmp_path / "broken"
    m.mkdir()
    (m / "datagen.py").write_text("def main(): pass\n")
    (m / "sizing.json").write_text("not json {")
    monkeypatch.setattr(data_gen, "_models_root", lambda: tmp_path)
    assert data_gen.is_sizeable("broken") is False


# ---- SIZE_BUCKETS / VALID_FORMATS -----------------------------------------

def test_size_buckets_cover_xs_through_xl():
    assert set(data_gen.SIZE_BUCKETS) == {"xs", "s", "m", "l", "xl"}
    # Should be monotonically increasing.
    sizes = [data_gen.SIZE_BUCKETS[k] for k in ("xs", "s", "m", "l", "xl")]
    assert sizes == sorted(sizes)


def test_valid_formats_include_all_emitters():
    assert set(data_gen.VALID_FORMATS) == {"sql", "csv", "json", "parquet"}
