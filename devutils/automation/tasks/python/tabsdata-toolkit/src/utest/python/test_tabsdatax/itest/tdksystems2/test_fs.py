#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end fs (file) system — real local staging, no docker.

fs is local, so the whole create -> model -> data -> modify-data path runs for
real against a temp root: it exercises the FileBackend base and the csv/ndjson/
parquet writers (files named ``<entity>_<stamp>.<fmt>``), and the insert-only
modify-data rule.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatax.itest.tdksystems2 import descriptor, store, tdx
from tabsdatax.itest.tdksystems2.errors import TdxError


def _sys(argv):
    return tdx._dispatch("tdx-system", argv)


def _dom(argv):
    return tdx._dispatch("tdx-domain", argv)


def test_fs_end_to_end(stores, tmp_path):
    root = str(tmp_path / "backends" / "fsroot")
    assert _sys(["create", "--type", "fs", "--sys", "f", "--sys-path", root]) == 0
    assert _dom(["create", "--sys", "f", "--domain", "d"]) == 0
    assert _dom(["create-model", "--domain", "d", "--model", "employees"]) == 0
    assert _dom(["create-data", "--domain", "d", "--size", "1"]) == 0

    info = descriptor.load(store.domain_path("d"))
    base = Path(info.meta["BASE_PATH"])
    # a timestamped file per entity per format: <entity>_<stamp>.<fmt>
    for fmt in ("csv", "ndjson", "parquet"):
        assert list((base / fmt).glob(f"employees_*.{fmt}"))
        assert list((base / fmt).glob(f"departments_*.{fmt}"))
    # no SQLALCHEMY_URI on a file domain; the URI is a file:// URI; entities are
    # the staged file paths (relative to the domain root)
    assert "SQLALCHEMY_URI" not in info.meta
    assert info.meta["URI"].startswith("file://")
    assert any(k.startswith("csv/employees_") for k in info.entities)

    # insert-only modify-data stages NEW timestamped files alongside the seed
    # (separate CLI invocations are always ms apart; nudge the clock in-process)
    import time

    before = len(list((base / "csv").glob("employees_*.csv")))
    time.sleep(0.005)
    assert (
        _dom([
            "modify-data",
            "--domain",
            "d",
            "--entity",
            "employees",
            "--sets",
            "2",
            "--ops",
            "Insert",
            "--seed",
            "5",
        ])
        == 0
    )
    assert len(list((base / "csv").glob("employees_*.csv"))) == before + 1

    # update / delete are rejected — a file change is only a new file
    with pytest.raises(TdxError):
        _dom([
            "modify-data",
            "--domain",
            "d",
            "--entity",
            "employees",
            "--sets",
            "1",
            "--ops",
            "Delete",
            "--seed",
            "6",
        ])


def test_fs_capabilities():
    from tabsdatax.itest.tdksystems2 import datagen, systems

    fs = systems.get("fs")
    # not a container (no restart / fetch-bin), but supports models/data/changes.
    assert not fs.is_container
    assert fs.supports_models and fs.supports_data and fs.supports_changes
    # any model can be staged as files — no per-system DDL.
    assert set(fs.available_models()) == set(datagen.list_models())


def test_fs_rejects_unsafe_root():
    from tabsdatax.itest.tdksystems2.systems.fs.backend import validate_fs_root

    for bad in ("", "relative/path", "/etc/x", "/single"):
        with pytest.raises(TdxError):
            validate_fs_root(bad)
    validate_fs_root("/tmp/tdx-ok/data")  # absolute, safe head, >= 2 elements
