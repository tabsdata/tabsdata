#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for tdx._prepare_descriptor_path -- the pre-write checkpoint shared by
`create`, `connect` and `domain create`. It refuses to clobber an existing
descriptor and creates the descriptor's parent directory, so a `<dir>/<file>`
target whose `<dir>` does not yet exist works -- including for sibling artifacts
written before the descriptor itself (e.g. connect's `.ssh.log`)."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import tdx
from tabsdatax.itest.tdksystems.errors import DatagenError


def test_creates_missing_parent_directory(tmp_path):
    target = tmp_path / "missing" / "sub" / "system.json"
    assert not target.parent.exists()
    tdx._prepare_descriptor_path(str(target), force=False)
    assert target.parent.is_dir()


def test_refuses_to_clobber_existing_descriptor(tmp_path):
    target = tmp_path / "d" / "system.json"
    tdx._prepare_descriptor_path(str(target), force=False)  # creates the dir
    target.write_text("existing")
    with pytest.raises(DatagenError, match="already exists"):
        tdx._prepare_descriptor_path(str(target), force=False)


def test_force_overwrites_and_keeps_directory(tmp_path):
    target = tmp_path / "d" / "system.json"
    tdx._prepare_descriptor_path(str(target), force=False)
    target.write_text("existing")
    tdx._prepare_descriptor_path(str(target), force=True)  # --force: no raise
    assert target.parent.is_dir()
