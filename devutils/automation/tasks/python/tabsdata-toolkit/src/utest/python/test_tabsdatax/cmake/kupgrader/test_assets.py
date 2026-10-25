#
# Copyright 2026 Tabsdata Inc.
#

import pytest

from tabsdatax.cmake.kupgrader.common.assets import (
    ASSETS_FAMILY,
    COPIED_ASSETS,
    copy_assets,
    stale_assets,
    verify_assets,
)
from tabsdatax.cmake.kupgrader.common.errors import KupgraderError
from tabsdatax.cmake.kupgrader.common.workflow import Context


@pytest.fixture
def context(tmp_path) -> Context:
    for name, source in COPIED_ASSETS.items():
        path = tmp_path / source
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"# {name}\n")
    return Context(mode="build", root_folder=tmp_path)


def asset_path(context: Context, name: str):
    return context.kprofile_folder / ASSETS_FAMILY / name


def test_assets_are_stale_until_copied(context):
    assert {asset.name for asset in stale_assets(context)} == set(COPIED_ASSETS)
    copied = copy_assets(context)
    assert {asset.name for asset in copied} == set(COPIED_ASSETS)
    for name, source in COPIED_ASSETS.items():
        assert asset_path(context, name).read_bytes() == (
            (context.root_folder / source).read_bytes()
        )
    assert stale_assets(context) == []
    assert copy_assets(context) == []


def test_a_changed_source_makes_its_asset_stale(context):
    copy_assets(context)
    name, source = next(iter(COPIED_ASSETS.items()))
    (context.root_folder / source).write_text("# changed\n")
    assert [asset.name for asset in stale_assets(context)] == [name]


def test_a_missing_source_fails(context):
    name, source = next(iter(COPIED_ASSETS.items()))
    (context.root_folder / source).unlink()
    with pytest.raises(KupgraderError, match=name):
        copy_assets(context)


def test_verification_fails_on_stale_assets(context):
    with pytest.raises(SystemExit):
        verify_assets(context)
    copy_assets(context)
    verify_assets(context)
