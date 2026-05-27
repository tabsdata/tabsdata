#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tabsdatak.server._tdruntime.python.error import (
    PackageNotInstalledError,
    UnsupportedInstallMethodError,
)
from tabsdatak.server._tdruntime.python.package import Package
from tabsdatak.server._tdruntime.python.recipe import InstallRecipe

pytestmark = pytest.mark.unit


def make_distribution(version: str, direct_url: dict | None) -> MagicMock:
    distribution = MagicMock()
    distribution.version = version
    distribution.read_text.return_value = (
        json.dumps(direct_url) if direct_url is not None else None
    )
    return distribution


def test_recipe_returns_none_when_optional_package_missing(monkeypatch):
    package = Package("optional-package", mandatory=False)
    monkeypatch.setattr(Package, "distribution", lambda self: None)
    assert package.recipe() is None


def test_recipe_raises_when_mandatory_package_missing(monkeypatch):
    package = Package("tabsdata", mandatory=True)
    monkeypatch.setattr(Package, "distribution", lambda self: None)
    with pytest.raises(PackageNotInstalledError):
        package.recipe()


def test_recipe_from_registry_when_no_direct_url(monkeypatch):
    package = Package("tabsdata")
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("2.0.0", None)
    )
    recipe = package.recipe()
    assert recipe is not None
    assert recipe.recipe == InstallRecipe.REGISTRY
    assert recipe.version == "2.0.0"
    assert recipe.location is None


def test_recipe_from_registry_when_direct_url_is_non_file(monkeypatch):
    package = Package("tabsdata")
    direct_url = {"url": "https://example.com/wheels/tabsdata-2.0.0.whl"}
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("2.0.0", direct_url)
    )
    recipe = package.recipe()
    assert recipe is not None
    assert recipe.recipe == InstallRecipe.REGISTRY


def test_recipe_from_wheel(monkeypatch, tmp_path):
    package = Package("tabsdata")
    wheel = tmp_path / "tabsdata-2.0.0-py3-none-any.whl"
    wheel.write_bytes(b"")
    direct_url = {"url": f"file://{wheel}"}
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("2.0.0", direct_url)
    )
    recipe = package.recipe()
    assert recipe is not None
    assert recipe.recipe == InstallRecipe.WHEEL
    assert recipe.version == "2.0.0"
    assert recipe.location == wheel


def test_recipe_from_editable(monkeypatch, tmp_path):
    package = Package("tabsdata")
    project_root = tmp_path / "tabsdata-ee"
    project_root.mkdir()
    direct_url = {
        "url": f"file://{project_root}",
        "dir_info": {"editable": True},
    }
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("2.0.0", direct_url)
    )
    recipe = package.recipe()
    assert recipe is not None
    assert recipe.recipe == InstallRecipe.EDITABLE
    assert recipe.location == project_root


def test_recipe_from_folder_frozen_is_unsupported(monkeypatch, tmp_path):
    package = Package("tabsdata")
    project_root = tmp_path / "snapshot"
    project_root.mkdir()
    direct_url = {
        "url": f"file://{project_root}",
        "dir_info": {"editable": False},
    }
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("2.0.0", direct_url)
    )
    with pytest.raises(UnsupportedInstallMethodError):
        package.recipe()


def test_path_from_url_strips_file_protocol():
    assert Package.path_from_url("file:///abs/path/to/file.whl") == Path(
        "/abs/path/to/file.whl"
    )


def test_path_from_url_handles_backslash_prefix():
    # Linux-style URL with a literal leading backslash gets trimmed.
    if os.name == "nt":
        pytest.skip("backslash trim path is exercised on Windows runs only")
    assert Package.path_from_url("file://\\\\server\\share\\x") == Path(
        "\\server\\share\\x"
    )
