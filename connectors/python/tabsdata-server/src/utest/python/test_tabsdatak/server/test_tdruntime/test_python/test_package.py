#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tabsdatak.common._interpreter.python.error import (
    PackageNotInstalledError,
    UnsupportedInstallMethodError,
)
from tabsdatak.common._interpreter.python.package import Package
from tabsdatak.common._interpreter.python.recipe import InstallRecipe

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


def test_recipe_from_index_when_no_direct_url(monkeypatch):
    package = Package("tabsdata")
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("2.0.0", None)
    )
    recipe = package.recipe()
    assert recipe is not None
    assert recipe.recipe == InstallRecipe.INDEX
    assert recipe.version == "2.0.0"
    assert recipe.location is None


def test_recipe_from_remote_wheel_is_url(monkeypatch):
    package = Package("tabsdata")
    url = "https://example.com/wheels/tabsdata-2.0.0-py3-none-any.whl"
    direct_url = {"url": url, "archive_info": {}}
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("2.0.0", direct_url)
    )
    recipe = package.recipe()
    assert recipe.recipe == InstallRecipe.URL
    assert recipe.url == url
    assert recipe.location is None


def test_recipe_from_remote_archive_keeps_its_subdirectory(monkeypatch):
    package = Package("tabsdata-jev-github")
    url = "https://example.com/tabsdata-jev-main.tar.gz"
    direct_url = {
        "url": url,
        "archive_info": {},
        "subdirectory": "source/tabsdata-jev-github",
    }
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("0.1.0", direct_url)
    )
    recipe = package.recipe()
    assert recipe.recipe == InstallRecipe.URL
    assert recipe.url == f"{url}#subdirectory=source/tabsdata-jev-github"


def test_recipe_from_remote_git_is_vcs_pinned_to_the_commit(monkeypatch):
    package = Package("tabsdata-jev-github")
    direct_url = {
        "url": "https://github.com/owner/tabsdata-jev",
        "vcs_info": {
            "vcs": "git",
            "commit_id": "aed77fbb3733da0437d51102793af9a2183ef3aa",
            "requested_revision": "v0.1.0",
        },
        "subdirectory": "source/tabsdata-jev-github",
    }
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("0.1.0", direct_url)
    )
    recipe = package.recipe()
    assert recipe.recipe == InstallRecipe.VCS
    assert recipe.url == (
        "git+https://github.com/owner/tabsdata-jev"
        "@aed77fbb3733da0437d51102793af9a2183ef3aa"
        "#subdirectory=source/tabsdata-jev-github"
    )
    assert recipe.location is None


def test_recipe_from_local_git_is_vcs(monkeypatch, tmp_path):
    """`git+file://` records a plain `file://` URL; `vcs_info` decides."""
    package = Package("tabsdata-jev-github")
    direct_url = {
        "url": f"file://{tmp_path}",
        "vcs_info": {"vcs": "git", "commit_id": "aed77fb", "requested_revision": "main"},
    }
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("0.1.0", direct_url)
    )
    recipe = package.recipe()
    assert recipe.recipe == InstallRecipe.VCS
    assert recipe.url == f"git+file://{tmp_path}@aed77fb"


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


def test_recipe_from_folder_install_is_frozen(monkeypatch, tmp_path):
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
    recipe = package.recipe()
    assert recipe.recipe == InstallRecipe.FROZEN
    assert recipe.version == "2.0.0"
    assert recipe.location == project_root


def test_recipe_from_folder_direct_reference_is_frozen(monkeypatch, tmp_path):
    """`name @ file:///folder` records an empty `dir_info`."""
    package = Package("tabsdata-jev-github")
    project_root = tmp_path / "tabsdata-jev-github"
    project_root.mkdir()
    direct_url = {"url": f"file://{project_root}", "dir_info": {}}
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("0.1.0", direct_url)
    )
    recipe = package.recipe()
    assert recipe.recipe == InstallRecipe.FROZEN
    assert recipe.location == project_root


def test_recipe_from_source_archive_is_frozen(monkeypatch, tmp_path):
    package = Package("tabsdata-jev-github")
    archive = tmp_path / "tabsdata_jev_github-0.1.0.tar.gz"
    archive.touch()
    direct_url = {"url": f"file://{archive}", "archive_info": {}}
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("0.1.0", direct_url)
    )
    recipe = package.recipe()
    assert recipe.recipe == InstallRecipe.FROZEN
    assert recipe.location == archive


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


def test_recipe_from_folder_with_subdirectory_is_frozen_in_it(monkeypatch, tmp_path):
    package = Package("tabsdata-jev-github")
    direct_url = {
        "url": f"file://{tmp_path}",
        "dir_info": {},
        "subdirectory": "source/tabsdata-jev-github",
    }
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("0.1.0", direct_url)
    )
    recipe = package.recipe()
    assert recipe.recipe == InstallRecipe.FROZEN
    assert recipe.location == tmp_path / "source" / "tabsdata-jev-github"


def test_recipe_from_local_archive_with_subdirectory_is_url(monkeypatch, tmp_path):
    package = Package("tabsdata-jev-github")
    archive = tmp_path / "tabsdata-jev.tar.gz"
    direct_url = {
        "url": f"file://{archive}",
        "archive_info": {},
        "subdirectory": "source/tabsdata-jev-github",
    }
    monkeypatch.setattr(
        Package, "distribution", lambda self: make_distribution("0.1.0", direct_url)
    )
    recipe = package.recipe()
    assert recipe.recipe == InstallRecipe.URL
    assert recipe.url == f"file://{archive}#subdirectory=source/tabsdata-jev-github"
