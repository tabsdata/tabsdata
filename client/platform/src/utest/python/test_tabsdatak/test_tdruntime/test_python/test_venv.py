#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatak._tdruntime.python.error import FeatureDetectionError
from tabsdatak._tdruntime.python.recipe import (
    InstallRecipe,
    PackageRecipe,
    TabsdataEdition,
)
from tabsdatak._tdruntime.python.venv import (
    TABSDATA_PACKAGE,
    TABSDATA_TOOLKIT_PACKAGE,
    PythonEnvironment,
)

pytestmark = pytest.mark.unit


def make_recipe(
    name: str,
    install: InstallRecipe = InstallRecipe.EDITABLE,
    location: Path | None = None,
) -> PackageRecipe:
    return PackageRecipe(
        name=name,
        recipe=install,
        version="2.0.0",
        location=location,
    )


def test_edition_from_manifest_returns_enterprise_when_flag_true():
    edition = PythonEnvironment.edition_from_manifest({"X-Enterprise": "true"})
    assert edition == TabsdataEdition.ENTERPRISE


def test_edition_from_manifest_is_case_insensitive():
    edition = PythonEnvironment.edition_from_manifest({"X-Enterprise": "TRUE"})
    assert edition == TabsdataEdition.ENTERPRISE


def test_edition_from_manifest_defaults_to_open_source_when_flag_false():
    edition = PythonEnvironment.edition_from_manifest({"X-Enterprise": "false"})
    assert edition == TabsdataEdition.OPEN_SOURCE


def test_edition_from_manifest_defaults_to_open_source_when_flag_missing():
    edition = PythonEnvironment.edition_from_manifest({})
    assert edition == TabsdataEdition.OPEN_SOURCE


def test_edition_from_location_returns_enterprise_for_tabsdata_ee(tmp_path: Path):
    recipe = make_recipe(TABSDATA_PACKAGE, location=tmp_path / "tabsdata-ee")
    assert (
        PythonEnvironment.edition_from_location(recipe) == TabsdataEdition.ENTERPRISE
    )


def test_edition_from_location_returns_open_source_for_other_paths(tmp_path: Path):
    recipe = make_recipe(TABSDATA_PACKAGE, location=tmp_path / "tabsdata-os")
    assert (
        PythonEnvironment.edition_from_location(recipe) == TabsdataEdition.OPEN_SOURCE
    )


def test_edition_from_location_raises_when_location_is_none():
    recipe = make_recipe(TABSDATA_PACKAGE, install=InstallRecipe.REGISTRY)
    with pytest.raises(FeatureDetectionError):
        PythonEnvironment.edition_from_location(recipe)


def test_split_packages_separates_toolkit_first():
    toolkit = make_recipe(TABSDATA_TOOLKIT_PACKAGE)
    main = make_recipe(TABSDATA_PACKAGE)
    other = make_recipe("tabsdata-agent")
    toolkit_only, others = PythonEnvironment.split_packages((main, toolkit, other))
    assert toolkit_only == (toolkit,)
    assert others == (main, other)


def test_split_packages_returns_empty_toolkit_when_absent():
    main = make_recipe(TABSDATA_PACKAGE)
    toolkit, others = PythonEnvironment.split_packages((main,))
    assert toolkit == ()
    assert others == (main,)


def test_link_libexec_launchers_skips_non_editable_packages(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    environment = PythonEnvironment(env_root)
    recipe = make_recipe(TABSDATA_PACKAGE, install=InstallRecipe.REGISTRY)
    environment.link_libexec_launchers((recipe,))
    assert not (env_root / "libexec" / "tabsdata").exists()


def test_link_libexec_launchers_skips_packages_without_location(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    environment = PythonEnvironment(env_root)
    recipe = make_recipe(TABSDATA_PACKAGE, install=InstallRecipe.EDITABLE)
    environment.link_libexec_launchers((recipe,))
    assert not (env_root / "libexec" / "tabsdata").exists()


def test_link_libexec_launchers_skips_missing_source_folder(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    project_root = tmp_path / "tabsdata-ee"
    project_root.mkdir()
    environment = PythonEnvironment(env_root)
    recipe = make_recipe(TABSDATA_PACKAGE, location=project_root)
    environment.link_libexec_launchers((recipe,))
    assert not (env_root / "libexec" / "tabsdata").exists()


def test_link_libexec_launchers_symlinks_each_source_file(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    project_root = tmp_path / "tabsdata-ee"
    source = project_root / "target" / "python" / "libexec" / "tabsdata"
    source.mkdir(parents=True)
    (source / "td_run_function").write_text("#!script", encoding="utf-8")
    (source / "td_fetch_request").write_text("#!script", encoding="utf-8")
    environment = PythonEnvironment(env_root)
    recipe = make_recipe(TABSDATA_PACKAGE, location=project_root)

    environment.link_libexec_launchers((recipe,))

    target = env_root / "libexec" / "tabsdata"
    assert target.is_dir()
    assert {entry.name for entry in target.iterdir()} == {
        "td_run_function",
        "td_fetch_request",
    }
    for entry in target.iterdir():
        assert entry.is_symlink()


def test_link_libexec_launchers_replaces_existing_symlink(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    project_root = tmp_path / "tabsdata-ee"
    source = project_root / "target" / "python" / "libexec" / "tabsdata"
    source.mkdir(parents=True)
    fresh = source / "td_run_function"
    fresh.write_text("fresh", encoding="utf-8")
    environment = PythonEnvironment(env_root)

    target = env_root / "libexec" / "tabsdata"
    target.mkdir(parents=True)
    stale_source = tmp_path / "stale"
    stale_source.write_text("stale", encoding="utf-8")
    (target / "td_run_function").symlink_to(stale_source)

    recipe = make_recipe(TABSDATA_PACKAGE, location=project_root)
    environment.link_libexec_launchers((recipe,))

    link = target / "td_run_function"
    assert link.is_symlink()
    assert link.resolve() == fresh.resolve()


def test_link_libexec_launchers_dedupes_same_source(tmp_path: Path):
    env_root = tmp_path / "venv"
    env_root.mkdir()
    project_root = tmp_path / "tabsdata-ee"
    source = project_root / "target" / "python" / "libexec" / "tabsdata"
    source.mkdir(parents=True)
    (source / "td_run_function").write_text("payload", encoding="utf-8")
    environment = PythonEnvironment(env_root)
    recipe_a = make_recipe(TABSDATA_PACKAGE, location=project_root)
    recipe_b = make_recipe("tabsdata-toolkit", location=project_root)

    environment.link_libexec_launchers((recipe_a, recipe_b))

    target = env_root / "libexec" / "tabsdata"
    assert len(list(target.iterdir())) == 1
