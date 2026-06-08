#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatak.common._interpreter.python.pyvenv import (
    VENV_AG,
    VENV_TD,
    build_connector_recipe,
    build_connector_recipes,
    build_recipes,
    render_package,
)
from tabsdatak.common._interpreter.python.recipe import (
    InstallRecipe,
    PackageRecipe,
    TabsdataEdition,
)
from tabsdatak.common._interpreter.python.venv import (
    CONNECTORS_RELATIVE_PATH,
    TABSDATA_AGENT_PACKAGE,
    TABSDATA_CONNECTORS_NAMES,
    TABSDATA_PACKAGE,
    TABSDATA_TOOLKIT_PACKAGE,
    EnvironmentProfile,
)

pytestmark = pytest.mark.unit


def make_recipe(
    name: str,
    install: InstallRecipe = InstallRecipe.EDITABLE,
    version: str | None = "2.0.0",
    location: Path | None = None,
) -> PackageRecipe:
    return PackageRecipe(
        name=name,
        recipe=install,
        version=version,
        location=location,
    )


def make_profile(
    packages: tuple[PackageRecipe, ...],
    edition: TabsdataEdition = TabsdataEdition.OPEN_SOURCE,
) -> EnvironmentProfile:
    return EnvironmentProfile(
        root=Path("/tmp/fake"),
        python_version="3.12.13",
        edition=edition,
        packages=packages,
    )


def test_build_recipes_open_source_returns_server_only():
    main = make_recipe(TABSDATA_PACKAGE, install=InstallRecipe.REGISTRY, location=None)
    toolkit = make_recipe(
        TABSDATA_TOOLKIT_PACKAGE, install=InstallRecipe.REGISTRY, location=None
    )
    profile = make_profile(
        (main, toolkit), edition=TabsdataEdition.OPEN_SOURCE
    )
    recipes = build_recipes(profile)
    assert len(recipes) == 1
    assert recipes[0].name == VENV_TD


def test_build_recipes_enterprise_returns_server_and_agent():
    main = make_recipe(TABSDATA_PACKAGE, install=InstallRecipe.REGISTRY, location=None)
    agent = make_recipe(
        TABSDATA_AGENT_PACKAGE, install=InstallRecipe.REGISTRY, location=None
    )
    profile = make_profile((main, agent), edition=TabsdataEdition.ENTERPRISE)
    recipes = build_recipes(profile)
    assert len(recipes) == 2
    assert recipes[0].name == VENV_TD
    assert recipes[1].name == VENV_AG


def test_build_recipes_server_excludes_agent_package():
    main = make_recipe(TABSDATA_PACKAGE, install=InstallRecipe.REGISTRY, location=None)
    agent = make_recipe(
        TABSDATA_AGENT_PACKAGE, install=InstallRecipe.REGISTRY, location=None
    )
    profile = make_profile((main, agent), edition=TabsdataEdition.ENTERPRISE)
    recipes = build_recipes(profile)
    server_names = {package.name for package in recipes[0].packages}
    assert TABSDATA_AGENT_PACKAGE not in server_names
    assert TABSDATA_PACKAGE in server_names


def test_build_recipes_agent_includes_agent_package():
    main = make_recipe(TABSDATA_PACKAGE, install=InstallRecipe.REGISTRY, location=None)
    agent = make_recipe(
        TABSDATA_AGENT_PACKAGE, install=InstallRecipe.REGISTRY, location=None
    )
    profile = make_profile((main, agent), edition=TabsdataEdition.ENTERPRISE)
    recipes = build_recipes(profile)
    agent_names = {package.name for package in recipes[1].packages}
    assert TABSDATA_AGENT_PACKAGE in agent_names
    assert TABSDATA_PACKAGE in agent_names


def test_build_connector_recipes_empty_when_tabsdata_missing():
    other = make_recipe("tabsdata-toolkit", install=InstallRecipe.REGISTRY)
    assert build_connector_recipes((other,)) == ()


def test_build_connector_recipes_empty_when_tabsdata_is_wheel(tmp_path: Path):
    wheel_path = tmp_path / "tabsdata-2.0.0.whl"
    tabsdata = make_recipe(
        TABSDATA_PACKAGE, install=InstallRecipe.WHEEL, location=wheel_path
    )
    assert build_connector_recipes((tabsdata,)) == ()


def test_build_connector_recipes_uses_constants_list_for_editable(tmp_path: Path):
    tabsdata = make_recipe(
        TABSDATA_PACKAGE, install=InstallRecipe.EDITABLE, location=tmp_path
    )
    connectors = build_connector_recipes((tabsdata,))
    assert len(connectors) == len(TABSDATA_CONNECTORS_NAMES)
    names = {recipe.name for recipe in connectors}
    assert names == set(TABSDATA_CONNECTORS_NAMES)


def test_build_connector_recipe_for_registry_has_no_location():
    tabsdata = make_recipe(
        TABSDATA_PACKAGE, install=InstallRecipe.REGISTRY, location=None
    )
    recipe = build_connector_recipe("tabsdata-conn-s3", tabsdata)
    assert recipe.recipe == InstallRecipe.REGISTRY
    assert recipe.location is None
    assert recipe.version == "2.0.0"


def test_build_connector_recipe_for_editable_resolves_connector_location(
    tmp_path: Path,
):
    tabsdata = make_recipe(
        TABSDATA_PACKAGE, install=InstallRecipe.EDITABLE, location=tmp_path
    )
    recipe = build_connector_recipe("tabsdata-conn-s3", tabsdata)
    assert recipe.recipe == InstallRecipe.EDITABLE
    assert recipe.location == tmp_path / CONNECTORS_RELATIVE_PATH / "tabsdata-conn-s3"


def test_render_package_includes_name_and_recipe():
    recipe = make_recipe(
        TABSDATA_PACKAGE, install=InstallRecipe.REGISTRY, location=None
    )
    line = render_package(recipe)
    assert "name=tabsdata" in line
    assert "recipe=Registry" in line


def test_render_package_omits_location_when_none():
    recipe = make_recipe(
        TABSDATA_PACKAGE, install=InstallRecipe.REGISTRY, location=None
    )
    line = render_package(recipe)
    assert "location=" not in line


def test_render_package_includes_location_when_set(tmp_path: Path):
    recipe = make_recipe(
        TABSDATA_PACKAGE, install=InstallRecipe.EDITABLE, location=tmp_path
    )
    line = render_package(recipe)
    assert f"location={tmp_path}" in line


def test_render_package_omits_version_when_falsey():
    recipe = make_recipe(
        TABSDATA_PACKAGE,
        install=InstallRecipe.REGISTRY,
        version=None,
        location=None,
    )
    line = render_package(recipe)
    assert "version=" not in line
