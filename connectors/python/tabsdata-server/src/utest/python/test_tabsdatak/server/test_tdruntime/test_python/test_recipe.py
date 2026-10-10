#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatak.common._interpreter.python.binary import Binary
from tabsdatak.common._interpreter.python.recipe import (
    SUPPORTED_INSTALL_RECIPES,
    InstallRecipe,
    PackageRecipe,
    TabsdataEdition,
    VenvRecipe,
)

pytestmark = pytest.mark.unit


def test_tabsdata_edition_values_are_human_readable():
    assert TabsdataEdition.OPEN_SOURCE.value == "Open Source"
    assert TabsdataEdition.ENTERPRISE.value == "Enterprise"


def test_install_recipe_values_are_human_readable():
    assert InstallRecipe.EDITABLE.value == "Editable"
    assert InstallRecipe.FROZEN.value == "Frozen"
    assert InstallRecipe.INDEX.value == "Index"
    assert InstallRecipe.URL.value == "Url"
    assert InstallRecipe.VCS.value == "Vcs"
    assert InstallRecipe.WHEEL.value == "Wheel"


def test_supported_install_recipes_are_every_install_recipe():
    assert SUPPORTED_INSTALL_RECIPES == frozenset(InstallRecipe)


def test_package_recipe_binaries_default_to_empty_tuple():
    recipe = PackageRecipe(
        name="tabsdata",
        recipe=InstallRecipe.INDEX,
        version="2.0.0",
        location=None,
    )
    assert recipe.binaries == ()


def test_package_recipe_is_frozen():
    recipe = PackageRecipe(
        name="tabsdata",
        recipe=InstallRecipe.INDEX,
        version="2.0.0",
        location=None,
    )
    with pytest.raises((AttributeError, Exception)):
        recipe.name = "other"


def test_package_recipe_carries_binaries():
    binaries = (Binary(name="td_apiserver", prefix="bin"),)
    recipe = PackageRecipe(
        name="tabsdata",
        recipe=InstallRecipe.EDITABLE,
        version="2.0.0",
        location=Path("/some/path"),
        binaries=binaries,
    )
    assert recipe.binaries == binaries


def test_venv_recipe_holds_packages_tuple():
    package = PackageRecipe(
        name="tabsdata",
        recipe=InstallRecipe.INDEX,
        version="2.0.0",
        location=None,
    )
    venv = VenvRecipe(name="server", packages=(package,))
    assert venv.name == "server"
    assert venv.packages == (package,)
