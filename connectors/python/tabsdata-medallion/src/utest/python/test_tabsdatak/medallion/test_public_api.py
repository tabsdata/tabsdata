#
# Copyright 2026 Tabsdata Inc.
#

import importlib
import pkgutil

import tabsdatak.medallion as medallion


def test_every_exported_name_resolves():
    for name in medallion.__all__:
        assert getattr(medallion, name) is not None, name


def test_submodules_are_private():
    submodules = [info.name for info in pkgutil.iter_modules(medallion.__path__)]
    assert submodules
    assert all(name.startswith("_") or name == "assets" for name in submodules)


def test_builders_are_exported():
    builders = {
        name
        for info in pkgutil.walk_packages(medallion.__path__, "tabsdatak.medallion.")
        for name in dir(importlib.import_module(info.name))
        if name.startswith("build_")
    }
    assert builders <= set(medallion.__all__)
