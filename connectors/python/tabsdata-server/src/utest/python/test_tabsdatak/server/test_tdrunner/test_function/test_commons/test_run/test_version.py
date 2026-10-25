#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as installed_version

import pytest

from tabsdatak.server._tdrunner.function._commons.run import version as module

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def test_version_is_the_installed_server_version():
    assert module.version() == installed_version("tabsdata-server")


def test_version_without_installed_server_is_unknown(monkeypatch: pytest.MonkeyPatch):
    def missing(distribution: str) -> str:
        raise PackageNotFoundError(distribution)

    monkeypatch.setattr(module, "installed_version", missing)
    assert module.version() == module.UNKNOWN
