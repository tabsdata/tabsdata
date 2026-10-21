#
# Copyright 2026 Tabsdata Inc.
#

import importlib

import pytest

from test_tabsdata.test_expansions.expansions import exported, generated_stubs

GENERATED_STUBS = generated_stubs()


@pytest.mark.pyo3
def test_generated_stubs_are_installed():
    assert "tabsdata.expansions.foundation.symbols" in GENERATED_STUBS


@pytest.mark.pyo3
@pytest.mark.parametrize("name", sorted(GENERATED_STUBS))
def test_stub_symbols_are_importable(name):
    module = importlib.import_module(name)
    for symbol in exported(GENERATED_STUBS[name]):
        assert hasattr(module, symbol), f"{name}.{symbol} is not importable"
