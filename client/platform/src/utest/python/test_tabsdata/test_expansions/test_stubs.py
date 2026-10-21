#
# Copyright 2026 Tabsdata Inc.
#

import pytest

from test_tabsdata.test_expansions.expansions import (
    STUB_BANNER,
    exported,
    native_modules,
    public,
    stub_path,
)

NATIVE_MODULES = native_modules()


@pytest.mark.pyo3
def test_native_modules_are_registered():
    assert {
        "tabsdata.expansions.foundation.symbols",
        "tabsdata.expansions.foundation.watashi",
        "tabsdata.expansions.tableframe.features.grok.engine",
        "tabsdata.expansions.tableframe.functions.mockup",
    } <= set(NATIVE_MODULES)


@pytest.mark.pyo3
@pytest.mark.parametrize("name", sorted(NATIVE_MODULES))
def test_native_module_has_a_generated_stub(name):
    stub = stub_path(name)
    assert stub is not None, f"No stub {name.rsplit('.', 1)[-1]}.pyi for {name}"
    assert STUB_BANNER in stub.read_text(encoding="utf-8")


@pytest.mark.pyo3
@pytest.mark.parametrize("name", sorted(NATIVE_MODULES))
def test_stub_declares_what_the_native_module_exports(name):
    stub = stub_path(name)
    assert stub is not None
    assert set(exported(stub)) == public(NATIVE_MODULES[name])
