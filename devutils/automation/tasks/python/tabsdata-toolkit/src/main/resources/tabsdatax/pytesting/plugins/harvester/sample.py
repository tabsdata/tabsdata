#
# Copyright 2026 Tabsdata Inc.
#

import pytest


@pytest.fixture(scope="session")
def shared():
    yield "shared"


@pytest.fixture(scope="module")
def per_module():
    yield "module"


@pytest.mark.parametrize("number", range(3))
def test_passes(number, shared, per_module):
    assert number >= 0


def test_fails(shared):
    assert 1 == 2


def test_skips():
    pytest.skip("not here")
