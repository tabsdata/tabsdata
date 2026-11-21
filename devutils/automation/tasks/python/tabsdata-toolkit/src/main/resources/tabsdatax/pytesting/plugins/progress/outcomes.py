#
# Copyright 2026 Tabsdata Inc.
#

import pytest


@pytest.mark.parametrize("number", range(4))
def test_passes(number):
    assert number >= 0


def test_fails():
    assert 1 == 2, "one is not two"


@pytest.fixture
def broken():
    raise RuntimeError("setup broke")


def test_errors(broken):
    pass


def test_skips():
    pytest.skip("not here")
