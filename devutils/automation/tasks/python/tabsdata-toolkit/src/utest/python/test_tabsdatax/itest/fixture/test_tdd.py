#
# Copyright 2026 Tabsdata Inc.
#

"""The fixtures' tdd client, and how a store tdd cannot list reaches the tests:
only those that declare one of its backends fail, with tdd's own message."""

from __future__ import annotations

import pytest

from tabsdatax.itest.fixture import _tdd, backend as common
from tabsdatax.itest.fixture.backends.fs import FsDomain, FsSystem


def test_a_missing_tdd_says_how_to_install_it(tmp_path, monkeypatch):
    monkeypatch.setenv(_tdd.ENV_TDD_DIR, str(tmp_path))
    with pytest.raises(_tdd.TddError, match="cargo make permit"):
        _tdd.names(_tdd.SYSTEM)


def test_a_store_without_its_key_fails_with_tdds_message(tmp_path, monkeypatch):
    monkeypatch.setitem(_tdd.store_dirs, _tdd.SYSTEM, tmp_path)
    monkeypatch.delenv("_TDD_CRYPTO_KEY", raising=False)
    with pytest.raises(_tdd.TddError, match="_TDD_CRYPTO_KEY"):
        _tdd.names(_tdd.SYSTEM)


class _Item:
    def __init__(self, *marks):
        self._marks = marks

    def iter_markers(self, name):
        return iter(self._marks if name == common.MARKER else ())


def test_an_unlisted_store_fails_only_the_tests_that_declare_its_backends(
    monkeypatch,
):
    monkeypatch.setitem(common.LISTING_ERRORS, common.SYSTEM, _tdd.TddError("boom"))
    system = getattr(pytest.mark, common.MARKER)("my_fs", FsSystem).mark
    domain = getattr(pytest.mark, common.MARKER)("inbox", FsDomain).mark
    with pytest.raises(pytest.fail.Exception, match="could not be listed: boom"):
        common.pytest_runtest_setup(_Item(system))
    assert common.pytest_runtest_setup(_Item(domain)) is None
    assert common.pytest_runtest_setup(_Item()) is None
