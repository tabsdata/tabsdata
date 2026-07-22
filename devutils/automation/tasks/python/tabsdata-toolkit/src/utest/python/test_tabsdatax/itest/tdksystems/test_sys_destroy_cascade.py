#
# Copyright 2026 Tabsdata Inc.
#

"""`tdx-system destroy` cascades to dependent domains: refuse unless -f, and
with -f destroy the domains (ignoring failures) before the system. Uses the
`dev` (no-op) backend so no external resource / docker is involved."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import store, tdx
from tabsdatax.itest.tdksystems.errors import DatagenError


def test_destroy_refuses_then_cascades_dependent_domains(stores):
    assert tdx._sys_create(["--type", "dev", "--sys", "d1", "-f"]) == 0
    assert tdx._domain_create(["--sys", "d1", "--domain", "dom1", "-f"]) == 0
    assert tdx._domain_create(["--sys", "d1", "--domain", "dom2", "-f"]) == 0

    # Without -f: refuse and list the dependents; nothing is removed.
    with pytest.raises(DatagenError, match="in use by domain"):
        tdx._sys_destroy_one("d1", None, None, force=False)
    assert store.system_path("d1").exists()
    assert store.domain_path("dom1").exists()
    assert store.domain_path("dom2").exists()

    # With -f: destroy both domains first, then the system.
    tdx._sys_destroy_one("d1", None, None, force=True)
    assert not store.domain_path("dom1").exists()
    assert not store.domain_path("dom2").exists()
    assert not store.system_path("d1").exists()


def test_destroy_without_dependents_is_unaffected(stores):
    assert tdx._sys_create(["--type", "dev", "--sys", "solo", "-f"]) == 0
    tdx._sys_destroy_one("solo", None, None, force=False)  # no dependents -> no -f needed
    assert not store.system_path("solo").exists()
