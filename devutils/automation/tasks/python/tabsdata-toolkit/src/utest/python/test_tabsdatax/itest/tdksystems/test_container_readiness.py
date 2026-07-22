#
# Copyright 2026 Tabsdata Inc.
#

"""_wait_ready aborts immediately when _readiness_fatal reports an
unrecoverable state (e.g. StarRocks' BE crash-loop), instead of burning the
whole timeout."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import systems
from tabsdatax.itest.tdksystems.errors import DatagenError


def test_wait_ready_fails_fast_on_fatal(monkeypatch):
    sysobj = systems.get("mysql")
    monkeypatch.setattr(sysobj, "check_ready", lambda c, p: False)
    monkeypatch.setattr(sysobj, "_readiness_fatal", lambda c, p: "BE crashed")
    with pytest.raises(DatagenError, match="cannot become ready: BE crashed"):
        sysobj._wait_ready("c", "p", timeout=30)


def test_wait_ready_default_has_no_fatal(monkeypatch):
    # Default _readiness_fatal returns None, so the loop just times out.
    sysobj = systems.get("mysql")
    monkeypatch.setattr(sysobj, "check_ready", lambda c, p: False)
    with pytest.raises(DatagenError, match="did not become ready"):
        sysobj._wait_ready("c", "p", timeout=0)
