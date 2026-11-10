#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import signal
import sys

import pytest

from tabsdatak.server._tdrunner.function._commons.run import shutdown

STOP_SIGNALS = [signal.SIGTERM, signal.SIGINT] + (
    [signal.SIGBREAK] if sys.platform == "win32" else []
)
HANDLED_SIGNALS = STOP_SIGNALS + ([] if sys.platform == "win32" else [signal.SIGHUP])


@pytest.fixture
def restored_handlers(monkeypatch):
    monkeypatch.setattr(shutdown, "requested", False)
    saved = {signum: signal.getsignal(signum) for signum in HANDLED_SIGNALS}
    yield
    for signum, handler in saved.items():
        signal.signal(signum, handler)


@pytest.mark.parametrize("signum", STOP_SIGNALS, ids=lambda signum: signum.name)
def test_a_stop_signal_requests_the_shutdown_and_stops(restored_handlers, signum):
    stopped: list[bool] = []
    shutdown.listen_for_shutdown(lambda: stopped.append(True))

    signal.getsignal(signum)(signum, None)

    assert shutdown.is_shutdown_requested()
    assert stopped == [True]


@pytest.mark.skipif(sys.platform == "win32", reason="Windows has no SIGHUP")
def test_sighup_is_ignored(restored_handlers):
    stopped: list[bool] = []
    shutdown.listen_for_shutdown(lambda: stopped.append(True))

    handler = signal.getsignal(signal.SIGHUP)
    assert callable(handler)
    handler(signal.SIGHUP, None)

    assert not shutdown.is_shutdown_requested()
    assert stopped == []
