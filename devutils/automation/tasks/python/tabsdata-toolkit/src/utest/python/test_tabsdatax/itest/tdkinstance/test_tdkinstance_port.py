#
# Copyright 2026 Tabsdata Inc.
#

import socket

import pytest

from tabsdatax.itest.tdkinstance.commands.port import pick_base, port_free


def _ipv6_loopback_available() -> bool:
    if not socket.has_ipv6:
        return False
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
            probe.bind(("::1", 0))
    except OSError:
        return False
    return True


def _listen(family: socket.AddressFamily, address: str) -> socket.socket:
    listener = socket.socket(family, socket.SOCK_STREAM)
    if family == socket.AF_INET6:
        listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    listener.bind((address, 0))
    listener.listen()
    return listener


def test_a_port_held_on_ipv4_loopback_is_busy():
    with _listen(socket.AF_INET, "127.0.0.1") as listener:
        assert not port_free(listener.getsockname()[1])


@pytest.mark.skipif(
    not _ipv6_loopback_available(), reason="the host has no IPv6 loopback"
)
def test_a_port_held_only_on_ipv6_loopback_is_busy():
    with _listen(socket.AF_INET6, "::1") as listener:
        assert not port_free(listener.getsockname()[1])


def test_a_released_port_is_free():
    with _listen(socket.AF_INET, "127.0.0.1") as listener:
        value = listener.getsockname()[1]
    assert port_free(value)


@pytest.mark.skipif(
    not _ipv6_loopback_available(), reason="the host has no IPv6 loopback"
)
def test_the_picker_skips_a_window_held_only_on_ipv6():
    with _listen(socket.AF_INET6, "::1") as listener:
        held = listener.getsockname()[1]
        base = pick_base([], count=1, floor=held, ceiling=held + 50, stride=1)
    assert base is not None
    assert base != held
