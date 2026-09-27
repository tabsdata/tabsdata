#
#  Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdserver.workers.apiready import ping_url


def write_config(tmp_path, addresses: list[str]) -> str:
    config = tmp_path / "apiserver.yaml"
    rendered = "".join(f'  - "{address}"\n' for address in addresses)
    config.write_text(f"addresses:\n{rendered}", encoding="utf-8")
    return str(config)


@pytest.mark.parametrize("wildcard", ["0.0.0.0", "[::]"])
def test_a_wildcard_bind_pings_localhost(tmp_path, wildcard):
    config = write_config(tmp_path, [f"{wildcard}:2457", "0.0.0.0:2457"])
    assert ping_url([config]).startswith("http://localhost:2457/")


def test_a_specific_bind_pings_that_address(tmp_path):
    config = write_config(tmp_path, ["[::1]:2457"])
    assert ping_url([config]).startswith("http://[::1]:2457/")


def test_an_explicit_host_overrides_the_bind(tmp_path):
    config = write_config(tmp_path, ["0.0.0.0:2457", "[::]:2457"])
    assert ping_url([config], host="net-apiserver").startswith(
        "http://net-apiserver:2457/"
    )
