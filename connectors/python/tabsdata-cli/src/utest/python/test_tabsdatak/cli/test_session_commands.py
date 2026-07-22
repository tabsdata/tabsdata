#
#  Copyright 2026 Tabsdata Inc.
#

"""Top-level session / config ``tdk`` commands: ``hints``, ``logout``, ``status``.

These live directly on the root group (not in a sub-group) and were previously
untested. ``login`` / ``shell`` are excluded: they drive real network auth and
an interactive prompt loop respectively. Same mock-client + tmp-dir seam as the
render / mutating harnesses; ``hints`` is fully offline (it only reads/writes
``options.json`` in the tabsdata dir).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    return client


def _invoke(argv, *, client, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with patch(
        "tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject
    ), patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)):
        return CliRunner().invoke(cli, argv)


# --- hints on/off: persists the toggle to options.json ----------------------


@pytest.mark.parametrize(
    "mode,stored,message",
    [
        ("on", "enabled", "Hints enabled"),
        ("off", "disabled", "Hints disabled"),
    ],
)
def test_hints_persists_toggle(mode, stored, message, tmp_path):
    result = _invoke(["hints", mode], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert message in result.output
    saved = json.loads((tmp_path / "options.json").read_text())
    assert saved["hints"] == stored


def test_hints_rejects_unknown_mode(tmp_path):
    # ``mode`` is a Choice(on, off); anything else is a usage error.
    result = _invoke(["hints", "maybe"], client=_client(), tmp_path=tmp_path)
    assert result.exit_code != 0
    assert "maybe" in result.output


# --- logout: calls the SDK, then clears local credentials + pins ------------


def test_logout_calls_sdk_and_clears_local_state(tmp_path):
    (tmp_path / "connection.json").write_text(json.dumps({"url": "http://x"}))
    (tmp_path / "pinned.json").write_text(json.dumps({"project": "p", "collection": "c"}))
    client = _client()

    result = _invoke(["logout"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "Logout successful" in result.output
    client.logout.assert_called_once_with()
    # The stored credentials are removed and the pins are cleared.
    assert not (tmp_path / "connection.json").exists()
    assert json.loads((tmp_path / "pinned.json").read_text()) == {}


def test_logout_surfaces_sdk_error_but_still_clears_credentials(tmp_path):
    (tmp_path / "connection.json").write_text(json.dumps({"url": "http://x"}))
    client = _client()
    client.logout.side_effect = RuntimeError("boom-xyz")

    result = _invoke(["logout"], client=client, tmp_path=tmp_path)
    # A failed server logout is a clean CLI error, not a traceback...
    assert result.exit_code != 0
    assert "boom-xyz" in result.output
    # ...but the local credentials are still removed (the finally block).
    assert not (tmp_path / "connection.json").exists()


# --- status: prints the server status -----------------------------------


def test_status_prints_server_status(tmp_path):
    client = _client()
    client.status = "SERVER-OK"  # `status` is a property on the real SDK
    result = _invoke(["status"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "Obtaining server status" in result.output
    assert "SERVER-OK" in result.output
