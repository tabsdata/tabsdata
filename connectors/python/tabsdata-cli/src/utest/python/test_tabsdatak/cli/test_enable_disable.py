#
#  Copyright 2026 Tabsdata Inc.
#

"""Enabling an enabled function (or disabling a disabled one) is a no-op.

``tdk fn enable|disable`` and its two restricted forms, ``tdk cron`` and
``tdk stream``, share one implementation: it reads the function's current state
and only calls the server when the state would actually change, so a repeated
enable/disable reports the state instead of writing it again.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer

GROUPS = ["fn", "cron", "stream"]


def _client(*, enabled: bool | None) -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "proj-a"
    client.get_function.return_value = MagicMock(enabled=enabled)
    # The cron/stream commands check for a trigger worker before touching the
    # function; give them one so only the already-in-state check can stop them.
    client.list_workers_generator.side_effect = lambda *a, **k: iter([MagicMock()])
    return client


def _invoke(argv, *, client, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv)


@pytest.mark.parametrize("group", GROUPS)
@pytest.mark.parametrize(
    "action,enabled,state",
    [("enable", True, "enabled"), ("disable", False, "disabled")],
)
def test_already_in_state_is_a_no_op(group, action, enabled, state, tmp_path):
    client = _client(enabled=enabled)
    result = _invoke(
        [group, action, "-n", "f1", "-c", "coll1"], client=client, tmp_path=tmp_path
    )
    # A no-op is success, and it says which state the function is already in.
    assert result.exit_code == 0, result.output
    assert f"Function 'f1' in collection 'coll1' is already {state}" in result.output
    client.enable_function.assert_not_called()


@pytest.mark.parametrize("group", GROUPS)
@pytest.mark.parametrize(
    "action,enabled",
    [("enable", True), ("disable", False)],
)
def test_state_change_still_calls_the_server(group, action, enabled, tmp_path):
    client = _client(enabled=not enabled)
    result = _invoke(
        [group, action, "-n", "f1", "-c", "coll1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert "Function updated successfully" in result.output
    client.enable_function.assert_called_once_with(
        collection_name="coll1", function_name="f1", enabled=enabled
    )


@pytest.mark.parametrize("group", GROUPS)
def test_unreadable_state_is_a_clean_error(group, tmp_path):
    # The state read is what fails for a function that does not exist.
    client = _client(enabled=None)
    client.get_function.side_effect = RuntimeError("boom-xyz")
    result = _invoke(
        [group, "enable", "-n", "f1", "-c", "coll1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code != 0
    assert "boom-xyz" in result.output
    client.enable_function.assert_not_called()
