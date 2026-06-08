#
# Copyright 2026 Tabs Data Inc.
#

import logging
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from tabsdata._cli.td.cli import cli

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)


def _run(args, pinned=None):
    """Invoke the CLI with a mocked (server-free) connection and a known
    pinned-objects dict, returning the mock server so a test can inspect what
    `cli()` stamped onto `connection.project`.

    `project pinned` is the target subcommand because it touches no server --
    cli()'s group callback (which does the project resolution) still runs
    first, which is the behavior under test.
    """
    server = MagicMock()

    def _init(ctx):
        ctx.obj["tabsdataserver"] = server

    def _load(ctx):
        ctx.obj["pinned_objects"] = pinned or {}

    with patch(
        "tabsdata._cli.td.cli.initialise_tabsdata_server_connection",
        side_effect=_init,
    ), patch(
        "tabsdata._cli.td.cli.load_pinned_objects", side_effect=_load
    ):
        result = CliRunner().invoke(cli, args)
    logger.debug(result.output)
    return server, result


class TestProjectResolution:
    """`cli()` puts the active project on the connection: the --project
    override wins, else the pinned project, else None (no CLI-side default --
    any defaulting is the server's job)."""

    def test_override_sets_connection_project(self):
        server, result = _run(["--project", "acme", "project", "pinned"])
        assert result.exit_code == 0
        assert server.connection.project == "acme"

    def test_pinned_project_used_when_no_override(self):
        server, result = _run(["project", "pinned"], pinned={"project": "beta"})
        assert result.exit_code == 0
        assert server.connection.project == "beta"

    def test_override_beats_pinned(self):
        server, _ = _run(
            ["--project", "acme", "project", "pinned"], pinned={"project": "beta"}
        )
        assert server.connection.project == "acme"

    def test_no_project_anywhere_leaves_none(self):
        server, _ = _run(["project", "pinned"], pinned={})
        assert server.connection.project is None


class TestProjectPinPersistence:
    """pin writes the project; pinned reads it back; unpin clears it.

    Hermetic: `DEFAULT_TABSDATA_DIRECTORY` is redirected to a tmp dir so
    `pinned.json` never touches the real `~/.tabsdata`, and the connection
    init is stubbed so pin/unpin/pinned don't depend on login state (they
    never call the server).
    """

    def test_pin_then_pinned_then_unpin(self, tmp_path):
        with patch(
            "tabsdata._cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)
        ), patch("tabsdata._cli.td.cli.initialise_tabsdata_server_connection"):
            runner = CliRunner()

            pinned = runner.invoke(cli, ["project", "pin", "-n", "gamma"])
            assert pinned.exit_code == 0, pinned.output

            shown = runner.invoke(cli, ["project", "pinned"])
            assert shown.exit_code == 0, shown.output
            assert "gamma" in shown.output

            unpinned = runner.invoke(cli, ["project", "unpin"])
            assert unpinned.exit_code == 0, unpinned.output

            shown_again = runner.invoke(cli, ["project", "pinned"])
            assert "No project is currently pinned" in shown_again.output
