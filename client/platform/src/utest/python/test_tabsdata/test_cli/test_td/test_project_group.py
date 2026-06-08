#
# Copyright 2026 Tabs Data Inc.
#

import json
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

    `pin show` is the target subcommand because it touches no server --
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
        server, result = _run(["--project", "acme", "pin", "show"])
        assert result.exit_code == 0
        assert server.connection.project == "acme"

    def test_pinned_project_used_when_no_override(self):
        server, result = _run(["pin", "show"], pinned={"project": "beta"})
        assert result.exit_code == 0
        assert server.connection.project == "beta"

    def test_override_beats_pinned(self):
        server, _ = _run(
            ["--project", "acme", "pin", "show"], pinned={"project": "beta"}
        )
        assert server.connection.project == "acme"

    def test_no_project_anywhere_leaves_none(self):
        server, _ = _run(["pin", "show"], pinned={})
        assert server.connection.project is None


class TestPinSurface:
    """`td pin set/unset/show` over the persisted pinned-objects store.

    Hermetic: `DEFAULT_TABSDATA_DIRECTORY` is redirected to a tmp dir so
    `pinned.json` never touches the real `~/.tabsdata`, and the connection
    init is stubbed so the commands don't depend on login state (they never
    call the server)."""

    def _runner(self, tmp_path):
        return patch(
            "tabsdata._cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)
        ), patch("tabsdata._cli.td.cli.initialise_tabsdata_server_connection")

    def _pinned(self, tmp_path):
        return json.loads((tmp_path / "pinned.json").read_text())

    def test_set_project_then_show_then_unset(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()

            assert runner.invoke(cli, ["pin", "set", "--proj", "gamma"]).exit_code == 0
            assert self._pinned(tmp_path).get("project") == "gamma"

            shown = runner.invoke(cli, ["pin", "show"])
            assert shown.exit_code == 0, shown.output
            assert "gamma" in shown.output

            unset = runner.invoke(cli, ["pin", "unset"])
            assert unset.exit_code == 0, unset.output
            assert self._pinned(tmp_path).get("project") is None

    def test_set_with_no_flags_fails(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            result = CliRunner().invoke(cli, ["pin", "set"])
            assert result.exit_code != 0
            assert "Nothing to pin" in result.output

    def test_set_collection_blocked_without_pinned_project(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            result = CliRunner().invoke(cli, ["pin", "set", "--coll", "c1"])
            assert result.exit_code != 0
            assert "No project pinned" in result.output
            if (tmp_path / "pinned.json").exists():
                assert self._pinned(tmp_path).get("collection") is None

    def test_set_collection_allowed_with_pinned_project(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            assert runner.invoke(cli, ["pin", "set", "--proj", "gamma"]).exit_code == 0
            result = runner.invoke(cli, ["pin", "set", "--coll", "c1"])
            assert result.exit_code == 0, result.output
            assert self._pinned(tmp_path).get("collection") == "c1"

    def test_set_project_and_collection_together(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            result = CliRunner().invoke(
                cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"]
            )
            assert result.exit_code == 0, result.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "gamma"
            assert pinned.get("collection") == "c1"

    def test_unset_coll_clears_only_collection(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            assert (
                runner.invoke(
                    cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"]
                ).exit_code
                == 0
            )
            unset = runner.invoke(cli, ["pin", "unset", "--coll"])
            assert unset.exit_code == 0, unset.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "gamma"
            assert pinned.get("collection") is None

    def test_unset_proj_clears_both(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            assert (
                runner.invoke(
                    cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"]
                ).exit_code
                == 0
            )
            unset = runner.invoke(cli, ["pin", "unset", "--proj"])
            assert unset.exit_code == 0, unset.output
            assert "c1" in unset.output  # cascade surfaced
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") is None
            assert pinned.get("collection") is None

    def test_bare_unset_clears_both(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            assert (
                runner.invoke(
                    cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"]
                ).exit_code
                == 0
            )
            unset = runner.invoke(cli, ["pin", "unset"])
            assert unset.exit_code == 0, unset.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") is None
            assert pinned.get("collection") is None

    def test_switching_project_clears_collection(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            assert (
                runner.invoke(
                    cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"]
                ).exit_code
                == 0
            )
            switched = runner.invoke(cli, ["pin", "set", "--proj", "delta"])
            assert switched.exit_code == 0, switched.output
            assert "c1" in switched.output  # cascade surfaced
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "delta"
            assert pinned.get("collection") is None

    def test_repinning_same_project_keeps_collection(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            assert (
                runner.invoke(
                    cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"]
                ).exit_code
                == 0
            )
            again = runner.invoke(cli, ["pin", "set", "--proj", "gamma"])
            assert again.exit_code == 0, again.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "gamma"
            assert pinned.get("collection") == "c1"
