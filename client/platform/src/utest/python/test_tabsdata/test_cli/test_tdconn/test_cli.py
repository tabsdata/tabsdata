#
# Copyright 2026 Tabs Data Inc.
#
"""End-to-end tests for the ``tdconn`` CLI (CRUD operations).

Tests drive the Click CLI via :class:`click.testing.CliRunner` against the
``api_server`` fake (in-memory collection-connection endpoints) and assert
on exit codes and captured output.  ``HOME`` is redirected via the
``isolated_home`` fixture so nothing leaks outside the test's temp
directory.
"""
from __future__ import annotations

import logging
from pathlib import Path

from click.testing import CliRunner

from tabsdata._cli.tdconn.cli import cli

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def test_add_and_list(isolated_home, conn_file, api_server):
    runner = CliRunner()
    src = conn_file(name="finance")

    result = runner.invoke(cli, ["add", "-f", str(src)])
    logger.debug(result.output)
    assert result.exit_code == 0
    assert "Added" in result.output and "'finance'" in result.output
    # ``add`` runs an explicit live probe; StubConn has no registered
    # extractor so the footer reflects the failure rather than lying
    # about a success we can't prove.
    assert "connectivity failed" in result.output

    result = runner.invoke(cli, ["list"])
    logger.debug(result.output)
    assert result.exit_code == 0
    # list renders a table with name/type/mode rather than one name per line.
    assert "finance" in result.output
    assert "StubConn" in result.output
    assert "src" in result.output


def test_add_duplicate_errors(isolated_home, conn_file, api_server):
    runner = CliRunner()
    src = conn_file(name="finance")

    result = runner.invoke(cli, ["add", "-f", str(src)])
    assert result.exit_code == 0

    result = runner.invoke(cli, ["add", "-f", str(src)])
    assert result.exit_code != 0
    assert "already exists" in result.output


def test_info_prints_identity(isolated_home, conn_file, api_server):
    runner = CliRunner()
    runner.invoke(cli, ["add", "-f", str(conn_file(name="finance"))])

    result = runner.invoke(cli, ["info", "--name", "finance"])
    logger.debug(result.output)
    assert result.exit_code == 0
    # info() prints the name/type/mode wireframe, no verify footer.
    assert "finance" in result.output
    assert "StubConn" in result.output
    assert "src" in result.output
    assert "connectivity verified" not in result.output
    # --details not passed -> no JSON body.
    assert '"conn_configs"' not in result.output


def test_info_with_details_prints_full_json(isolated_home, conn_file, api_server):
    runner = CliRunner()
    runner.invoke(cli, ["add", "-f", str(conn_file(name="finance"))])

    result = runner.invoke(cli, ["info", "--name", "finance", "--details"])
    logger.debug(result.output)
    assert result.exit_code == 0
    # Identity still rendered ...
    assert "finance" in result.output
    assert "StubConn" in result.output
    # ... plus the stored JSON body beneath it.
    assert '"conn_configs"' in result.output
    assert '"uri": "stub://host/db"' in result.output


def test_info_short_details_flag_is_d(isolated_home, conn_file, api_server):
    runner = CliRunner()
    runner.invoke(cli, ["add", "-f", str(conn_file(name="finance"))])

    result = runner.invoke(cli, ["info", "-n", "finance", "-d"])
    logger.debug(result.output)
    assert result.exit_code == 0
    assert '"conn_configs"' in result.output


def test_list_with_verify_adds_verified_column(isolated_home, conn_file, api_server):
    runner = CliRunner()
    # StubConn has no registered extractor, so logic.verify will raise -- the
    # list output should still render all rows and mark them not verified.
    runner.invoke(cli, ["add", "-f", str(conn_file(name="finance"))])
    runner.invoke(cli, ["add", "-f", str(conn_file(name="sales"))])

    result = runner.invoke(cli, ["list", "-V"])
    logger.debug(result.output)
    assert result.exit_code == 0
    assert "Verified" in result.output
    assert "finance" in result.output
    assert "sales" in result.output
    # No extractor is registered for StubConn -> both rows are marked not
    # verified, and the summary reflects 0/2.
    assert "verified 0 / 2" in result.output


def test_update(isolated_home, conn_file, api_server):
    runner = CliRunner()
    runner.invoke(cli, ["add", "-f", str(conn_file(name="finance", description="v1"))])

    v2 = conn_file(name="finance", description="v2")
    result = runner.invoke(cli, ["update", "--name", "finance", "-f", str(v2)])
    logger.debug(result.output)
    assert result.exit_code == 0
    assert "Updated" in result.output
    assert "'finance'" in result.output
    # StubConn has no registered extractor; the footer surfaces the
    # live probe's real outcome instead of a blanket ``verified``.
    assert "connectivity failed" in result.output


def test_add_reaches_the_server_store(isolated_home, conn_file, api_server):
    runner = CliRunner()

    result = runner.invoke(cli, ["add", "-f", str(conn_file(name="finance"))])
    logger.debug(result.output)
    assert result.exit_code == 0
    # The document landed server-side, keyed by the connection name; no
    # instance file was written (connections live behind the apiserver now).
    assert "finance" in api_server.documents
    assert not (isolated_home / ".tabsdata" / "instances").exists()


def test_long_file_flag_still_works(isolated_home, conn_file, api_server):
    runner = CliRunner()
    src = conn_file(name="finance")
    result = runner.invoke(cli, ["add", "--file", str(src)])
    assert result.exit_code == 0


def test_short_name_flag_works(isolated_home, conn_file, api_server):
    runner = CliRunner()
    runner.invoke(cli, ["add", "-f", str(conn_file(name="finance"))])

    result = runner.invoke(cli, ["info", "-n", "finance"])
    logger.debug(result.output)
    assert result.exit_code == 0
    assert "finance" in result.output
    assert "StubConn" in result.output


def test_add_from_stdin(isolated_home, conn_file, api_server):
    runner = CliRunner()
    src = conn_file(name="stdin_conn")
    stdin_content = src.read_text(encoding="utf-8")

    result = runner.invoke(cli, ["add", "-f", "-"], input=stdin_content)
    logger.debug(result.output)
    assert result.exit_code == 0
    assert "Added" in result.output
    assert "'stdin_conn'" in result.output


def test_update_from_stdin(isolated_home, conn_file, api_server):
    runner = CliRunner()
    runner.invoke(
        cli, ["add", "-f", str(conn_file(name="finance", description="v1"))]
    )

    v2 = conn_file(name="finance", description="v2")
    stdin_content = v2.read_text(encoding="utf-8")

    result = runner.invoke(
        cli, ["update", "-n", "finance", "-f", "-"], input=stdin_content
    )
    logger.debug(result.output)
    assert result.exit_code == 0
    assert "Updated" in result.output
    assert "'finance'" in result.output
