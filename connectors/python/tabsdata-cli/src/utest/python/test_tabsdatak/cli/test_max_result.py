#
#  Copyright 2026 Tabsdata Inc.
#

"""``--max-result`` behaviour for the ``tdk`` list commands.

Every standalone list command caps its listing at ``--max-result`` rows
(default 100) and says so when the server holds more. The cap is enforced by
consuming the SDK's ``*_generator`` twin, so the command must stop pulling
pages once it has enough -- these tests assert both the visible contract (rows
shown, notice emitted) and the invisible one (the generator is not drained).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.cli_utils import DEFAULT_MAX_RESULT
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


class _User:
    def __init__(self, index: int):
        self.name = f"user-{index:04d}"
        self.full_name = f"User {index}"
        self.email = f"user-{index}@x.io"
        self.enabled = True
        self._data = {"name": self.name, "email": self.email}


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "proj-a"
    return client


def _counting_users(total: int) -> tuple[MagicMock, list[int]]:
    """A client whose user listing yields ``total`` rows, counting each one
    pulled so a test can prove the command stopped early."""
    client = _client()
    pulled = []

    def _generator(*_args, **_kwargs):
        for index in range(total):
            pulled.append(index)
            yield _User(index)

    client.list_users_generator.side_effect = _generator
    return client, pulled


def _invoke(argv: list[str], *, client: MagicMock, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv, env={"COLUMNS": "200"})


MORE_RESULTS = "More results available"


def test_default_cap_applies_and_announces_more(tmp_path):
    client, pulled = _counting_users(250)
    result = _invoke(["user", "list"], client=client, tmp_path=tmp_path)

    assert result.exit_code == 0, result.output
    assert f"Number of users: {DEFAULT_MAX_RESULT}" in result.output
    assert MORE_RESULTS in result.output
    # One row beyond the cap is pulled to detect the overflow, and no more:
    # the remaining pages are never fetched.
    assert len(pulled) == DEFAULT_MAX_RESULT + 1


def test_explicit_cap_applies(tmp_path):
    client, pulled = _counting_users(250)
    result = _invoke(
        ["user", "list", "--max-result", "5"], client=client, tmp_path=tmp_path
    )

    assert result.exit_code == 0, result.output
    assert "Number of users: 5" in result.output
    assert "user-0004" in result.output
    assert "user-0005" not in result.output
    assert MORE_RESULTS in result.output
    assert len(pulled) == 6


def test_no_notice_when_listing_fits_exactly(tmp_path):
    client, pulled = _counting_users(5)
    result = _invoke(
        ["user", "list", "--max-result", "5"], client=client, tmp_path=tmp_path
    )

    assert result.exit_code == 0, result.output
    assert "Number of users: 5" in result.output
    assert MORE_RESULTS not in result.output
    assert len(pulled) == 5


def test_no_notice_when_listing_is_short(tmp_path):
    client, _ = _counting_users(2)
    result = _invoke(
        ["user", "list", "--max-result", "5"], client=client, tmp_path=tmp_path
    )

    assert result.exit_code == 0, result.output
    assert "Number of users: 2" in result.output
    assert MORE_RESULTS not in result.output


def test_zero_means_unlimited(tmp_path):
    client, pulled = _counting_users(150)
    result = _invoke(
        ["user", "list", "--max-result", "0"], client=client, tmp_path=tmp_path
    )

    assert result.exit_code == 0, result.output
    assert "Number of users: 150" in result.output
    assert MORE_RESULTS not in result.output
    assert len(pulled) == 150


def test_negative_cap_rejected(tmp_path):
    client, _ = _counting_users(10)
    result = _invoke(
        ["user", "list", "--max-result", "-1"], client=client, tmp_path=tmp_path
    )

    assert result.exit_code != 0


def test_json_stays_parseable_and_notice_goes_to_stderr(tmp_path):
    client, _ = _counting_users(250)
    result = _invoke(
        ["user", "list", "-o", "json", "--max-result", "3"],
        client=client,
        tmp_path=tmp_path,
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert [row["name"] for row in payload] == ["user-0000", "user-0001", "user-0002"]
    assert MORE_RESULTS not in result.stdout
    assert MORE_RESULTS in result.stderr


# The ``role`` group also carries the option on its three list commands, but it
# is not registered on the CLI right now (see the TODO in tabsdatak.cli.td.cli),
# so it cannot be reached through an invocation.
@pytest.mark.parametrize(
    "argv",
    [
        ["user", "list"],
        ["group", "list"],
        ["project", "list"],
        ["collection", "list"],
        ["fn", "list", "-c", "coll-a"],
        ["fn", "patches", "-n", "fn-a", "-c", "coll-a"],
        ["table", "list", "-c", "coll-a"],
        ["table", "versions", "-c", "coll-a", "--name", "tbl-a"],
        ["plan", "list"],
        ["trx", "list"],
        ["fnrun", "list"],
        ["worker", "list"],
    ],
)
def test_every_list_command_offers_the_option(argv, tmp_path):
    client = _client()
    result = _invoke([*argv, "--help"], client=client, tmp_path=tmp_path)

    assert result.exit_code == 0, result.output
    assert "--max-result" in result.output


@pytest.mark.parametrize("command", ["list", "list_perm", "list_user"])
def test_role_list_commands_offer_the_option(command):
    from tabsdatak.cli.td import role_group

    params = getattr(role_group, command).params
    assert any(p.name == "max_result" for p in params)
