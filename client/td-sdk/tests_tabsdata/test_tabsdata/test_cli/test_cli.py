#
# Copyright 2024 Tabs Data Inc.
#

import logging

import pytest
from click.testing import CliRunner

from tabsdata._cli.cli import cli

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def test_cli():
    runner = CliRunner()
    result = runner.invoke(
        cli,
    )
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
def test_status(login):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["status"],
    )
    assert result.exit_code == 0


def test_examples_existing_folder_fails(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["examples", "--dir", tmp_path],
    )
    logger.debug(result.output)
    assert result.exit_code != 0


def test_info_with_license():
    runner = CliRunner()
    result = runner.invoke(cli, ["info", "--license"])
    logger.debug(result.output)
    assert result.exit_code == 0


def test_info_with_third_party():
    runner = CliRunner()
    result = runner.invoke(cli, ["info", "--third-party"])
    logger.debug(result.output)
    assert result.exit_code == 0


def test_info_with_release_notes_party():
    runner = CliRunner()
    result = runner.invoke(cli, ["info", "--release-notes"])
    logger.debug(result.output)
    assert result.exit_code == 0


def test_info_with_all_options():
    runner = CliRunner()
    result = runner.invoke(
        cli, ["info", "--license", "--third-party", "--release-notes"]
    )
    logger.debug(result.output)
    assert result.exit_code == 0
