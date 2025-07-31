#
# Copyright 2025 Tabs Data Inc.
#

import logging

import pytest
from click.testing import CliRunner

from tabsdata._cli.cli import cli

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_wrong_command_raises_exception(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "potato"])
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
def test_auth_info(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "info"])
    logger.debug(result.output)
    assert result.exit_code == 0
