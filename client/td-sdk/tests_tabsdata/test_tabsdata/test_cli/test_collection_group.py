#
# Copyright 2024 Tabs Data Inc.
#

import logging

import pytest
from click.testing import CliRunner

from tabsdata.cli.cli import cli

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_wrong_command_raises_exception(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["collection", "potato"])
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_create_prompt(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            ["collection", "create", "test_collection_create_prompt"],
            input="the_decription\n",
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "collection",
                "delete",
                "test_collection_create_prompt",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_create_no_prompt(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "create",
                "test_collection_create_no_prompt",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "collection",
                "delete",
                "test_collection_create_no_prompt",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_create(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "collection",
                "create",
                "test_collection_create",
                "--description",
                "test_collection_create_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "collection",
                "delete",
                "test_collection_create",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_delete_cli(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "collection",
                "create",
                "test_collection_delete_cli",
                "--description",
                "test_collection_delete_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            ["collection", "delete", "test_collection_delete_cli"],
            input="delete\n",
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "collection",
                "delete",
                "test_collection_delete_cli",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_delete_no_prompt(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "collection",
                "create",
                "test_collection_delete_no_prompt",
                "--description",
                "test_collection_delete_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "delete",
                "test_collection_delete_no_prompt",
                "--confirm",
                "delete",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "collection",
                "delete",
                "test_collection_delete_no_prompt",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_delete_wrong_options_raises_error(login):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "collection",
            "delete",
            "test_collection_delete_no_exists_raises_error",
            "--confirm",
            "delete",
        ],
    )
    assert result.exit_code != 0
    try:
        result = runner.invoke(
            cli,
            [
                "collection",
                "create",
                "test_collection_delete_raises_error",
                "--description",
                "test_collection_delete_raises_error_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            [
                "collection",
                "delete",
                "test_collection_delete_raises_error",
                "--confirm",
                "yes",
            ],
        )
        assert result.exit_code != 0
        result = runner.invoke(
            cli,
            ["collection", "delete", "test_collection_delete_raises_error"],
            input="yes\n",
        )
        assert result.exit_code != 0
        result = runner.invoke(
            cli,
            [
                "collection",
                "--no-prompt",
                "delete",
                "test_collection_delete_raises_error",
            ],
            input="delete\n",
        )
        assert result.exit_code != 0
    finally:
        runner.invoke(
            cli,
            [
                "collection",
                "delete",
                "test_collection_delete_raises_error",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_list(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["collection", "list"])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_update(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "collection",
                "create",
                "test_collection_update",
                "--description",
                "test_collection_update_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            [
                "collection",
                "update",
                "test_collection_update",
                "--description",
                "new_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            ["collection", "delete", "test_collection_update", "--confirm", "delete"],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_info(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "collection",
                "create",
                "test_collection_info",
                "--description",
                "test_collection_info_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            [
                "collection",
                "info",
                "test_collection_info",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            ["collection", "delete", "test_collection_info", "--confirm", "delete"],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_info_error(login):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "collection",
            "info",
            "test_collection_info_error",
        ],
    )
    assert result.exit_code != 0
