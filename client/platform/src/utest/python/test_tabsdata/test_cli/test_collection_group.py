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


@pytest.mark.integration
@pytest.mark.requires_internet
def test_wrong_command_raises_exception(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "collection", "potato"])
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_create_prompt(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            ["collection", "create", "--name", "test_collection_create_prompt"],
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
                "--name",
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
                "--name",
                "test_collection_create_no_prompt",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "delete",
                "--name",
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
                "--no-prompt",
                "collection",
                "create",
                "--name",
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
                "--no-prompt",
                "collection",
                "delete",
                "--name",
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
                "--no-prompt",
                "collection",
                "create",
                "--name",
                "test_collection_delete_cli",
                "--description",
                "test_collection_delete_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            ["collection", "delete", "--name", "test_collection_delete_cli"],
            input="delete\n",
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "delete",
                "--name",
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
                "--no-prompt",
                "collection",
                "create",
                "--name",
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
                "--name",
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
                "--no-prompt",
                "collection",
                "delete",
                "--name",
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
            "--no-prompt",
            "collection",
            "delete",
            "--name",
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
                "--no-prompt",
                "collection",
                "create",
                "--name",
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
                "--no-prompt",
                "collection",
                "delete",
                "--name",
                "test_collection_delete_raises_error",
                "--confirm",
                "yes",
            ],
        )
        assert result.exit_code != 0
        result = runner.invoke(
            cli,
            ["collection", "delete", "--name", "test_collection_delete_raises_error"],
            input="yes\n",
        )
        assert result.exit_code != 0
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "delete",
                "--name",
                "test_collection_delete_raises_error",
            ],
            input="delete\n",
        )
        assert result.exit_code != 0
    finally:
        runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "delete",
                "--name",
                "test_collection_delete_raises_error",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_list(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "collection", "list"])
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
                "--no-prompt",
                "collection",
                "create",
                "--name",
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
                "--no-prompt",
                "collection",
                "update",
                "--name",
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
            [
                "--no-prompt",
                "collection",
                "delete",
                "--name",
                "test_collection_update",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_info(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "create",
                "--name",
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
                "--no-prompt",
                "collection",
                "info",
                "--name",
                "test_collection_info",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "delete",
                "--name",
                "test_collection_info",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_info_error(login):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "collection",
            "info",
            "--name",
            "test_collection_info_error",
        ],
    )
    assert result.exit_code != 0


@pytest.mark.integration
def test_coll_cli_inter_coll_perm_list(tabsserver_connection, login):
    coll_a_name = "test_coll_cli_inter_coll_perm_list_coll_a"
    coll_b_name = "test_coll_cli_inter_coll_perm_list_coll_b"
    tabsserver_connection.delete_collection(coll_a_name, raise_for_status=False)
    tabsserver_connection.delete_collection(coll_b_name, raise_for_status=False)
    try:
        coll_a = tabsserver_connection.create_collection(coll_a_name)
        tabsserver_connection.create_collection(coll_b_name)
        coll_a.create_permission(coll_b_name)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "list-perm",
                "--name",
                coll_a_name,
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        tabsserver_connection.delete_collection(coll_a_name, raise_for_status=False)
        tabsserver_connection.delete_collection(coll_b_name, raise_for_status=False)


@pytest.mark.integration
def test_coll_cli_inter_coll_perm_create(tabsserver_connection, login):
    coll_a_name = "test_coll_cli_inter_coll_perm_create_coll_a"
    coll_b_name = "test_coll_cli_inter_coll_perm_create_coll_b"
    tabsserver_connection.delete_collection(coll_a_name, raise_for_status=False)
    tabsserver_connection.delete_collection(coll_b_name, raise_for_status=False)
    try:
        coll_a = tabsserver_connection.create_collection(coll_a_name)
        tabsserver_connection.create_collection(coll_b_name)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "add-perm",
                "--name",
                coll_a_name,
                "--to-coll",
                coll_b_name,
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        listed_permissions = coll_a.permissions
        assert listed_permissions
        assert listed_permissions[0].to_collection.name == coll_b_name
    finally:
        tabsserver_connection.delete_collection(coll_a_name, raise_for_status=False)
        tabsserver_connection.delete_collection(coll_b_name, raise_for_status=False)


@pytest.mark.integration
def test_coll_cli_inter_coll_perm_delete(tabsserver_connection, login):
    coll_a_name = "test_coll_cli_inter_coll_perm_delete_coll_a"
    coll_b_name = "test_coll_cli_inter_coll_perm_delete_coll_b"
    tabsserver_connection.delete_collection(coll_a_name, raise_for_status=False)
    tabsserver_connection.delete_collection(coll_b_name, raise_for_status=False)
    try:
        coll_a = tabsserver_connection.create_collection(coll_a_name)
        coll_b = tabsserver_connection.create_collection(coll_b_name)
        perm = coll_a.create_permission(coll_b)
        listed_permissions = coll_a.permissions
        assert perm in listed_permissions
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "delete-perm",
                "--name",
                coll_a_name,
                "--to-coll",
                coll_b_name,
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        assert perm not in coll_a.permissions
    finally:
        tabsserver_connection.delete_collection(coll_a_name, raise_for_status=False)
        tabsserver_connection.delete_collection(coll_b_name, raise_for_status=False)


@pytest.mark.integration
def test_coll_cli_inter_coll_perm_delete_with_id(tabsserver_connection, login):
    coll_a_name = "test_coll_cli_inter_coll_perm_delete_with_id_coll_a"
    coll_b_name = "test_coll_cli_inter_coll_perm_delete_with_id_coll_b"
    tabsserver_connection.delete_collection(coll_a_name, raise_for_status=False)
    tabsserver_connection.delete_collection(coll_b_name, raise_for_status=False)
    try:
        coll_a = tabsserver_connection.create_collection(coll_a_name)
        coll_b = tabsserver_connection.create_collection(coll_b_name)
        perm = coll_a.create_permission(coll_b)
        listed_permissions = coll_a.permissions
        assert perm in listed_permissions
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "collection",
                "delete-perm",
                "--name",
                coll_a_name,
                "--id",
                perm.id,
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        assert perm not in coll_a.permissions
    finally:
        tabsserver_connection.delete_collection(coll_a_name, raise_for_status=False)
        tabsserver_connection.delete_collection(coll_b_name, raise_for_status=False)
