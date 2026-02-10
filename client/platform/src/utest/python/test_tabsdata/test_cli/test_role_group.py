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
    result = runner.invoke(cli, ["--no-prompt", "role", "potato"])
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_create_prompt(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            ["role", "create", "--name", "test_role_create_prompt"],
            input="the_description\n",
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "role",
                "delete",
                "--name",
                "test_role_create_prompt",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_create_no_prompt(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "create",
                "--name",
                "test_role_create_no_prompt",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "delete",
                "--name",
                "test_role_create_no_prompt",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_delete_cli(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "create",
                "--name",
                "test_role_delete_cli",
                "--description",
                "test_role_delete_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            ["role", "delete", "--name", "test_role_delete_cli"],
            input="delete\n",
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "delete",
                "--name",
                "test_role_delete_cli",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_delete_no_prompt(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "create",
                "--name",
                "test_role_delete_no_prompt",
                "--description",
                "test_role_delete_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "delete",
                "--name",
                "test_role_delete_no_prompt",
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
                "role",
                "delete",
                "--name",
                "test_role_delete_no_prompt",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_delete_wrong_options_raises_error(login):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "role",
            "delete",
            "--name",
            "test_role_delete_no_exists_raises_error",
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
                "role",
                "create",
                "--name",
                "test_role_delete_raises_error",
                "--description",
                "test_role_delete_raises_error_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "delete",
                "--name",
                "test_role_delete_raises_error",
                "--confirm",
                "yes",
            ],
        )
        assert result.exit_code != 0
        result = runner.invoke(
            cli,
            ["role", "delete", "--name", "test_role_delete_raises_error"],
            input="yes\n",
        )
        assert result.exit_code != 0
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "delete",
                "--name",
                "test_role_delete_raises_error",
            ],
            input="delete\n",
        )
        assert result.exit_code != 0
    finally:
        runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "delete",
                "--name",
                "test_role_delete_raises_error",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_list(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["--no-prompt", "role", "list"])
    logger.debug(result.output)
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_update(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "create",
                "--name",
                "test_role_update",
                "--description",
                "test_role_update_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "update",
                "--name",
                "test_role_update",
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
                "role",
                "delete",
                "--name",
                "test_role_update",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_info(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "create",
                "--name",
                "test_role_info",
                "--description",
                "test_role_info_description",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "info",
                "--name",
                "test_role_info",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "delete",
                "--name",
                "test_role_info",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_info_error(login):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--no-prompt",
            "role",
            "info",
            "--name",
            "test_role_info_error",
        ],
    )
    assert result.exit_code != 0


@pytest.mark.integration
def test_role_cli_perm_list(tabsserver_connection, login):
    role_name = "test_role_cli_perm_list"
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = tabsserver_connection.create_role(role_name)
        role.create_permission("sa")
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "list-perm",
                "--name",
                role_name,
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_cli_perm_add(tabsserver_connection, login):
    role_name = "test_role_cli_perm_add"
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = tabsserver_connection.create_role(role_name)
        assert not role.permissions
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "add-perm",
                "--name",
                role_name,
                "--perm",
                "sa",
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        assert role.permissions
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_cli_perm_delete(tabsserver_connection, login):
    role_name = "test_role_cli_perm_delete"
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    try:
        role = tabsserver_connection.create_role(role_name)
        perm = role.create_permission("sa")
        listed_permissions = role.permissions
        assert perm in listed_permissions
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "delete-perm",
                "--name",
                role_name,
                "--id",
                perm.id,
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        assert perm not in role.permissions
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)


@pytest.mark.integration
def test_role_cli_user_list(tabsserver_connection, login):
    role_name = "test_role_cli_user_list"
    user_name = "test_role_cli_user_list_user"
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    tabsserver_connection.delete_user(user_name, raise_for_status=False)
    try:
        role = tabsserver_connection.create_role(role_name)
        user = tabsserver_connection.create_user(
            user_name, "test_role_cli_user_list_password"
        )
        role.add_user(user)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "list-user",
                "--name",
                role_name,
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)
        tabsserver_connection.delete_user(user_name, raise_for_status=False)


@pytest.mark.integration
def test_role_cli_user_add(tabsserver_connection, login):
    role_name = "test_role_cli_user_add"
    user_name = "test_role_cli_user_add_user"
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    tabsserver_connection.delete_user(user_name, raise_for_status=False)
    try:
        role = tabsserver_connection.create_role(role_name)
        tabsserver_connection.create_user(user_name, "test_role_cli_user_add_password")
        assert not role.users
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "add-user",
                "--name",
                role_name,
                "--user",
                user_name,
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        assert role.users
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)
        tabsserver_connection.delete_user(user_name, raise_for_status=False)


@pytest.mark.integration
def test_role_cli_user_delete(tabsserver_connection, login):
    role_name = "test_role_cli_user_delete"
    user_name = "test_role_cli_user_delete_user"
    tabsserver_connection.delete_role(role_name, raise_for_status=False)
    tabsserver_connection.delete_user(user_name, raise_for_status=False)
    try:
        role = tabsserver_connection.create_role(role_name)
        user = tabsserver_connection.create_user(
            user_name, "test_role_cli_user_delete_password"
        )
        role.add_user(user)
        listed_users = role.users
        assert user in listed_users
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "role",
                "delete-user",
                "--name",
                role_name,
                "--user",
                user_name,
            ],
        )
        logger.debug(result.output)
        assert result.exit_code == 0
        assert user not in role.users
    finally:
        tabsserver_connection.delete_role(role_name, raise_for_status=False)
        tabsserver_connection.delete_user(user_name, raise_for_status=False)
