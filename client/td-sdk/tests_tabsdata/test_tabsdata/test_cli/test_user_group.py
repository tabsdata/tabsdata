#
# Copyright 2024 Tabs Data Inc.
#

import pytest
from click.testing import CliRunner

from tabsdata._cli.cli import cli

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


@pytest.mark.integration
@pytest.mark.requires_internet
def test_wrong_command_raises_exception(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["user", "potato"])
    assert result.exit_code == 2


@pytest.mark.integration
@pytest.mark.requires_internet
def test_user_create_prompt(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            ["user", "create", "--name", "test_user_create_prompt"],
            input=(
                "the_password\nthe_password\nthe_prompt_fullname\nprompt_email"
                "@tabsdata.com\n"
            ),
        )
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "user",
                "delete",
                "--name",
                "test_user_create_prompt",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_user_create(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "user",
                "create",
                "--name",
                "test_user_create",
                "--password",
                "the_password",
                "--full-name",
                "test_user_create_fullname",
                "--email",
                "test_user_create_email@tabsdata.com",
            ],
        )
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli, ["user", "delete", "--name", "test_user_create", "--confirm", "delete"]
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_user_create_no_prompt(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "user",
                "create",
                "--name",
                "test_user_create_no_prompt",
                "--password",
                "the_password",
            ],
        )
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "user",
                "delete",
                "--name",
                "test_user_create_no_prompt",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_user_create_no_prompt_missing_password_fails(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "--no-prompt",
                "user",
                "create",
                "--name",
                "test_user_create_no_prompt",
            ],
        )
        assert result.exit_code != 0
    finally:
        runner.invoke(
            cli,
            [
                "user",
                "delete",
                "--name",
                "test_user_create_no_prompt",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_user_delete(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "user",
                "create",
                "--name",
                "test_user_delete",
                "--password",
                "the_password",
                "--full-name",
                "test_user_delete_fullname",
                "--email",
                "test_user_delete_email@tabsdata.com",
            ],
        )
        assert result.exit_code == 0
        result = runner.invoke(
            cli, ["user", "delete", "--name", "test_user_delete", "--confirm", "delete"]
        )
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli, ["user", "delete", "--name", "test_user_delete", "--confirm", "delete"]
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_user_delete_prompt(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "user",
                "create",
                "--name",
                "test_user_delete_prompt",
                "--password",
                "the_password",
                "--full-name",
                "test_user_delete_prompt_fullname",
                "--email",
                "test_user_delete_prompt_email@tabsdata.com",
            ],
        )
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            ["user", "delete", "--name", "test_user_delete_prompt"],
            input="delete\n",
        )
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli,
            [
                "user",
                "delete",
                "--name",
                "test_user_delete_prompt",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_user_delete_error(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "user",
                "create",
                "--name",
                "test_user_delete_error",
                "--password",
                "the_password",
                "--full-name",
                "test_user_delete_error_fullname",
                "--email",
                "test_user_delete_error_email@tabsdata.com",
            ],
        )
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            ["user", "delete", "--name", "test_user_delete_error", "--confirm", "yes"],
        )
        assert result.exit_code != 0
        result = runner.invoke(
            cli, ["user", "delete", "--name", "test_user_delete_error"], input="yes\n"
        )
        assert result.exit_code != 0
        result = runner.invoke(
            cli,
            ["user", "--no-prompt", "delete", "--name", "test_user_delete_error"],
            input="delete\n",
        )
        assert result.exit_code != 0
        result = runner.invoke(
            cli,
            [
                "user",
                "delete",
                "--name",
                "test_user_delete_no_exists_raises_error",
                "--confirm",
                "delete",
            ],
        )
        assert result.exit_code != 0

    finally:
        runner.invoke(
            cli,
            [
                "user",
                "delete",
                "--name",
                "test_user_delete_error",
                "--confirm",
                "delete",
            ],
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_user_list(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["user", "list"])
    assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.requires_internet
def test_user_update(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "user",
                "create",
                "--name",
                "test_user_update",
                "--password",
                "the_password",
                "--full-name",
                "test_user_update_fullname",
                "--email",
                "test_user_update_email@tabsdata.com",
            ],
        )
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            [
                "user",
                "update",
                "--name",
                "test_user_update",
                "--full-name",
                "new_full_name",
            ],
        )
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli, ["user", "delete", "--name", "test_user_update", "--confirm", "delete"]
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_user_info(login):
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "user",
                "create",
                "--name",
                "test_user_info",
                "--password",
                "the_password",
                "--full-name",
                "test_user_info_fullname",
                "--email",
                "test_user_info_email@tabsdata.com",
            ],
        )
        assert result.exit_code == 0
        result = runner.invoke(cli, ["user", "info", "--name", "test_user_info"])
        assert result.exit_code == 0
    finally:
        runner.invoke(
            cli, ["user", "delete", "--name", "test_user_info", "--confirm", "delete"]
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_user_info_error(login):
    runner = CliRunner()
    result = runner.invoke(cli, ["user", "info", "--name", "test_user_info_error"])
    assert result.exit_code != 0
