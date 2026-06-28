#
# Copyright 2024 Tabs Data Inc.
#

import logging

import pytest
from click.testing import CliRunner

from tabsdata._cli.td import collection_group
from tabsdata._cli.td.cli import cli

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
@pytest.mark.requires_internet
def test_collection_create_source_with_conn_file(login, tmp_path):
    conn_file = tmp_path / "conn.yaml"
    conn_file.write_text(
        "kind: mysqlSrcConn\n"
        "apiVersion: '1.0'\n"
        "type: tabsdatak.conn.mysql:MySQLSrcConn\n"
        "spec:\n"
        "  uri: mysql://127.0.0.1:3306/db\n"
        "  credentials:\n"
        "    kind: userPasswordCredentials\n"
        "    apiVersion: '1.0'\n"
        "    type: tabsdatak.conn.common.types:UserPassword\n"
        "    spec:\n"
        "      user: secret:u\n"
        "      password: secret:p\n"
    )
    runner = CliRunner()
    try:
        result = runner.invoke(
            cli,
            [
                "collection",
                "create",
                "--name",
                "test_create_source_conn_file",
                "--description",
                "source via conn file",
                "--conn-file",
                str(conn_file),
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
                "--name",
                "test_create_source_conn_file",
                "--confirm",
                "delete",
            ],
        )


def _installed(type_: str, direction: str) -> bool:
    # noinspection PyProtectedMember
    from tabsdatak import _template as template

    try:
        template.resolve_conn(type_, direction)
        return True
    except template.TemplateError:
        return False


_MYSQL_SRC_YAML = """\
kind: mysqlSrcConn
apiVersion: '1.0'
type: tabsdatak.conn.mysql:MySQLSrcConn
spec:
  uri: mysql://h:3306/db
  credentials:
    kind: userPasswordCredentials
    apiVersion: '1.0'
    type: tabsdatak.conn.common.types:UserPassword
    spec:
      user: secret:etl
      password: secret:hunter2
"""


def test_group_for_kind():
    assert collection_group._group_for_kind("mysqlSrcConn") == "sources"
    assert collection_group._group_for_kind("mysqlDestConn") == "destinations"
    import click

    with pytest.raises(click.ClickException):
        collection_group._group_for_kind("notAConn")


@pytest.mark.skipif(
    not _installed("mysql", "src"), reason="mysql connector not installed"
)
def test_load_connection_document_dehydrates(tmp_path):
    # noinspection PyProtectedMember
    from tabsdatak import _template as template

    path = tmp_path / "conn.yaml"
    path.write_text(_MYSQL_SRC_YAML)
    connection, group, secrets = collection_group._load_connection_document(
        str(path), None
    )
    assert group == "sources"
    assert {kv["value"] for kv in secrets} == {"etl", "hunter2"}
    sent = template.from_yaml(connection)
    creds = sent["spec"]["credentials"]["spec"]
    assert creds["user"] == ":secret:"
    assert creds["password"] == ":secret:"
    assert sent["spec"]["uri"] == "mysql://h:3306/db"


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_create_source_without_conn_file_fails(login):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "collection",
            "create",
            "--name",
            "test_create_source_no_conn",
            "--description",
            "d",
            "--group",
            "sources",
        ],
    )
    assert result.exit_code != 0
    assert "conn-file" in result.output
