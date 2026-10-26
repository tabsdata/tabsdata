#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    format_option,
    hint_common_solutions,
    logical_prompt,
    max_result_option,
    present,
    render,
    take,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer

SECRETS_FILE_HELP = (
    "YAML file with the secrets of the project, or '-' for stdin. Maps each "
    "secret to its keys and each key to its value. A value '$secret:VARIABLE' "
    "is read from that environment variable; a key with no value keeps the "
    "value already stored (empty if there is none)."
)
NULL_TAG = "tag:yaml.org,2002:null"


@cli_command
@click.group()
def project():
    """Project management commands"""


def _load_secrets_file(secrets_file: str) -> dict[str, dict[str, str | None]]:
    from ruamel.yaml import YAML
    from ruamel.yaml.nodes import MappingNode, ScalarNode

    try:
        raw = (
            sys.stdin.read()
            if secrets_file == "-"
            else Path(secrets_file).read_text(encoding="utf-8")
        )
    except OSError as error:
        raise click.ClickException(f"cannot read secrets file: {error}")
    try:
        document = YAML().compose(raw)
    except Exception as error:  # noqa: BLE001 -- any YAML parse failure
        raise click.ClickException(f"invalid secrets YAML: {error}")
    if document is None:
        return {}
    if not isinstance(document, MappingNode):
        raise click.ClickException("the secrets file must map each secret to its keys")
    variables: set[str] = set()
    secrets: dict[str, dict[str, str | None]] = {}
    for name_node, entries_node in document.value:
        name = name_node.value
        if not isinstance(entries_node, MappingNode):
            raise click.ClickException(
                f"the secret '{name}' must map each of its keys to a value"
            )
        if name.lower() in (known.lower() for known in secrets):
            raise click.ClickException(f"the secret '{name}' is given more than once")
        entries: dict[str, str | None] = {}
        for key_node, value_node in entries_node.value:
            key = key_node.value
            if not isinstance(value_node, ScalarNode):
                raise click.ClickException(
                    f"the value of '{name}.{key}' must be a single value"
                )
            if key.lower() in (known.lower() for known in entries):
                raise click.ClickException(
                    f"the key '{key}' of the secret '{name}' is given more than once"
                )
            entries[key] = _secret_value(name, key, value_node, variables)
        secrets[name] = entries
    if variables:
        click.echo(
            "Project secrets read from environment variable(s): "
            + ", ".join(sorted(variables))
        )
    return secrets


def _secret_value(name: str, key: str, node, variables: set[str]) -> str | None:
    from tabsdatak.common._json_serde import _SECRET_ENV_REF_PREFIX

    if getattr(node.tag, "value", node.tag) == NULL_TAG:
        return None
    value = node.value
    if not value.startswith(_SECRET_ENV_REF_PREFIX):
        return value
    variable = value[len(_SECRET_ENV_REF_PREFIX) :]
    if variable not in os.environ:
        raise click.ClickException(
            f"the environment variable '{variable}' for '{name}.{key}' is not set"
        )
    variables.add(variable)
    return os.environ[variable]


@project.command()
@click.option("--name", "-n", help="Name of the project to create.")
@click.option("--description", help="Description of the project.")
@click.option("--secrets-file", help=SECRETS_FILE_HELP)
@click.pass_context
def create(ctx: click.Context, name: str, description: str, secrets_file: str):
    """Create a new project"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the project to create")
    description = description or ""
    secrets = None if secrets_file is None else _load_secrets_file(secrets_file)
    click.echo("Creating a new project")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.create_project(name, description, secrets=secrets)
        click.echo("Project created successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to create project: {e}")


@project.command()
@click.option("--name", "-n", help="Name of the project to delete.")
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a project by name."""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the project to delete")
    click.echo(f"Deleting project: {name}")
    click.echo("-" * 10)
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_project(name)
        click.echo("Project deleted successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete project: {e}")


@project.command()
@format_option
@view_options()
@click.option("--name", "-n", help="Name of the project to display.")
@click.pass_context
def get(ctx: click.Context, name: str):
    """Display a project by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the project to display")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        prj = server.get_project(name)

        def _view():
            return RecordView(
                title="Project",
                fields=[
                    ("Name", prj.name),
                    ("Description", present(prj.description)),
                    ("Defined by", present(prj.defined_by)),
                    ("Defined on", present(prj.defined_on_str)),
                ],
            )

        render(ctx, data=lambda: prj._data, view=_view)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display project: {e}")


@project.command(name="list")
@format_option
@view_options()
@max_result_option
@click.pass_context
def list_projects(ctx: click.Context):
    """List all projects"""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_projects, truncated = take(ctx, server.list_projects_generator())

        def _view():
            return TableView(
                title="Projects",
                columns=[
                    Column("Name", Kind.ID),
                    Column("Description"),
                    Column("Defined by"),
                    Column("Defined on", Kind.TIME),
                ],
                rows=[
                    [
                        prj.name,
                        present(prj.description),
                        present(prj.defined_by),
                        present(prj.defined_on_str),
                    ]
                    for prj in list_of_projects
                ],
                count="projects",
            )

        render(
            ctx,
            data=lambda: [p._data for p in list_of_projects],
            view=_view,
            truncated=truncated,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list projects: {e}")


@project.command()
@format_option
@view_options()
@click.option("--name", "-n", help="Name of the project whose secrets to list.")
@click.pass_context
def secrets(ctx: click.Context, name: str):
    """List the secrets of a project, with their keys (never their values)"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the project whose secrets to list")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        project_secrets = server.get_project_secrets(name)

        def _view():
            return TableView(
                title="Secrets",
                context=[("Project", name)],
                columns=[Column("Secret", Kind.ID), Column("Keys")],
                rows=[
                    [secret, present(", ".join(keys))]
                    for secret, keys in project_secrets.items()
                ],
                count="secrets",
            )

        render(ctx, data=lambda: project_secrets, view=_view)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list project secrets: {e}")


@project.command()
@click.option("--name", "-n", help="Name of the project to update.")
@click.option("--new-name", help="New name for the project.")
@click.option("--description", help="New description for the project.")
@click.option(
    "--secrets-file",
    help=SECRETS_FILE_HELP + " Replaces all the secrets of the project.",
)
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    new_name: str,
    description: str,
    secrets_file: str,
):
    """Update a project by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the project to update")
    secrets = None if secrets_file is None else _load_secrets_file(secrets_file)
    click.echo(f"Updating project: {name}")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.update_project(
            name,
            new_name=new_name,
            new_description=description,
            new_secrets=secrets,
        )
        click.echo("Project updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update project: {e}")
