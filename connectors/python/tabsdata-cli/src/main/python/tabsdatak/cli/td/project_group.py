#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._command import TdkGroup
from tabsdatak.cli._prompted import prompted
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    IdType,
    MutuallyExclusiveOption,
    ancestry,
    format_option,
    hint_common_solutions,
    list_options,
    list_params,
    max_result_option,
    present,
    progress,
    proj_option,
    record_fields_option,
    render,
    render_deleted,
    render_entity,
    render_list,
    require_word,
    take,
    view_options,
)
from tabsdatak.cli.td.tag_commands import tag_group
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
TOPOLOGY_VERSION_HINT = "'tdk project topology list' lists topology version ids."
TOPOLOGY_VERSION_ID = IdType(TOPOLOGY_VERSION_HINT)
_TOPOLOGY_VERSION_HELP = (
    "Read the graph at this topology version, as 'tdk project topology list' "
    "lists them."
)
_GRAPH_AT_HELP = (
    "Read the graph as it was at this time, in the formats '--at' takes "
    "everywhere else."
)


@cli_command
@click.group(cls=TdkGroup)
def project():
    """Project management commands"""


def _load_secrets_file(
    ctx: click.Context, secrets_file: str
) -> dict[str, dict[str, str | None]]:
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
        progress(
            ctx,
            "Project secrets read from environment variable(s): "
            + ", ".join(sorted(variables)),
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


@capability(
    "admin", "write", "read", paths={"--secrets-file": "read"}, credential="session"
)
@project.command()
@format_option
@prompted(
    "--name",
    "-n",
    prompt="Name of the project to create",
    help="Name of the project to create.",
)
@click.option("--description", help="Description of the project.")
@click.option(
    "--secrets-file",
    help=SECRETS_FILE_HELP,
    type=click.Path(exists=True, dir_okay=False, allow_dash=True),
)
@click.pass_context
def create(ctx: click.Context, name: str, description: str, secrets_file: str):
    """Create a new project"""
    description = description or ""
    secrets = None if secrets_file is None else _load_secrets_file(ctx, secrets_file)
    progress(ctx, "Creating a new project")
    progress(ctx, "-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        created = server.create_project(name, description, secrets=secrets)
        render_entity(
            ctx, data=lambda: created._data, message="Project created successfully"
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to create project: {e}")


@capability("admin", "destructive", credential="session")
@project.command()
@format_option
@prompted(
    "--name",
    "-n",
    prompt="Name of the project to delete",
    help="Name of the project to delete.",
)
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a project by name."""
    progress(ctx, f"Deleting project: {name}")
    progress(ctx, "-" * 10)
    require_word(ctx, confirm, "delete", "Please type 'delete' to confirm deletion")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_project(name)
        render_deleted(
            ctx, kind="project", name=name, message="Project deleted successfully"
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete project: {e}")


@capability("analytics", "read")
@project.command()
@format_option
@view_options()
@prompted(
    "--name",
    "-n",
    prompt="Name of the project to display",
    help="Name of the project to display.",
)
@record_fields_option
@click.pass_context
def get(ctx: click.Context, name: str):
    """Display a project by name"""
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


@capability("analytics", "read")
@project.command(name="list")
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_projects(ctx: click.Context):
    """List all projects"""
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_projects, truncated = take(
            ctx, server.list_projects_generator(**list_params(ctx))
        )

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

        render_list(
            ctx,
            items=lambda: [p._data for p in list_of_projects],
            truncated=truncated,
            view=_view,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list projects: {e}")


@capability("admin", "read", credential="session")
@project.command()
@format_option
@view_options()
@prompted(
    "--name",
    "-n",
    prompt="Name of the project whose secrets to list",
    help="Name of the project whose secrets to list.",
)
@click.pass_context
def secrets(ctx: click.Context, name: str):
    """List the secrets of a project, with their keys (never their values)"""
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


@capability(
    "admin", "write", "read", paths={"--secrets-file": "read"}, credential="session"
)
@project.command()
@format_option
@prompted(
    "--name",
    "-n",
    prompt="Name of the project to update",
    help="Name of the project to update.",
)
@click.option("--new-name", help="New name for the project.")
@click.option("--description", help="New description for the project.")
@click.option(
    "--secrets-file",
    help=SECRETS_FILE_HELP + " Replaces all the secrets of the project.",
    type=click.Path(exists=True, dir_okay=False, allow_dash=True),
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
    secrets = None if secrets_file is None else _load_secrets_file(ctx, secrets_file)
    progress(ctx, f"Updating project: {name}")
    progress(ctx, "-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        updated = server.update_project(
            name,
            new_name=new_name,
            new_description=description,
            new_secrets=secrets,
        )
        render_entity(
            ctx, data=lambda: updated._data, message="Project updated successfully"
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update project: {e}")


project.add_command(tag_group("project"))


def topology_options(fn):
    for option in (
        click.option(
            "--at",
            help=_GRAPH_AT_HELP,
            cls=MutuallyExclusiveOption,
            mutually_exclusive=["topology_version"],
        ),
        click.option(
            "--topology-version",
            "topology_version",
            type=TOPOLOGY_VERSION_ID,
            help=_TOPOLOGY_VERSION_HELP,
            cls=MutuallyExclusiveOption,
            mutually_exclusive=["at"],
        ),
    ):
        fn = option(fn)
    return fn


def _tags_cell(tags: dict | None) -> str | None:
    return present(", ".join(f"{k}={v}" for k, v in sorted((tags or {}).items())))


def _graph_views(ctx: click.Context, document: dict) -> list[TableView]:
    from tabsdatak.common._datetime import _convert_timestamp_to_string

    functions = document.get("all_functions") or {}
    tables = document.get("all_tables") or {}
    system = set(document.get("system_tables") or ())
    kinds: dict[tuple[str, str], set[str]] = {}
    for relation in document.get("relations") or ():
        table_id = relation["table_version_id"]
        if table_id in system:
            continue
        function = ("function", relation["function_version_id"])
        table = ("table", table_id)
        kind = next(iter(relation["edge"]))
        pair = (function, table) if kind == "Output" else (table, function)
        kinds.setdefault(pair, set()).add(kind)

    def label(node: tuple[str, str]) -> str:
        kind, node_id = node
        info = (functions if kind == "function" else tables).get(node_id) or {}
        return f"{kind} {info.get('collection')}/{info.get('name')}"

    def edge_kind(found: set[str]) -> str:
        if "Output" in found:
            return "output"
        if "Dependency" in found:
            return "dependency, trigger" if "Trigger" in found else "dependency"
        return "trigger"

    nodes = [
        ["function", f.get("collection"), f.get("name"), _tags_cell(f.get("tags"))]
        for f in functions.values()
    ] + [
        ["table", t.get("collection"), t.get("name"), _tags_cell(t.get("tags"))]
        for table_id, t in tables.items()
        if table_id not in system
    ]
    nodes.sort(key=lambda row: (row[0], row[1] or "", row[2] or ""))
    edges = sorted(
        [label(source), label(target), edge_kind(found)]
        for (source, target), found in kinds.items()
    )
    notes = (
        [f"System tables left out: {len(system)}; '-o json' has them."]
        if system
        else []
    )
    return [
        TableView(
            title="Graph",
            context=[
                *ancestry(ctx),
                ("Topology version", document.get("version")),
                ("At", _convert_timestamp_to_string(document.get("at"))),
            ],
            columns=[
                Column("Kind"),
                Column("Collection", Kind.ID),
                Column("Name", Kind.ID),
                Column("Tags"),
            ],
            rows=nodes,
            count="functions and tables",
        ),
        TableView(
            title="Edges",
            columns=[Column("From"), Column("To"), Column("Kind")],
            rows=edges,
            count="edges",
            notes=notes,
        ),
    ]


@capability("analytics", "read")
@project.command()
@proj_option
@topology_options
@format_option
@view_options()
@click.pass_context
def graph(ctx: click.Context, topology_version: str, at: str):
    """Show the project's functions and tables and the edges between them"""
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        document = server.project_graph(topology_version=topology_version, at=at)
        render(ctx, data=document, view=lambda: _graph_views(ctx, document))
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to obtain project graph: {e}")


@project.group()
def topology():
    """The project's topology versions"""


@capability("analytics", "read")
@topology.command(name="list")
@proj_option
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_topology_versions(ctx: click.Context):
    """List the project's topology versions, each with when it was current"""
    try:
        from tabsdatak.common._datetime import _convert_timestamp_to_string

        server: TabsdataServer = ctx.obj["tabsdataserver"]
        versions, truncated = take(
            ctx, server.list_topology_versions_generator(**list_params(ctx))
        )

        def _view():
            return TableView(
                title="Topology versions",
                context=ancestry(ctx),
                columns=[
                    Column("Version", Kind.ID),
                    Column("Valid from", Kind.TIME),
                    Column("Valid to", Kind.TIME),
                ],
                rows=[
                    [
                        version["id"],
                        _convert_timestamp_to_string(version["valid_from"]),
                        (
                            "current"
                            if version.get("valid_to") is None
                            else _convert_timestamp_to_string(version["valid_to"])
                        ),
                    ]
                    for version in versions
                ],
                count="topology versions",
            )

        render_list(ctx, items=versions, truncated=truncated, view=_view)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list topology versions: {e}")
