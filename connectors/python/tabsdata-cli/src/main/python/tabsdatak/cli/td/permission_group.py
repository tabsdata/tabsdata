#
# Copyright 2026 Tabs Data Inc.
#


import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    output_option,
    render,
    MutuallyExclusiveOption,
    hint_common_solutions,
    logical_prompt,
    verify_login_or_prompt,
)
from tabsdatak.sdk.tabsdata_server import (
    RBAC_ONLY_WILDCARD_MESSAGE,
    SIMPLE_PROJECT_PERMISSION_TYPES,
    SIMPLE_RBAC_ONLY_WILDCARD_TYPES,
    SIMPLE_SYSTEM_PERMISSION_TYPES,
    VALID_SIMPLE_PERMISSION_TYPES,
    TabsdataServer,
)


def _validate_permission(perm: str, project: str) -> None:
    """Client-side validation matching the server's facade rules.

    - The org-wide read/develop wildcards are RBAC Mode View only.
    - Project-scoped permissions require ``--proj``; system-scoped
      permissions forbid it.
    """
    perm = perm.lower()
    if perm in SIMPLE_RBAC_ONLY_WILDCARD_TYPES:
        raise click.ClickException(RBAC_ONLY_WILDCARD_MESSAGE)
    if perm not in VALID_SIMPLE_PERMISSION_TYPES:
        raise click.ClickException(
            f"'{perm}' is an invalid permission. The valid values are: "
            f"{', '.join(VALID_SIMPLE_PERMISSION_TYPES)}."
        )
    if perm in SIMPLE_PROJECT_PERMISSION_TYPES and not project:
        raise click.ClickException(
            f"Permission '{perm}' is project-scoped and requires --proj. "
            "The project-scoped permissions are: "
            f"{', '.join(SIMPLE_PROJECT_PERMISSION_TYPES)}."
        )
    if perm in SIMPLE_SYSTEM_PERMISSION_TYPES and project:
        raise click.ClickException(
            f"Permission '{perm}' is system-scoped and does not take a "
            "--proj. The system-scoped permissions are: "
            f"{', '.join(SIMPLE_SYSTEM_PERMISSION_TYPES)}. "
            "Please remove the --proj option."
        )


@cli_command
@click.group()
def permission():
    """Permission management commands (Simple Security Mode)"""


@permission.command()
@click.option(
    "--user",
    "-u",
    help="User to grant the permission to.",
)
@click.option(
    "--permission",
    help=(
        "Permission to grant. Valid values are "
        f"{', '.join(VALID_SIMPLE_PERMISSION_TYPES)}. The project-scoped "
        "permissions ("
        f"{', '.join(SIMPLE_PROJECT_PERMISSION_TYPES)}) require --proj; the "
        "system-scoped ones ("
        f"{', '.join(SIMPLE_SYSTEM_PERMISSION_TYPES)}) must not carry --proj."
    ),
)
@click.option(
    "--proj",
    "project",
    help=(
        "Project the permission applies to. Required for project-scoped "
        "permissions, forbidden for system-scoped ones."
    ),
)
@click.pass_context
def grant(ctx: click.Context, user: str, permission: str, project: str):
    """Grant a permission to a user"""
    verify_login_or_prompt(ctx)
    user = user or logical_prompt(ctx, "User to grant the permission to")
    permission = permission or logical_prompt(
        ctx,
        "Permission to grant. Valid values are "
        f"'{', '.join(VALID_SIMPLE_PERMISSION_TYPES)}'",
    )
    _validate_permission(permission, project)
    try:
        if project:
            click.echo(
                f"Granting permission '{permission}' to user '{user}' on "
                f"project '{project}'"
            )
        else:
            click.echo(f"Granting permission '{permission}' to user '{user}'")
        click.echo("-" * 10)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.grant_permission(user, permission, project)
        click.echo("Permission granted successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to grant permission: {e}")


@permission.command()
@click.option(
    "--user",
    "-u",
    help="User to revoke the permission from.",
)
@click.option(
    "--permission",
    help=(
        "Permission to revoke. Valid values are "
        f"{', '.join(VALID_SIMPLE_PERMISSION_TYPES)}. The project-scoped "
        "permissions ("
        f"{', '.join(SIMPLE_PROJECT_PERMISSION_TYPES)}) require --proj; the "
        "system-scoped ones ("
        f"{', '.join(SIMPLE_SYSTEM_PERMISSION_TYPES)}) must not carry --proj."
    ),
)
@click.option(
    "--proj",
    "project",
    help=(
        "Project the permission applies to. Required for project-scoped "
        "permissions, forbidden for system-scoped ones."
    ),
)
@click.pass_context
def revoke(ctx: click.Context, user: str, permission: str, project: str):
    """Revoke a permission from a user"""
    verify_login_or_prompt(ctx)
    user = user or logical_prompt(ctx, "User to revoke the permission from")
    permission = permission or logical_prompt(
        ctx,
        "Permission to revoke. Valid values are "
        f"'{', '.join(VALID_SIMPLE_PERMISSION_TYPES)}'",
    )
    _validate_permission(permission, project)
    try:
        if project:
            click.echo(
                f"Revoking permission '{permission}' from user '{user}' on "
                f"project '{project}'"
            )
        else:
            click.echo(f"Revoking permission '{permission}' from user '{user}'")
        click.echo("-" * 10)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.revoke_permission(user, permission, project)
        click.echo("Permission revoked successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to revoke permission: {e}")


@permission.command(name="list")
@click.option(
    "--user",
    "-u",
    help="List the effective flat permissions of this user.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["project"],
)
@click.option(
    "--proj",
    "project",
    help="List everyone with authority on this project.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["user"],
)
@output_option
@click.pass_context
def list_permissions(ctx: click.Context, user: str, project: str):
    """List a user's permissions, or who has access to a project"""
    verify_login_or_prompt(ctx)
    if not user and not project:
        raise click.ClickException(
            "Provide either --user (a user's effective permissions) or "
            "--proj (everyone with authority on a project)."
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        if user:
            _list_by_user(ctx, server, user)
        else:
            _list_by_project(ctx, server, project)
    except click.ClickException:
        raise
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list permissions: {e}")


def _list_by_user(ctx: click.Context, server: TabsdataServer, user: str):
    permissions = server.list_permissions_by_user(user)

    def _table():
        table = Table(title=f"Permissions of user '{user}'")
        table.add_column("Permission", style="cyan", no_wrap=True)
        table.add_column("Project")
        for perm in permissions:
            table.add_row(perm.permission, perm.project if perm.project else "-")
        click.echo()
        Console().print(table)
        click.echo(f"Number of permissions: {len(permissions)}")
        click.echo()

    render(
        ctx,
        data=[{"permission": p.permission, "project": p.project} for p in permissions],
        table=_table,
    )


def _list_by_project(ctx: click.Context, server: TabsdataServer, project: str):
    access = server.list_permissions_by_project(project)

    def _table():
        table = Table(title=f"Access on project '{project}'")
        table.add_column("User", style="cyan", no_wrap=True)
        table.add_column("Permission")
        table.add_column("Source")
        for entry in access:
            table.add_row(entry.user, entry.permission, entry.source)
        click.echo()
        Console().print(table)
        click.echo(f"Number of grants: {len(access)}")
        click.echo()

    render(
        ctx,
        data=[
            {"user": e.user, "permission": e.permission, "source": e.source}
            for e in access
        ],
        table=_table,
    )
