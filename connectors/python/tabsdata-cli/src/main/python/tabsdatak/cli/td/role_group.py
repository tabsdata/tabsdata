#
# Copyright 2026 Tabsdata Inc.
#


import rich_click as click

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    MutuallyExclusiveOption,
    format_option,
    get_currently_pinned_object,
    get_pin,
    hint_common_solutions,
    logical_prompt,
    max_result_option,
    present,
    render,
    require_word,
    set_pin,
    store_cli_options,
    take,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView
from tabsdatak.sdk.tabsdata_server import (
    PERMISSION_TYPES_WITH_ENTITY,
    VALID_PERMISSION_TYPES,
    Role,
    TabsdataServer,
)

ALL_ENTITIES_REPRESENTATION = "<ALL>"


@cli_command
@click.group()
def role():
    """Role management commands"""


@role.command()
@click.option(
    "--name",
    "-n",
    help="Name of the role to which the permissions will be added.",
)
@click.option(
    "--perm",
    help=(
        "Permission to add. Will "
        "be prompted for it if not provided. Valid values are "
        f"{', '.join(str(p) for p in VALID_PERMISSION_TYPES)}"
    ),
)
@click.option(
    "--proj",
    "project",
    help=(
        "Project to which the permission will apply. If '"
        f"{ALL_ENTITIES_REPRESENTATION}' is provided, it will "
        "apply to all projects. This option is only allowed for "
        "permissions that require an entity, which are "
        f"{', '.join(str(p) for p in PERMISSION_TYPES_WITH_ENTITY)}."
    ),
)
@click.pass_context
def add_perm(ctx: click.Context, name: str, perm: str, project: str):
    """Add a permission to a role"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(
            ctx, "Name of the role to add the permission to", option="--name"
        )
    )
    perm = perm or logical_prompt(
        ctx,
        "Permission to add. Valid values are "
        f"'{', '.join(str(p) for p in VALID_PERMISSION_TYPES)}'",
        option="--perm",
    )
    flattened_list = [item for tup in PERMISSION_TYPES_WITH_ENTITY for item in tup]
    requires_entity = perm in flattened_list
    if project:
        if not requires_entity:
            raise click.ClickException(
                f"Permission '{perm}' does not require an entity. The permissions "
                "that require an entity are: "
                f"{', '.join(str(p) for p in PERMISSION_TYPES_WITH_ENTITY)}. "
                "Please remove the --proj option."
            )
        if project == ALL_ENTITIES_REPRESENTATION:
            project = None
    else:
        if requires_entity:
            project = logical_prompt(
                ctx,
                "Project to which the permission will apply. If "
                f"'{ALL_ENTITIES_REPRESENTATION}' is provided, it will apply "
                "to all projects.",
                default_value=ALL_ENTITIES_REPRESENTATION,
                option="--proj",
            )
            if project == ALL_ENTITIES_REPRESENTATION:
                project = None
    try:
        click.echo(f"Adding permission of type '{perm}' to role '{name}'")
        click.echo("-" * 10)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        role = server.get_role(name)
        permission = role.create_permission(perm, project)

        click.echo("Permission added successfully")
        click.echo(f"Permission ID: {permission.id}")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to add permission: {e}")


@role.command()
@click.option(
    "--name",
    "-n",
    help="Name of the role to which the user will be added.",
)
@click.option(
    "--user",
    "-u",
    help="User that will be added to the role.",
)
@click.pass_context
def add_user(ctx: click.Context, name: str, user: str):
    """Add a user to a role"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(ctx, "Name of the role to the user to", option="--name")
    )
    user = user or logical_prompt(
        ctx, "User that will be added to the role", option="--user"
    )
    try:
        click.echo(f"Adding user '{user}' to role '{name}'")
        click.echo("-" * 10)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        role = server.get_role(name)
        role.add_user(user)

        click.echo("User added successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to add user to role: {e}")


@role.command()
@click.option("--name", "-n", help="Name of the role to create.")
@click.option("--description", help="Description of the role.")
@click.pass_context
def create(
    ctx: click.Context,
    name: str,
    description: str,
):
    """Create a new role"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(ctx, "Name of the role to create", option="--name")
    )
    description = description or ""
    click.echo("Creating a new role")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.create_role(name, description)
        click.echo("Role created successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to create role: {e}")


@role.command()
@click.option("--name", "-n", help="Name of the role to delete.")
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a role by name"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(ctx, "Name of the role to delete", option="--name")
    )
    click.echo(f"Deleting role: {name}")
    click.echo("-" * 10)
    require_word(ctx, confirm, "delete", "Please type 'delete' to confirm deletion")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_role(name)
        click.echo("Role deleted successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete role: {e}")


@role.command()
@click.option(
    "--name",
    "-n",
    help="Name of the role from which the permissions will be deleted.",
)
@click.option(
    "--id",
    help="ID of the permission to delete.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["perm", "project"],
)
@click.option(
    "--perm",
    help=(
        "Permission to delete. Valid values are "
        f"{', '.join(str(p) for p in VALID_PERMISSION_TYPES)}"
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["id"],
)
@click.option(
    "--proj",
    "project",
    help=(
        "Project to which the permission applies. "
        "This option is only allowed for permissions that require an entity, which are "
        f"{', '.join(str(p) for p in PERMISSION_TYPES_WITH_ENTITY)}."
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["id"],
)
@click.pass_context
def delete_perm(ctx: click.Context, name: str, id: str, perm: str, project: str):
    """Delete a permission from a role"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(
            ctx,
            "Name of the role from which the permission will be deleted",
            option="--name",
        )
    )
    if not perm:
        id = id or logical_prompt(ctx, "ID of the permission to delete", option="--id")
    else:
        project = _obtain_project_for_perm(perm, project, ctx)
    try:
        if id:
            click.echo(f"Deleting permission from role '{name}' with ID '{id}'")
        else:
            click.echo(f"Deleting permission '{perm}' from role '{name}'")
        click.echo("-" * 10)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        role = server.get_role(name)
        if id:
            _delete_permission_by_id(role, id, name)
        else:
            _delete_permission_by_type_and_entity(perm, project, role, name)

        click.echo("Permission deleted successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete permission: {e}")


def _delete_permission_by_id(role: Role, id: str, name: str):
    permissions = role.list_permissions(filter=f"id:eq:{id}")
    try:
        permission = permissions[0]
    except IndexError:
        raise click.ClickException(
            f"No permission found for role '{name}' with ID '{id}'. The"
            " existing permissions are:"
            f" {', '.join(p.id for p in role.permissions)}"
        )
    role.delete_permission(permission)


def _obtain_project_for_perm(perm: str, project: str, ctx: click.Context):
    flattened_list = [item for tup in PERMISSION_TYPES_WITH_ENTITY for item in tup]
    requires_entity = perm in flattened_list
    if project:
        if not requires_entity:
            raise click.ClickException(
                f"Permission '{perm}' does not require an entity. The permissions "
                "that require an entity are: "
                f"{', '.join(str(p) for p in PERMISSION_TYPES_WITH_ENTITY)}. "
                "Please remove the --proj option."
            )
        if project == ALL_ENTITIES_REPRESENTATION:
            project = None
    else:
        if requires_entity:
            project = logical_prompt(
                ctx,
                "Project to which the permission that will be deleted "
                "currently applies. Provide "
                f"'{ALL_ENTITIES_REPRESENTATION}' if it applies "
                "to all projects.",
                default_value=ALL_ENTITIES_REPRESENTATION,
                option="--proj",
            )
            if project == ALL_ENTITIES_REPRESENTATION:
                project = None
    return project


def _delete_permission_by_type_and_entity(
    perm: str, project: str, role: Role, name: str
):
    for n, value in VALID_PERMISSION_TYPES:
        if perm.lower() == n or perm.lower() == value:
            permission_type = value
            break
    else:
        raise click.ClickException(
            f"'{perm}' is an invalid permission type. "
            "The valid values are: "
            f"{', '.join(str(p) for p in VALID_PERMISSION_TYPES)}."
        )
    permissions = role.list_permissions(
        filter=f"permission_type:eq:{permission_type}",
    )
    for p in permissions:
        if p.entity == project:
            role.delete_permission(p)
            break
    else:
        flattened_list = [item for tup in PERMISSION_TYPES_WITH_ENTITY for item in tup]
        list_of_permissions = []
        for p in role.permissions:
            entity = p.entity
            if not entity:
                entity = (
                    ALL_ENTITIES_REPRESENTATION if p.type in flattened_list else "-"
                )
            list_of_permissions.append((p.type, entity))
        raise click.ClickException(
            f"No permission found for role '{name}' with type '{perm}' and "
            "applying to the provided entity. The existing "
            "permissions are: "
            f"{', '.join(str(p) for p in list_of_permissions)}"
        )


@role.command()
@click.option(
    "--name",
    "-n",
    help="Name of the role from which the user will be deleted.",
)
@click.option(
    "--user",
    "-u",
    help="User to delete from the role.",
)
@click.pass_context
def delete_user(ctx: click.Context, name: str, user: str):
    """Delete a user from a role"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(
            ctx, "Name of the role from which the user will be deleted", option="--name"
        )
    )
    user = user or logical_prompt(ctx, "User to delete from the role", option="--user")
    try:
        click.echo(f"Deleting user '{user}' from role '{name}'")
        click.echo("-" * 10)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        role = server.get_role(name)
        users = role.list_users(filter=f"user:eq:{user}")
        try:
            user = users[0]
        except IndexError:
            raise click.ClickException(
                f"No users found for role '{name}' with name '{user}'. The existing "
                f"users are: {', '.join(u.name for u in role.users)}"
            )
        role.delete_user(user)

        click.echo("User deleted from role successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete user from role: {e}")


@role.command()
@format_option
@view_options()
@click.option("--name", "-n", help="Name of the role to display.")
@click.pass_context
def info(ctx: click.Context, name: str):
    """Display a role by name"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(ctx, "Name of the role to display", option="--name")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        role = server.get_role(name)

        def _view():
            return RecordView(
                title="Role",
                fields=[
                    ("Name", role.name),
                    ("ID", role.id),
                    ("Description", present(role.description)),
                    ("Created on", present(role.created_on_str)),
                    ("Created by", present(role.created_by)),
                ],
            )

        render(ctx, data=lambda: role._data, view=_view)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display role: {e}")


@role.command(name="list")
@format_option
@view_options()
@max_result_option
@click.pass_context
def list_roles(ctx: click.Context):
    """List all roles"""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_roles, truncated = take(ctx, server.list_roles_generator())

        def _view():
            return TableView(
                title="Roles",
                columns=[
                    Column("Name", Kind.ID),
                    Column("ID", Kind.ID),
                    Column("Description"),
                    Column("Created on", Kind.TIME),
                    Column("Created by"),
                ],
                rows=[
                    [
                        role.name,
                        role.id,
                        present(role.description),
                        present(role.created_on_str),
                        present(role.created_by),
                    ]
                    for role in list_of_roles
                ],
                count="roles",
            )

        render(
            ctx,
            data=lambda: [{**r.kwargs, "name": r.name} for r in list_of_roles],
            view=_view,
            truncated=truncated,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list roles: {e}")


@role.command()
@format_option
@view_options()
@click.option("--name", "-n", help="Name of the role to which the permissions belong.")
@max_result_option
@click.pass_context
def list_perm(ctx: click.Context, name: str):
    """List all permissions of a role"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(
            ctx, "Name of the role to which the permissions belong", option="--name"
        )
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        role = server.get_role(name)
        list_of_permissions, truncated = take(ctx, role.list_permissions_generator())
        with_entity = {item for tup in PERMISSION_TYPES_WITH_ENTITY for item in tup}

        def _entity(permission):
            if permission.type not in with_entity:
                return None
            return permission.entity or ALL_ENTITIES_REPRESENTATION

        def _view():
            return TableView(
                title="Permissions",
                context=[("Role", name)],
                columns=[
                    Column("ID", Kind.ID),
                    Column("Permission type"),
                    Column("Entity name"),
                    Column("Granted on", Kind.TIME),
                    Column("Granted by"),
                ],
                rows=[
                    [
                        permission.id,
                        permission.type,
                        _entity(permission),
                        present(permission.granted_on_str),
                        present(permission.granted_by),
                    ]
                    for permission in list_of_permissions
                ],
                count="permissions",
            )

        render(
            ctx,
            data=lambda: [{**p.kwargs, "id": p.id} for p in list_of_permissions],
            view=_view,
            truncated=truncated,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list permissions: {e}")


@role.command()
@format_option
@view_options()
@click.option("--name", "-n", help="Name of the role to which the users belong.")
@max_result_option
@click.pass_context
def list_user(ctx: click.Context, name: str):
    """List all users of a role"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(
            ctx, "Name of the role to which the users belong", option="--name"
        )
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        role = server.get_role(name)
        list_of_users, truncated = take(ctx, role.list_users_generator())

        def _view():
            return TableView(
                title="Users",
                context=[("Role", name)],
                columns=[
                    Column("Name", Kind.ID),
                    Column("Full name"),
                    Column("Email"),
                    Column("Enabled", Kind.FLAG),
                ],
                rows=[
                    [
                        user.name,
                        present(user.full_name),
                        present(user.email),
                        user.enabled,
                    ]
                    for user in list_of_users
                ],
                count="users",
            )

        render(
            ctx,
            data=lambda: [{**u.kwargs, "name": u.name} for u in list_of_users],
            view=_view,
            truncated=truncated,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list users: {e}")


@role.command()
@click.option("--name", "-n", help="Name of the role to pin.")
@click.pass_context
def pin(ctx: click.Context, name: str):
    """Pin a role by name"""
    name = name or logical_prompt(ctx, "Name of the role to be pinned", option="--name")
    click.echo(f"Pinning role: {name}")
    click.echo("-" * 10)
    try:
        previously_pinned = get_pin(ctx, "role")
        if previously_pinned:
            click.echo(f"Unpinning previously pinned role: {previously_pinned}")
        set_pin(ctx, "role", name)
        store_cli_options(ctx)
        click.echo("Role pinned successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to pin role: {e}")


@role.command()
@click.pass_context
def unpin(ctx: click.Context):
    """Unpin the currently pinned role"""
    click.echo("Unpinning role")
    click.echo("-" * 10)
    try:
        previously_pinned = get_pin(ctx, "role")
        if not previously_pinned:
            click.echo("No previously pinned role to unpin.")
        else:
            set_pin(ctx, "role", None)
            store_cli_options(ctx)
            click.echo("Role unpinned successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to unpin role: {e}")


@role.command()
@click.option("--name", "-n", help="Name of the role to update.")
@click.option("--new-name", help="New name for the role.")
@click.option("--description", help="New description for the role.")
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    new_name: str,
    description: str,
):
    """Update a role by name"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(ctx, "Name of the role to update", option="--name")
    )
    click.echo(f"Updating role: {name}")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.update_role(name, new_name=new_name, new_description=description)
        click.echo("Role updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update role: {e}")
