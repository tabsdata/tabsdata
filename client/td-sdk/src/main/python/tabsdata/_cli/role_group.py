#
# Copyright 2025 Tabs Data Inc.
#


import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata._cli.cli_utils import (
    MutuallyExclusiveOption,
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    store_pinned_objects,
    verify_login_or_prompt,
)
from tabsdata.api.tabsdata_server import (
    PERMISSION_TYPES_WITH_ENTITY,
    VALID_PERMISSION_TYPES,
    Role,
    TabsdataServer,
)

ALL_ENTITIES_REPRESENTATION = "<ALL>"


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
    "--coll",
    help=(
        "Collection to which the permission will apply. If '"
        f"{ALL_ENTITIES_REPRESENTATION}' is provided, it will "
        "apply to all collections. This option is only allowed for "
        "permissions that require an entity, which are "
        f"{', '.join(str(p) for p in PERMISSION_TYPES_WITH_ENTITY)}."
    ),
)
@click.pass_context
def add_perm(ctx: click.Context, name: str, perm: str, coll: str):
    """Add a permission to a role"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(ctx, "Name of the role to add the permission to")
    )
    perm = perm or logical_prompt(
        ctx,
        "Permission to add. Valid values are "
        f"'{', '.join(str(p) for p in VALID_PERMISSION_TYPES)}'",
    )
    flattened_list = [item for tup in PERMISSION_TYPES_WITH_ENTITY for item in tup]
    requires_entity = perm in flattened_list
    if coll:
        if not requires_entity:
            raise click.ClickException(
                f"Permission '{perm}' does not require an entity. The permissions "
                "that require an entity are: "
                f"{', '.join(str(p) for p in PERMISSION_TYPES_WITH_ENTITY)}. "
                "Please remove the --coll option."
            )
        if coll == ALL_ENTITIES_REPRESENTATION:
            coll = None
    else:
        if requires_entity:
            coll = logical_prompt(
                ctx,
                "Collection to which the permission will apply. If "
                f"'{ALL_ENTITIES_REPRESENTATION}' is provided, it will apply "
                "to all collections.",
                default_value=ALL_ENTITIES_REPRESENTATION,
            )
            if coll == ALL_ENTITIES_REPRESENTATION:
                coll = None
    try:
        click.echo(f"Adding permission of type '{perm}' to role '{name}'")
        click.echo("-" * 10)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        role = server.get_role(name)
        permission = role.create_permission(perm, coll)

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
        or logical_prompt(ctx, "Name of the role to the user to")
    )
    user = user or logical_prompt(ctx, "User that will be added to the role")
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
        or logical_prompt(ctx, "Name of the role to create")
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
        or logical_prompt(ctx, "Name of the role to delete")
    )
    click.echo(f"Deleting role: {name}")
    click.echo("-" * 10)
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
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
    mutually_exclusive=["perm", "coll"],
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
    "--coll",
    help=(
        "Collection to which the permission applies. "
        "This option is only allowed for permissions that require an entity, which are "
        f"{', '.join(str(p) for p in PERMISSION_TYPES_WITH_ENTITY)}."
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["id"],
)
@click.pass_context
def delete_perm(ctx: click.Context, name: str, id: str, perm: str, coll: str):
    """Delete a permission from a role"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(
            ctx, "Name of the role from which the permission will be deleted"
        )
    )
    if not perm:
        id = id or logical_prompt(ctx, "ID of the permission to delete")
    else:
        coll = _obtain_coll_for_perm(perm, coll, ctx)
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
            _delete_permission_by_type_and_entity(perm, coll, role, name)

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


def _obtain_coll_for_perm(perm: str, coll: str, ctx: click.Context):
    flattened_list = [item for tup in PERMISSION_TYPES_WITH_ENTITY for item in tup]
    requires_entity = perm in flattened_list
    if coll:
        if not requires_entity:
            raise click.ClickException(
                f"Permission '{perm}' does not require an entity. The permissions "
                "that require an entity are: "
                f"{', '.join(str(p) for p in PERMISSION_TYPES_WITH_ENTITY)}. "
                "Please remove the --coll option."
            )
        if coll == ALL_ENTITIES_REPRESENTATION:
            coll = None
    else:
        if requires_entity:
            coll = logical_prompt(
                ctx,
                "Collection to which the permission that will be deleted "
                "currently applies. Provide "
                f"'{ALL_ENTITIES_REPRESENTATION}' if it applies "
                "to all collections.",
                default_value=ALL_ENTITIES_REPRESENTATION,
            )
            if coll == ALL_ENTITIES_REPRESENTATION:
                coll = None
    return coll


def _delete_permission_by_type_and_entity(perm: str, coll: str, role: Role, name: str):
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
        if p.entity == coll:
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
        or logical_prompt(ctx, "Name of the role from which the user will be deleted")
    )
    user = user or logical_prompt(ctx, "User to delete from the role")
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
@click.option("--name", "-n", help="Name of the role to display.")
@click.pass_context
def info(ctx: click.Context, name: str):
    """Display a role by name"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(ctx, "Name of the role to display")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        role = server.get_role(name)

        table = Table(title=f"Role '{name}'")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("ID", no_wrap=True)
        table.add_column("Description")
        table.add_column("Created on")
        table.add_column("Created by")

        table.add_row(
            role.name,
            role.id,
            role.description,
            role.created_on_str,
            role.created_by,
        )

        click.echo()
        console = Console()
        console.print(table)
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display role: {e}")


@role.command()
@click.pass_context
def list(ctx: click.Context):
    """List all roles"""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_roles = server.roles

        table = Table(title="Roles")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("ID", no_wrap=True)
        table.add_column("Description")
        table.add_column("Created on")
        table.add_column("Created by")

        for role in list_of_roles:
            table.add_row(
                role.name,
                role.id,
                role.description,
                role.created_on_str,
                role.created_by,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of roles: {len(list_of_roles)}")
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list roles: {e}")


@role.command()
@click.option("--name", "-n", help="Name of the role to which the permissions belong.")
@click.pass_context
def list_perm(ctx: click.Context, name: str):
    """List all permissions of a role"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(ctx, "Name of the role to which the permissions belong")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        role = server.get_role(name)
        list_of_permissions = role.permissions

        table = Table(title=f"Permissions of role '{name}'")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Permission type")
        table.add_column("Entity name")
        table.add_column("Granted on")
        table.add_column("Granted by")

        for permission in list_of_permissions:
            permission_type = permission.type
            flattened_list = [
                item for tup in PERMISSION_TYPES_WITH_ENTITY for item in tup
            ]
            if permission_type not in flattened_list:
                entity = "-"
            else:
                entity = (
                    permission.entity
                    if permission.entity
                    else ALL_ENTITIES_REPRESENTATION
                )
            table.add_row(
                permission.id,
                permission_type,
                entity,
                permission.granted_on_str,
                permission.granted_by,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of permissions: {len(list_of_permissions)}")
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list permissions: {e}")


@role.command()
@click.option("--name", "-n", help="Name of the role to which the users belong.")
@click.pass_context
def list_user(ctx: click.Context, name: str):
    """List all users of a role"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "role")
        or logical_prompt(ctx, "Name of the role to which the users belong")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        role = server.get_role(name)
        list_of_users = role.users

        table = Table(title=f"Users of role '{name}'")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Full name")
        table.add_column("Email")
        table.add_column("Enabled")

        for user in list_of_users:
            table.add_row(user.name, user.full_name, user.email, str(user.enabled))

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of users: {len(list_of_users)}")
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list users: {e}")


@role.command()
@click.option("--name", "-n", help="Name of the role to pin.")
@click.pass_context
def pin(ctx: click.Context, name: str):
    """Pin a role by name"""
    name = name or logical_prompt(ctx, "Name of the role to be pinned")
    click.echo(f"Pinning role: {name}")
    click.echo("-" * 10)
    try:
        previously_pinned = ctx.obj["pinned_objects"].get("role")
        if previously_pinned:
            click.echo(f"Unpinning previously pinned role: {previously_pinned}")
        ctx.obj["pinned_objects"]["role"] = name
        store_pinned_objects(ctx)
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
        previously_pinned = ctx.obj["pinned_objects"].get("role")
        if not previously_pinned:
            click.echo("No previously pinned role to unpin.")
        else:
            ctx.obj["pinned_objects"]["role"] = None
            store_pinned_objects(ctx)
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
        or logical_prompt(ctx, "Name of the role to update")
    )
    description = description or ""
    click.echo(f"Updating role: {name}")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.update_role(name, new_name=new_name, new_description=description)
        click.echo("Role updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update role: {e}")
