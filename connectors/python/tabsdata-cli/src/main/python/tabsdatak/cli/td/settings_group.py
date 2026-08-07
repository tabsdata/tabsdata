#
# Copyright 2026 Tabsdata Inc.
#


import rich_click as click

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    DISABLED,
    ENABLED,
    OPTION_HINTS,
    OPTION_PROMPT,
    OPTION_SERVER,
    PIN_COLLECTION,
    PIN_PROJECT,
    SERVER_URL_DEFAULTS_HELP,
    clear_current_cli_option,
    cli_option_enabled,
    configured_server,
    format_option,
    get_current_cli_option,
    get_pin,
    hint_common_solutions,
    render,
    set_cli_option_enabled,
    set_current_cli_option,
    set_pin,
    store_cli_options,
)

_NOTHING_TO_SET = (
    "Nothing to set. Specify at least one setting: --hints true|false, "
    "--prompt true|false, --server SERVER, --proj PROJECT and/or "
    "--coll COLLECTION."
)
_NOTHING_TO_UNSET = (
    "Nothing to unset. Specify at least one setting: --hints, --prompt, "
    "--server, --proj and/or --coll -- or --all for every one of them."
)

_TRUE = "true"
_FALSE = "false"
# The on/off settings take the value explicitly, so a command reads as what it
# leaves behind; both default to true, which is what `tdk unset` restores. The
# same two words render them in `tdk settings`.
_BOOL = click.Choice((_TRUE, _FALSE))


@cli_command
@click.command("set")
@click.option(
    "--hints",
    type=_BOOL,
    help="Show the hints the CLI adds to errors. Default: true.",
)
@click.option(
    "--prompt",
    type=_BOOL,
    help=(
        "Prompt for a value a command needs but was not given. Default: true; "
        "'tdk set --prompt false' is the standing form of the root "
        "'--no-prompt' flag."
    ),
)
@click.option(
    "--server",
    "url",
    metavar="SERVER",
    help=(
        "URL of the Tabsdata Server 'tdk login' uses without '--server'. "
        + SERVER_URL_DEFAULTS_HELP
    ),
)
@click.option(
    "--proj",
    "-P",
    "project",
    help="Project to pin as the default of project-scoped commands.",
)
@click.option(
    "--coll",
    "-c",
    "collection",
    help="Collection to pin (requires a pinned project).",
)
@click.pass_context
def set_setting(
    ctx: click.Context,
    hints: str,
    prompt: str,
    url: str,
    project: str,
    collection: str,
):
    """Give a setting a value.

    Settings are persisted in 'options.json' under the Tabsdata directory
    ('~/.tabsdata') and apply to every later invocation; 'tdk unset' drops them
    back to their default and 'tdk settings' shows them. More than one can be
    set at once.

    Examples:
        tdk set --hints false
        tdk set --prompt false
        tdk set --server http://127.0.0.1:2457
        tdk set --server 127.0.0.1              # http://127.0.0.1:2457
        tdk set --proj sales
        tdk set --proj sales --coll orders
    """
    if hints is None and prompt is None and not (url or project or collection):
        raise click.UsageError(_NOTHING_TO_SET)
    if hints is not None:
        _set_enabled(ctx, OPTION_HINTS, hints == _TRUE)
    if prompt is not None:
        _set_enabled(ctx, OPTION_PROMPT, prompt == _TRUE)
    if url:
        _set_server(ctx, url)
    if project or collection:
        _pin(ctx, project, collection)
    store_cli_options(ctx)


@cli_command
@click.command("unset")
@click.option("--hints", is_flag=True, help="Restore the hints default (true).")
@click.option(
    "--prompt",
    is_flag=True,
    help="Restore the prompting default (true).",
)
@click.option("--server", "url", is_flag=True, help="Clear the configured server.")
@click.option(
    "--proj",
    "-P",
    "project",
    is_flag=True,
    help=(
        "Unpin the project. Unpins the collection too, since a collection pin "
        "only resolves under a project."
    ),
)
@click.option("--coll", "-c", "collection", is_flag=True, help="Unpin the collection.")
@click.option(
    "--all",
    "all_settings",
    is_flag=True,
    help=(
        "Unset every setting: shorthand for all the flags above, leaving every "
        "one of them at its default."
    ),
)
@click.pass_context
def unset_setting(
    ctx: click.Context,
    hints: bool,
    prompt: bool,
    url: bool,
    project: bool,
    collection: bool,
    all_settings: bool,
):
    """Drop a setting back to its default.

    The counterpart of 'tdk set': it removes what was stored for every setting
    it is given, so the shipped default applies again. More than one can be
    unset at once.

    Examples:
        tdk unset --hints
        tdk unset --prompt
        tdk unset --server
        tdk unset --coll                        # unpin the collection only
        tdk unset --proj                        # unpins the collection too
        tdk unset --all                         # every setting
    """
    if all_settings:
        hints = prompt = url = project = collection = True
    if not (hints or prompt or url or project or collection):
        raise click.UsageError(_NOTHING_TO_UNSET)
    if hints:
        _clear_enabled(ctx, OPTION_HINTS)
    if prompt:
        _clear_enabled(ctx, OPTION_PROMPT)
    if url:
        _unset_server(ctx)
    if project or collection:
        _unpin(ctx, project)
    store_cli_options(ctx)


@cli_command
@click.command("settings")
@format_option
@click.pass_context
def settings(ctx: click.Context):
    """Show the current settings.

    The machine formats mirror what is stored in 'options.json'.
    """
    hints_enabled = cli_option_enabled(ctx, OPTION_HINTS)
    prompt_enabled = cli_option_enabled(ctx, OPTION_PROMPT)
    current_server = configured_server(ctx)
    project = get_pin(ctx, PIN_PROJECT)
    collection = get_pin(ctx, PIN_COLLECTION)

    def _table():
        click.echo(f"Hints:             {_true_false(hints_enabled)}")
        click.echo(f"Prompt:            {_true_false(prompt_enabled)}")
        click.echo(
            f"Server:            {current_server if current_server else '(none)'}"
        )
        click.echo(f"Pinned project:    {project if project else '(none)'}")
        click.echo(f"Pinned collection: {collection if collection else '(none)'}")

    render(
        ctx,
        data={
            OPTION_HINTS: _stored(hints_enabled),
            OPTION_PROMPT: _stored(prompt_enabled),
            OPTION_SERVER: current_server,
            "pins": {PIN_PROJECT: project, PIN_COLLECTION: collection},
        },
        table=_table,
    )


_TOGGLE_MESSAGES = {
    OPTION_HINTS: (
        "Hints enabled. You will now see hints in the CLI.",
        "Hints disabled. You will no longer see hints in the CLI.",
    ),
    OPTION_PROMPT: (
        "Prompting enabled. Commands will prompt for missing values.",
        "Prompting disabled. Commands will fail instead of prompting for "
        "missing values.",
    ),
}

# Both on/off settings default to true, so unsetting one always lands on true.
_CLEARED_LABELS = {OPTION_HINTS: "Hints", OPTION_PROMPT: "Prompting"}


def _set_enabled(ctx: click.Context, option: str, enabled: bool):
    set_cli_option_enabled(ctx, option, enabled)
    click.echo(_TOGGLE_MESSAGES[option][0 if enabled else 1])


def _clear_enabled(ctx: click.Context, option: str):
    label = _CLEARED_LABELS[option]
    if get_current_cli_option(ctx, option) is None:
        click.echo(f"{label} was already at its default (true).")
    else:
        clear_current_cli_option(ctx, option)
        click.echo(f"{label} unset, back to its default (true).")


def _set_server(ctx: click.Context, url: str):
    set_current_cli_option(ctx, OPTION_SERVER, url)
    click.echo(f"Server set to {url}. 'tdk login' will use it without '--server'.")


def _unset_server(ctx: click.Context):
    previous = configured_server(ctx)
    clear_current_cli_option(ctx, OPTION_SERVER)
    if previous:
        click.echo(f"Server unset (was {previous}).")
    else:
        click.echo("No server was configured.")


def _pin(ctx: click.Context, project: str, collection: str):
    # A collection only resolves relative to a pinned project -- one being set
    # in this same command, or a pre-existing pin. Fail fast otherwise.
    if collection and not (project or get_pin(ctx, PIN_PROJECT)):
        raise click.ClickException(
            "No project pinned. Pin a project first with 'tdk set --proj <name>' "
            "before pinning a collection."
        )
    try:
        if project:
            previously_pinned = get_pin(ctx, PIN_PROJECT)
            if previously_pinned and previously_pinned != project:
                click.echo(f"Unpinning previously pinned project: {previously_pinned}")
            set_pin(ctx, PIN_PROJECT, project)
            click.echo(f"Pinning project: {project}")
            # Switching to a different project orphans a collection pinned under
            # the old one -- clear it, unless this command also sets a new
            # collection (handled below). Re-pinning the same project keeps it.
            if (
                previously_pinned
                and previously_pinned != project
                and not collection
                and get_pin(ctx, PIN_COLLECTION)
            ):
                orphan = get_pin(ctx, PIN_COLLECTION)
                set_pin(ctx, PIN_COLLECTION, None)
                click.echo(
                    "Also unpinning collection (it belonged to the previously "
                    f"pinned project): {orphan}"
                )
        if collection:
            previously_pinned = get_pin(ctx, PIN_COLLECTION)
            if previously_pinned and previously_pinned != collection:
                click.echo(
                    f"Unpinning previously pinned collection: {previously_pinned}"
                )
            set_pin(ctx, PIN_COLLECTION, collection)
            click.echo(f"Pinning collection: {collection}")
        click.echo("Pins updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to set pins: {e}")


def _unpin(ctx: click.Context, project: bool):
    try:
        # Clearing the project always cascades to the collection, since a
        # collection only resolves under a project.
        cleared = []
        if project and get_pin(ctx, PIN_PROJECT):
            cleared.append(f"project ({get_pin(ctx, PIN_PROJECT)})")
        if get_pin(ctx, PIN_COLLECTION):
            cleared.append(f"collection ({get_pin(ctx, PIN_COLLECTION)})")

        if project:
            set_pin(ctx, PIN_PROJECT, None)
        set_pin(ctx, PIN_COLLECTION, None)

        if cleared:
            click.echo("Unpinned: " + ", ".join(cleared))
        else:
            click.echo("Nothing was pinned.")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to unset pins: {e}")


def _true_false(enabled: bool) -> str:
    return _TRUE if enabled else _FALSE


def _stored(enabled: bool) -> str:
    return ENABLED if enabled else DISABLED
