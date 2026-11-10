#
# Copyright 2026 Tabsdata Inc.
#


import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._command import TdkCommand
from tabsdatak.cli._credentials import ensure_stateful
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    DISABLED,
    ENABLED,
    OPTION_HINTS,
    OPTION_PROMPT,
    OPTION_SERVER,
    OPTION_TRIGGER_WAIT,
    PIN_COLLECTION,
    PIN_PROJECT,
    PROFILE_KEY,
    SERVER_URL_DEFAULTS_HELP,
    active_profile,
    clear_current_cli_option,
    cli_option_enabled,
    configured_server,
    default_profile,
    format_option,
    get_current_cli_option,
    get_pin,
    hint_common_solutions,
    load_cli_options,
    present,
    progress,
    render,
    set_cli_option_enabled,
    set_current_cli_option,
    set_pin,
    set_profile,
    store_cli_options,
    store_settings,
    validate_profile_name,
    view_options,
)
from tabsdatak.common._tabular import RecordView

_NOTHING_TO_SET = (
    "Nothing to set. Specify at least one setting: --hints true|false, "
    "--prompt true|false, --trigger-wait true|false, --server SERVER, --proj "
    "PROJECT, --coll COLLECTION and/or --profile NAME."
)
_NOTHING_TO_UNSET = (
    "Nothing to unset. Specify at least one setting: --hints, --prompt, "
    "--trigger-wait, --server, --proj, --coll and/or --profile -- or --all for "
    "every one of them but the profile."
)

_TRUE = "true"
_FALSE = "false"
# The on/off settings take the value explicitly, so a command reads as what it
# leaves behind; `tdk unset` restores their default. The same two words render
# them in `tdk settings`.
_BOOL = click.Choice((_TRUE, _FALSE))
_DEFAULTS = {OPTION_HINTS: True, OPTION_PROMPT: True, OPTION_TRIGGER_WAIT: True}


def _valid_profile(ctx: click.Context, param, value):
    return validate_profile_name(value) if value is not None else value


@capability("session", "write")
@cli_command
@click.command("set", cls=TdkCommand, login=False)
@format_option
@click.option(
    "--hints",
    type=_BOOL,
    help="Show the hints the CLI adds to errors. Default: true. Global.",
)
@click.option(
    "--prompt",
    type=_BOOL,
    help=(
        "Prompt for a value a command needs but was not given. Default: true; "
        "'tdk set --prompt false' is the standing form of the root "
        "'--no-prompt' flag. Global."
    ),
)
@click.option(
    "--trigger-wait",
    type=_BOOL,
    help=(
        "Make 'tdk fn trigger' wait for the plan it starts, as '--wait' does; "
        "'--no-wait' still returns at once. Default: true. Global."
    ),
)
@click.option(
    "--server",
    "url",
    metavar="SERVER",
    help=(
        "URL of the Tabsdata Server 'tdk login' uses without '--server'. "
        + SERVER_URL_DEFAULTS_HELP
        + " Global."
    ),
)
@click.option(
    "--proj",
    "-P",
    "project",
    help=(
        "Project to pin as the default of project-scoped commands. Per "
        "profile: pinned in the profile the command runs under."
    ),
)
@click.option(
    "--coll",
    "-c",
    "collection",
    help="Collection to pin (requires a pinned project). Per profile.",
)
@click.option(
    "--profile",
    metavar="NAME",
    callback=_valid_profile,
    help=(
        "Profile commands run under when given no --profile, instead of the "
        "default profile. Global. A --proj or --coll of the same command is "
        "pinned in it."
    ),
)
@click.pass_context
def set_setting(
    ctx: click.Context,
    hints: str,
    prompt: str,
    trigger_wait: str,
    url: str,
    project: str,
    collection: str,
    profile: str,
):
    """Give a setting a value.

    Settings apply to every later invocation. The pinned project and collection
    are per profile: they belong to the profile the command runs under. Every
    other setting is global: one value for all profiles.
    'tdk unset' drops them back to their default and 'tdk settings' shows
    them. More than one can be set at once.

    Examples:
        tdk set --hints false
        tdk set --prompt false
        tdk set --trigger-wait false            # fn trigger returns at once
        tdk set --server http://127.0.0.1:2457
        tdk set --server 127.0.0.1              # http://127.0.0.1:2457
        tdk set --proj sales
        tdk set --proj sales --coll orders
        tdk set --profile dev                   # default profile for commands
        tdk set --profile dev --proj sales      # ...and pin dev's project
    """
    ensure_stateful(ctx, "tdk set")
    shared_given = (
        hints is not None or prompt is not None or trigger_wait is not None or bool(url)
    )
    pins_given = bool(project or collection)
    if not (shared_given or pins_given or profile):
        raise click.UsageError(_NOTHING_TO_SET)
    if profile:
        _set_default_profile(ctx, profile)
    if hints is not None:
        _set_enabled(ctx, OPTION_HINTS, hints == _TRUE)
    if prompt is not None:
        _set_enabled(ctx, OPTION_PROMPT, prompt == _TRUE)
    if trigger_wait is not None:
        _set_enabled(ctx, OPTION_TRIGGER_WAIT, trigger_wait == _TRUE)
    if url:
        _set_server(ctx, url)
    if pins_given:
        _pin(ctx, project, collection)
    if shared_given or profile:
        store_settings(ctx)
    if pins_given:
        store_cli_options(ctx)
    render(ctx, data=lambda: _settings_document(ctx))


@capability("session", "write")
@cli_command
@click.command("unset", cls=TdkCommand, login=False)
@format_option
@click.option("--hints", is_flag=True, help="Restore the hints default (true). Global.")
@click.option(
    "--prompt",
    is_flag=True,
    help="Restore the prompting default (true). Global.",
)
@click.option(
    "--trigger-wait",
    is_flag=True,
    help="Restore the default of 'tdk fn trigger' (true: wait for its plan). Global.",
)
@click.option(
    "--server", "url", is_flag=True, help="Clear the configured server. Global."
)
@click.option(
    "--proj",
    "-P",
    "project",
    is_flag=True,
    help=(
        "Unpin the project. Unpins the collection too, since a collection pin "
        "only resolves under a project. Per profile."
    ),
)
@click.option(
    "--coll",
    "-c",
    "collection",
    is_flag=True,
    help="Unpin the collection. Per profile.",
)
@click.option(
    "--all",
    "all_settings",
    is_flag=True,
    help=(
        "Unset every setting and the pins of the profile: shorthand for "
        "--hints, --prompt, --trigger-wait, --server, --proj and --coll, leaving "
        "every one of them at its default. The profile set with 'tdk set "
        "--profile' stays as it is."
    ),
)
@click.option(
    "--profile",
    is_flag=True,
    help=(
        "Drop the profile set with 'tdk set --profile', so commands given no "
        "--profile use the default profile again."
    ),
)
@click.pass_context
def unset_setting(
    ctx: click.Context,
    hints: bool,
    prompt: bool,
    trigger_wait: bool,
    url: bool,
    project: bool,
    collection: bool,
    all_settings: bool,
    profile: bool,
):
    """Drop a setting back to its default.

    The counterpart of 'tdk set': it removes what was stored for every setting
    it is given, so the shipped default applies again. The pinned project and
    collection are per profile, every other setting is global. More than one
    can be unset at once.

    Examples:
        tdk unset --hints
        tdk unset --prompt
        tdk unset --trigger-wait
        tdk unset --server
        tdk unset --coll                        # unpin the collection only
        tdk unset --proj                        # unpins the collection too
        tdk unset --all                         # every setting, the profile's pins
        tdk unset --profile                     # back to the default profile
    """
    ensure_stateful(ctx, "tdk unset")
    if all_settings:
        hints = prompt = trigger_wait = url = project = collection = True
    shared_given = hints or prompt or trigger_wait or url
    pins_given = project or collection
    if not (shared_given or pins_given or profile):
        raise click.UsageError(_NOTHING_TO_UNSET)
    if hints:
        _clear_enabled(ctx, OPTION_HINTS)
    if prompt:
        _clear_enabled(ctx, OPTION_PROMPT)
    if trigger_wait:
        _clear_enabled(ctx, OPTION_TRIGGER_WAIT)
    if url:
        _unset_server(ctx)
    if pins_given:
        _unpin(ctx, project)
    if profile:
        _clear_default_profile(ctx)
    if shared_given or profile:
        store_settings(ctx)
    if pins_given:
        store_cli_options(ctx)
    render(ctx, data=lambda: _settings_document(ctx))


@capability("session", "read")
@cli_command
@click.command("settings", cls=TdkCommand, login=False)
@format_option
@view_options()
@click.pass_context
def settings(ctx: click.Context):
    """Show the current settings: the global ones, and the pinned project and
    collection of the profile the command runs under.

    The machine formats give the stored values and name that profile.
    """
    profile = active_profile(ctx)
    hints_enabled = cli_option_enabled(ctx, OPTION_HINTS)
    prompt_enabled = cli_option_enabled(ctx, OPTION_PROMPT)
    trigger_wait = _enabled(ctx, OPTION_TRIGGER_WAIT)
    current_server = configured_server(ctx)
    project = get_pin(ctx, PIN_PROJECT)
    collection = get_pin(ctx, PIN_COLLECTION)

    def _view():
        return RecordView(
            title="Settings",
            fields=[
                ("Profile", profile or "(default)"),
                ("Hints", _true_false(hints_enabled)),
                ("Prompt", _true_false(prompt_enabled)),
                ("Trigger wait", _true_false(trigger_wait)),
                ("Server", present(current_server)),
                ("Pinned project", present(project)),
                ("Pinned collection", present(collection)),
            ],
        )

    render(ctx, data=lambda: _settings_document(ctx), view=_view)


def _settings_document(ctx: click.Context) -> dict:
    return {
        "profile": active_profile(ctx),
        OPTION_HINTS: _stored(cli_option_enabled(ctx, OPTION_HINTS)),
        OPTION_PROMPT: _stored(cli_option_enabled(ctx, OPTION_PROMPT)),
        OPTION_TRIGGER_WAIT: _stored(_enabled(ctx, OPTION_TRIGGER_WAIT)),
        OPTION_SERVER: configured_server(ctx),
        "pins": {
            PIN_PROJECT: get_pin(ctx, PIN_PROJECT),
            PIN_COLLECTION: get_pin(ctx, PIN_COLLECTION),
        },
    }


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
    OPTION_TRIGGER_WAIT: (
        "Trigger wait enabled. 'tdk fn trigger' will wait for its plan unless "
        "given '--no-wait'.",
        "Trigger wait disabled. 'tdk fn trigger' will return at once unless "
        "given '--wait'.",
    ),
}

_CLEARED_LABELS = {
    OPTION_HINTS: "Hints",
    OPTION_PROMPT: "Prompting",
    OPTION_TRIGGER_WAIT: "Trigger wait",
}


def _enabled(ctx: click.Context, option: str) -> bool:
    return cli_option_enabled(ctx, option, default=_DEFAULTS[option])


def _set_enabled(ctx: click.Context, option: str, enabled: bool):
    set_cli_option_enabled(ctx, option, enabled)
    progress(ctx, _TOGGLE_MESSAGES[option][0 if enabled else 1])


def _clear_enabled(ctx: click.Context, option: str):
    label = _CLEARED_LABELS[option]
    default = _true_false(_DEFAULTS[option])
    if get_current_cli_option(ctx, option) is None:
        progress(ctx, f"{label} was already at its default ({default}).")
    else:
        clear_current_cli_option(ctx, option)
        progress(ctx, f"{label} unset, back to its default ({default}).")


def _set_default_profile(ctx: click.Context, profile: str):
    set_current_cli_option(ctx, PROFILE_KEY, profile)
    # A --proj or --coll of this command pins in the profile just set, so the
    # two land together.
    set_profile(ctx, profile)
    load_cli_options(ctx)
    progress(ctx, f"Profile set to {profile}. Commands given no --profile now use it.")


def _clear_default_profile(ctx: click.Context):
    previous = default_profile(ctx)
    if previous:
        clear_current_cli_option(ctx, PROFILE_KEY)
        progress(
            ctx,
            f"Profile unset (was {previous}). Commands given no --profile use the "
            "default profile.",
        )
    else:
        progress(ctx, "No profile was set.")


def _set_server(ctx: click.Context, url: str):
    set_current_cli_option(ctx, OPTION_SERVER, url)
    progress(ctx, f"Server set to {url}. 'tdk login' will use it without '--server'.")


def _unset_server(ctx: click.Context):
    previous = configured_server(ctx)
    clear_current_cli_option(ctx, OPTION_SERVER)
    if previous:
        progress(ctx, f"Server unset (was {previous}).")
    else:
        progress(ctx, "No server was configured.")


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
                progress(
                    ctx, f"Unpinning previously pinned project: {previously_pinned}"
                )
            set_pin(ctx, PIN_PROJECT, project)
            progress(ctx, f"Pinning project: {project}")
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
                progress(
                    ctx,
                    "Also unpinning collection (it belonged to the previously "
                    f"pinned project): {orphan}",
                )
        if collection:
            previously_pinned = get_pin(ctx, PIN_COLLECTION)
            if previously_pinned and previously_pinned != collection:
                progress(
                    ctx, f"Unpinning previously pinned collection: {previously_pinned}"
                )
            set_pin(ctx, PIN_COLLECTION, collection)
            progress(ctx, f"Pinning collection: {collection}")
        progress(ctx, "Pins updated successfully")
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
            progress(ctx, "Unpinned: " + ", ".join(cleared))
        else:
            progress(ctx, "Nothing was pinned.")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to unset pins: {e}")


def _true_false(enabled: bool) -> str:
    return _TRUE if enabled else _FALSE


def _stored(enabled: bool) -> str:
    return ENABLED if enabled else DISABLED
