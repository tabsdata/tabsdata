#
# Copyright 2026 Tabsdata Inc.
#

"""The contract a package extending tdk builds on: the only names of
``tabsdatak.cli`` an extension imports. The root command and the version are
functions because an extension is imported while ``tabsdatak.cli.td.cli``
itself is still importing; the SDK reference and the Tabsdata directory are
read through functions too, since they import the SDK."""

from tabsdatak.cli._capability import (
    ANY,
    SESSION,
    Capability,
    CapabilityDeclarationError,
    Domain,
    Effect,
    Fs,
    capability,
    capability_of,
    option_strings,
    path_params,
    primary_option,
)
from tabsdatak.cli._command import (
    INTERNAL_PREFIX,
    InternalOption,
    TdkCommand,
    TdkGroup,
    internal_option,
)
from tabsdatak.cli._credentials import (
    BEARER_ENV,
    SERVER_ENV,
    SERVICE_TOKEN,
    SERVICE_TOKEN_PREFIX,
    TOKEN_ENV,
    env_server,
)
from tabsdatak.cli._error_document import (
    SECRET_OPTIONS,
    SECRET_SHORT_OPTIONS,
    error_document,
    redact,
)
from tabsdatak.cli._errors import ExitCode, TdkError, TdkException, classify
from tabsdatak.cli._examples import (
    SNIPPET,
    examples_root,
    load_examples,
    snippet_metadata,
)
from tabsdatak.cli._machine import Machine, running
from tabsdatak.cli._prompted import pinned, prompted
from tabsdatak.cli._registry import cli_command, cli_extension
from tabsdatak.cli._unattended import (
    UNATTENDED_ENV,
    UNATTENDED_MARKERS,
    coding_agent,
    unattended_cause,
)
from tabsdatak.cli.cli_utils import (
    active_profile,
    command_failure,
    configured_server,
    format_option,
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    login_server,
    make_console,
    max_result_option,
    output_format,
    paged_options,
    present,
    proj_option,
    render,
    render_deleted,
    render_done,
    render_list,
    require_word,
    take,
    verify_login_or_prompt,
    version_skew,
    view_options,
)
from tabsdatak.cli.td.config_utils import Action
from tabsdatak.cli.td.connection_group import (
    connector_kind,
    connector_types,
    mode_option,
    type_filter_option,
)
from tabsdatak.cli.td.doctor_group import checks_view
from tabsdatak.common._constants import tabsdata_directory
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView

__all__ = [
    "ANY",
    "Action",
    "BEARER_ENV",
    "Capability",
    "CapabilityDeclarationError",
    "Column",
    "Domain",
    "Effect",
    "ExitCode",
    "Fs",
    "INTERNAL_PREFIX",
    "InternalOption",
    "Kind",
    "Machine",
    "RecordView",
    "SECRET_OPTIONS",
    "SECRET_SHORT_OPTIONS",
    "SERVER_ENV",
    "SERVICE_TOKEN",
    "SERVICE_TOKEN_PREFIX",
    "SESSION",
    "SNIPPET",
    "TOKEN_ENV",
    "TableView",
    "TdkCommand",
    "TdkGroup",
    "TdkError",
    "TdkException",
    "UNATTENDED_ENV",
    "UNATTENDED_MARKERS",
    "active_profile",
    "capability",
    "capability_of",
    "pinned",
    "prompted",
    "checks_view",
    "classify",
    "cli_command",
    "cli_extension",
    "coding_agent",
    "command_failure",
    "configured_server",
    "connector_kind",
    "connector_types",
    "doc_markdown",
    "docs_catalog",
    "env_server",
    "error_document",
    "examples_root",
    "format_option",
    "get_currently_pinned_object",
    "hint_common_solutions",
    "internal_option",
    "load_examples",
    "logical_prompt",
    "login_server",
    "make_console",
    "max_result_option",
    "mode_option",
    "option_strings",
    "paged_options",
    "output_format",
    "path_params",
    "present",
    "primary_option",
    "proj_option",
    "redact",
    "render",
    "render_deleted",
    "render_done",
    "render_list",
    "require_word",
    "root_command",
    "running",
    "snippet_metadata",
    "tabsdata_directory",
    "take",
    "type_filter_option",
    "unattended_cause",
    "verify_login_or_prompt",
    "version",
    "version_skew",
    "view_options",
]


def root_command():
    from tabsdatak.cli.td.cli import cli

    return cli


def version() -> str:
    from tabsdatak.cli.td.cli import __version__

    return __version__


def docs_catalog():
    """The SDK reference ``tdk docs`` reads: its ``entries``, one per
    documented symbol."""
    from tabsdatak.cli._docs._catalog import build_catalog

    return build_catalog()


def doc_markdown(entry) -> str:
    """One reference entry as ``tdk docs show`` prints it."""
    from tabsdatak.cli._docs._render import markdown

    return markdown(entry)
