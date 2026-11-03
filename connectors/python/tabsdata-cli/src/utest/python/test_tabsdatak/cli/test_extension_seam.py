#
# Copyright 2026 Tabsdata Inc.
#

import rich_click as click

from tabsdatak.cli import extension

CONTRACT = [
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
    "Kind",
    "Machine",
    "RecordView",
    "SERVER_ENV",
    "SERVICE_TOKEN",
    "SERVICE_TOKEN_PREFIX",
    "SESSION",
    "SNIPPET",
    "TOKEN_ENV",
    "TableView",
    "TdkError",
    "TdkException",
    "UNATTENDED_ENV",
    "capability",
    "capability_of",
    "checks_view",
    "classify",
    "cli_command",
    "cli_extension",
    "command_failure",
    "configured_server",
    "connector_kind",
    "connector_types",
    "doc_markdown",
    "docs_catalog",
    "env_server",
    "examples_root",
    "format_option",
    "get_currently_pinned_object",
    "hint_common_solutions",
    "load_examples",
    "logical_prompt",
    "login_server",
    "make_console",
    "max_result_option",
    "mode_option",
    "option_strings",
    "output_format",
    "path_params",
    "present",
    "primary_option",
    "proj_option",
    "render",
    "render_deleted",
    "render_done",
    "render_list",
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


def test_the_extension_contract_is_pinned():
    assert sorted(extension.__all__) == CONTRACT
    for name in CONTRACT:
        assert getattr(extension, name) is not None, name


def test_the_root_command_and_version_resolve_on_call():
    assert isinstance(extension.root_command(), click.Group)
    assert isinstance(extension.version(), str)


def test_the_tabsdata_directory_follows_home_at_call_time(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    assert extension.tabsdata_directory() == str(tmp_path / ".tabsdata")


def test_the_bundled_examples_and_reference_read_through_the_contract():
    examples = extension.load_examples(extension.examples_root())
    assert examples and all(example.name for example in examples)
    entry = next(iter(extension.docs_catalog().entries))
    assert entry.symbol in extension.doc_markdown(entry)
