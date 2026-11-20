#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli._docs._catalog import (
    AmbiguousSymbol,
    Catalog,
    DocEntry,
    UnknownSymbol,
    build_catalog,
)
from tabsdatak.cli._docs._introspect import argument_descriptions
from tabsdatak.cli._errors import ExitCode
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.status_utils.groups import Health


@pytest.fixture(scope="module")
def catalog() -> Catalog:
    return build_catalog()


def _invoke(argv, tmp_path, env=None):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = None

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv, env=env)


def _document(result) -> dict:
    lines = result.stdout.splitlines()
    assert len(lines) == 1, result.stdout
    return json.loads(lines[0])


def test_every_category_has_entries(catalog):
    categories = {entry.category for entry in catalog.entries}
    assert categories == {
        "api",
        "triggers",
        "connectors",
        "tableframe",
        "types",
        "errors",
        "statuses",
        "credentials",
        "practices",
    }


def test_qualified_names_are_unique(catalog):
    names = [entry.qualified_name for entry in catalog.entries]
    assert len(names) == len(set(names))


def test_a_connector_enum_lists_its_values_and_where_to_import_it(catalog):
    entry = catalog.resolve("SrcFileFormat")
    assert entry.kind == "enum"
    assert "`SrcFileFormat.CSV`" in entry.summary
    assert "`tabsdatak.conn.localfile`" in entry.description


def test_bare_name_resolves_to_the_connector_class(catalog):
    entry = catalog.resolve("PostgresSrc")
    assert entry.qualified_name == "tabsdatak.conn.postgres.PostgresSrc"
    assert entry.category == "connectors"
    assert entry.extra["role"] == "source"
    assert entry.extra["collection_group"] == "sources"
    assert entry.connection_types == ["postgres-sql-in"]
    assert "tdk connection template -t postgres-sql-in -f conn.yaml" in entry.see_also
    queries = next(p for p in entry.parameters if p["name"] == "queries")
    assert queries["required"] is True
    assert queries["description"].startswith("SELECT statements")


def test_connection_type_id_resolves_to_its_class(catalog):
    assert catalog.resolve("postgres-sql-out").symbol == "PostgresDest"


def test_qualified_name_resolves(catalog):
    entry = catalog.resolve("tabsdatak.api.publisher")
    assert entry.kind == "function"
    assert entry.category == "api"


def test_tableframe_is_documented_once_with_stub_signatures(catalog):
    entry = catalog.resolve("TableFrame")
    assert entry.category == "tableframe"
    method = catalog.resolve("TableFrame.with_columns")
    assert method.kind == "method"
    assert method.signature.startswith("with_columns(")
    assert "*args, **kwargs" not in method.signature


@pytest.mark.parametrize(
    ("symbol", "summary"),
    [
        ("TableFrame.join", "Join this `TableFrame`"),
        ("Expr.sum", "Return the sum"),
        ("LazyGroupBy.agg", "Reduce each group"),
        ("ExprStringNameSpace.contains", "Return whether each string"),
        ("ExprDateTimeNameSpace.year", "Extract the year"),
        ("col", "Reference one or more columns"),
    ],
)
def test_engine_classes_carry_their_stub_docstrings(catalog, symbol, summary):
    entry = catalog.resolve(symbol)
    assert entry.category == "tableframe"
    assert entry.summary.startswith(summary)


def test_generator_notes_stay_out_of_the_docs(catalog):
    assert "#MANUAL CHANGE" not in catalog.resolve("TableFrame.join").description
    assert catalog.resolve("TableFrame.join").parameters[0]["description"]


def test_a_phrase_falls_back_to_its_words(catalog):
    assert catalog.search("write parquet to s3")[0].symbol.startswith("S3")
    years = [entry.symbol for entry in catalog.search("extract year from date")][:3]
    assert "ExprDateTimeNameSpace.year" in years
    assert catalog.search("zzz qqq") == []


def test_triggers_are_not_repeated_under_api(catalog):
    assert catalog.resolve("CronTrigger").category == "triggers"


def test_error_code_carries_its_message(catalog):
    entry = catalog.resolve("COMMON_11")
    assert entry.kind == "error"
    assert entry.extra["enum"] == "CommonErrorCode"
    assert "{allowed}" in entry.extra["message"]


def test_status_aliases_follow_the_cli_groups(catalog):
    entry = catalog.resolve("fnrun")
    assert entry.symbol == "function_run"
    assert {s["health"] for s in entry.extra["statuses"]} <= {
        health.value for health in Health
    }
    assert any(s["final"] for s in entry.extra["statuses"])
    assert catalog.resolve("plan").symbol == "execution"


def test_per_connector_alias_is_ambiguous(catalog):
    with pytest.raises(AmbiguousSymbol) as caught:
        catalog.resolve("CredentialsSpec")
    assert len(caught.value.candidates) > 1


def test_unknown_symbol_suggests_close_names(catalog):
    with pytest.raises(UnknownSymbol) as caught:
        catalog.resolve("PostgresS")
    assert "PostgresSrc" in [s.symbol for s in caught.value.suggestions]


def test_search_ranks_exact_names_first(catalog):
    matches = catalog.search("join")
    assert matches[0].symbol == "TableFrame.join"


def test_duplicate_qualified_names_are_dropped():
    first = DocEntry("A", "m.A", "api", "class")
    second = DocEntry("A", "m.A", "errors", "class")
    assert Catalog([first, second]).resolve("A") is first


def test_argument_descriptions_join_continuation_lines():
    doc = (
        "Summary.\n\n"
        "Args:\n"
        "    mask: Five-field cron\n"
        "        expression (UTC).\n"
        "    tz (str): Time zone.\n\n"
        "Returns:\n"
        "    ignored: not an argument.\n"
    )
    assert argument_descriptions(doc) == {
        "mask": "Five-field cron expression (UTC).",
        "tz": "Time zone.",
    }


def test_show_prints_the_markdown_page(tmp_path):
    result = _invoke(["docs", "show", "--name", "PostgresSrc"], tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert result.stdout.startswith(
        "# PostgresSrc  (connectors · source · tabsdatak.conn.postgres)"
    )
    assert "| queries | QueriesSpec | yes |" in result.stdout
    assert "tdk connection template -t postgres-sql-in" in result.stdout


def test_show_emits_one_json_document_in_a_machine_run(tmp_path):
    result = _invoke(["docs", "show", "--name", "CronTrigger", "--machine"], tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["symbol"] == "CronTrigger"
    assert document["qualified_name"] == "tabsdatak.api.CronTrigger"
    assert document["category"] == "triggers"
    assert document["kind"] == "class"
    assert [p["name"] for p in document["parameters"]] == ["mask"]
    for key in ("signature", "summary", "members", "connection_types", "see_also"):
        assert key in document


def test_list_is_a_subcommand_not_a_symbol(tmp_path):
    result = _invoke(["docs", "list", "--category", "triggers", "--machine"], tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["truncated"] is False
    assert {item["symbol"] for item in document["items"]} == {
        "AllTablesTrigger",
        "CronTrigger",
        "TablesTrigger",
    }


def test_search_emits_the_list_envelope(tmp_path):
    result = _invoke(["docs", "search", "--query", "with_col", "--machine"], tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    items = _document(result)["items"]
    assert items[0]["symbol"].startswith("TableFrame.with_col")


def test_search_stops_at_max_result_and_says_so(tmp_path):
    result = _invoke(
        ["docs", "search", "--query", "a", "--max-result", "3", "--machine"], tmp_path
    )
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert len(document["items"]) == 3
    assert document["truncated"] is True


def test_category_narrows_resolution(tmp_path):
    result = _invoke(["docs", "show", "--name", "TableFrame", "--category", "api", "--machine"], tmp_path)
    assert result.exit_code == ExitCode.NOT_FOUND, result.output


def test_ambiguous_symbol_is_a_usage_error(tmp_path):
    result = _invoke(["docs", "show", "--name", "CredentialsSpec", "--machine"], tmp_path)
    assert result.exit_code == ExitCode.USAGE, result.output
    error = _document(result)["error"]
    assert error["type"] == "usage"
    assert "tabsdatak.conn.postgres.CredentialsSpec" in error["message"]


def test_unknown_symbol_is_not_found(tmp_path):
    result = _invoke(["docs", "show", "--name", "NoSuchSymbolAnywhere", "--machine"], tmp_path)
    assert result.exit_code == ExitCode.NOT_FOUND, result.output
    error = _document(result)["error"]
    assert error["type"] == "not_found"
    assert "tdk docs search" in error["hint"]


def test_a_program_searching_the_docs_gets_the_20_best_hits():
    from click.testing import CliRunner as _Runner

    from tabsdatak.cli.td.cli import cli as _cli

    result = _Runner().invoke(_cli, ["docs", "search", "--query", "table", "--machine"])
    document = json.loads(result.stdout)
    assert len(document["items"]) == 20 and document["truncated"] is True
    more = _Runner().invoke(
        _cli, ["docs", "search", "--query", "table", "--max-result", "30", "--machine"]
    )
    assert len(json.loads(more.stdout)["items"]) == 30
