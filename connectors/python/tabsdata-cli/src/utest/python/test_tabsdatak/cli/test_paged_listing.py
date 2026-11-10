#
# Copyright 2026 Tabsdata Inc.
#

"""A listing tdk holds itself, such as the bundled reference, pages with
``--cursor`` like a server listing."""

from __future__ import annotations

import json

from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli


def _pages(argv: list[str]) -> list[dict]:
    pages, cursor = [], None
    while True:
        more = ["--cursor", cursor] if cursor else []
        result = CliRunner().invoke(cli, [*argv, *more, "--machine"])
        assert result.exit_code == 0, result.output
        pages.append(json.loads(result.stdout))
        cursor = pages[-1]["next"]
        if cursor is None:
            return pages


def _all_symbols() -> list[str]:
    result = CliRunner().invoke(cli, ["docs", "list", "--max-result", "0", "-o", "jsonb"])
    return [item["symbol"] for item in json.loads(result.stdout)["items"]]


def test_every_row_comes_back_once_across_pages(machine_settings):
    machine_settings.update(max_result=1000)
    pages = _pages(["docs", "list", "--max-result", "40", "--fields", "symbol"])
    symbols = [item["symbol"] for page in pages for item in page["items"]]
    assert len(pages) > 1
    assert symbols == _all_symbols()
    assert "showing the next 40" in pages[1]["hint"]


def test_a_byte_cut_resumes_where_it_stopped(machine_settings):
    machine_settings.update(max_result=1000, max_bytes=8192)
    pages = _pages(["docs", "list", "--max-result", "1000"])
    symbols = [item["symbol"] for page in pages for item in page["items"]]
    assert symbols == _all_symbols()
    assert all(len(json.dumps(page)) <= 8192 for page in pages)
    assert "all that fit in one result" in pages[0]["hint"]


def test_a_cursor_only_continues_its_own_listing():
    first = CliRunner().invoke(cli, ["docs", "list", "--max-result", "5", "-o", "jsonb"])
    cursor = json.loads(first.stdout)["next"]
    argv = ["docs", "list", "--category", "api", "--cursor", cursor, "-o", "jsonb"]
    result = CliRunner().invoke(cli, argv)
    assert result.exit_code == 2
    assert json.loads(result.stdout)["error"]["type"] == "usage"
