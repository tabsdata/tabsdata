#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import asyncio
import io
from types import SimpleNamespace

import pytest

import tabsdatak.common._tabular as tabular
from tabsdatak.common._tabular import Column, Display, RecordView, TableView

# noinspection PyProtectedMember
from tabsdatak.common._tabular import _browser
from tabsdatak.common._tabular._browser import TableBrowser, ValueScreen

LONG = "a value long enough to be cut by any table that keeps its columns short " * 3
BODY = "### Description\n\nCurrently, pyiceberg metadata is not cached."


def issues() -> TableView:
    return TableView(
        title="Issues",
        columns=[Column("title"), Column("body")]
        + [Column(f"column_{index}") for index in range(20)],
        rows=[[LONG, BODY] + list(range(20)), ["short", None] + list(range(20))],
        count="issues",
        max_width=128,
    )


def run(app: TableBrowser, steps) -> object:
    async def drive():
        async with app.run_test(size=(80, 20)) as pilot:
            return await steps(app, pilot)

    return asyncio.run(drive())


@pytest.mark.common
def test_the_browser_keeps_every_column_at_its_width_and_quits_with_q():
    async def steps(app, pilot):
        shown = app.query_one("#table").virtual_size.width
        await pilot.press("q")
        return shown, app.return_code

    shown, return_code = run(TableBrowser(issues()), steps)

    assert shown > 128 + 80
    assert return_code == 0


@pytest.mark.common
def test_clicking_a_mark_opens_the_whole_value():
    view = TableView(title="Issues", columns=[Column("body")], rows=[[BODY]])

    async def steps(app, pilot):
        mark = 1 + 1 + 1 + len("### Description")
        await pilot.click("#table", offset=(mark, 3))
        await pilot.pause()
        screen = app.screen
        title = str(screen.query_one("#box-title").render())
        value = str(screen.query_one("#box-value").render())
        return isinstance(screen, ValueScreen), title, value

    opened, title, value = run(TableBrowser(view), steps)

    assert opened
    assert title == "body  ·  row 1"
    assert value == BODY


@pytest.mark.common
def test_a_record_value_opens_under_its_field_name():
    record = RecordView(title="Function", fields=[("Description", BODY)])

    async def steps(app, pilot):
        app.action_show_value(0, 1)
        await pilot.pause()
        return str(app.screen.query_one("#box-title").render())

    assert run(TableBrowser(record), steps) == "Description"


@pytest.mark.common
@pytest.mark.parametrize("close", ["escape", "q", "click outside"])
def test_the_value_window_closes_with_esc_q_or_a_click_outside(close):
    async def steps(app, pilot):
        app.action_show_value(0, 1)
        await pilot.pause()
        if close == "click outside":
            await pilot.click(offset=(0, 0))
        else:
            await pilot.press(close)
        await pilot.pause()
        return isinstance(app.screen, ValueScreen)

    assert run(TableBrowser(issues()), steps) is False


@pytest.mark.common
def test_a_click_inside_the_value_window_keeps_it_open():
    async def steps(app, pilot):
        app.action_show_value(0, 1)
        await pilot.pause()
        await pilot.click("#box-value", offset=(2, 0))
        await pilot.pause()
        return isinstance(app.screen, ValueScreen)

    assert run(TableBrowser(issues()), steps) is True


@pytest.mark.common
@pytest.mark.parametrize("key", ["q", "escape"])
def test_q_or_esc_quits_the_browser(key):
    async def steps(app, pilot):
        await pilot.press(key)
        await pilot.pause()
        return app.is_running

    assert run(TableBrowser(issues()), steps) is False


@pytest.mark.common
def test_esc_in_the_value_window_closes_only_the_window():
    async def steps(app, pilot):
        app.action_show_value(0, 1)
        await pilot.pause()
        await pilot.press("escape")
        await pilot.pause()
        return app.is_running, isinstance(app.screen, ValueScreen)

    assert run(TableBrowser(issues()), steps) == (True, False)


@pytest.mark.common
@pytest.mark.parametrize(
    "platform, term_program, key",
    [
        ("linux", None, "Shift"),
        ("win32", None, "Shift"),
        ("darwin", "Apple_Terminal", "fn"),
        ("darwin", "iTerm.app", "⌥"),
        ("darwin", None, "⌥"),
    ],
)
def test_the_select_key_depends_on_the_terminal(
    monkeypatch, platform, term_program, key
):
    monkeypatch.setattr(_browser, "sys", SimpleNamespace(platform=platform))
    if term_program is None:
        monkeypatch.delenv("TERM_PROGRAM", raising=False)
    else:
        monkeypatch.setenv("TERM_PROGRAM", term_program)

    assert _browser.select_key() == key


@pytest.mark.common
def test_the_footers_explain_the_keys():
    key = _browser.select_key()

    assert _browser.footer_text().plain == (
        f"q/esc Quit  ·  wheel Scroll  ·  … Whole value  ·  {key}+drag Select"
    )
    assert _browser.value_footer_text().plain == (
        f"esc Close  ·  click outside Close  ·  {key}+drag Select"
    )


@pytest.mark.common
def test_browse_on_a_terminal_opens_the_browser(monkeypatch, capsys):
    browsers = []
    monkeypatch.setattr(
        tabular,
        "sys",
        SimpleNamespace(stdout=SimpleNamespace(isatty=lambda: True)),
    )
    monkeypatch.setattr(TableBrowser, "run", lambda self: browsers.append(self))

    tabular.show(issues(), Display(browse=True))

    assert len(browsers) == 1
    assert capsys.readouterr().out == ""


@pytest.mark.common
def test_browse_without_a_terminal_prints_instead(monkeypatch, capsys):
    browsers = []
    monkeypatch.setattr(TableBrowser, "run", lambda self: browsers.append(self))

    tabular.show(issues(), Display(browse=True))

    assert browsers == []
    assert "Number of issues: 2" in capsys.readouterr().out


@pytest.mark.common
def test_print_never_opens_the_browser(monkeypatch):
    browsers = []
    stream = io.StringIO()
    monkeypatch.setattr(TableBrowser, "run", lambda self: browsers.append(self))
    monkeypatch.setattr(
        tabular,
        "print_view",
        lambda view, display: stream.write(view.title),
    )

    tabular.show(issues(), Display(browse=False))

    assert browsers == []
    assert stream.getvalue() == "Issues"
