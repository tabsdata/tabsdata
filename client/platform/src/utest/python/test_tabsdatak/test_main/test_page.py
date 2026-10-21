#
# Copyright 2026 Tabsdata Inc.
#

import re

import pytest
from rich.text import Text

from tabsdatak.main import logo, tabsdata

SOURCE = """\
#
# Copyright 2026 Tabsdata Inc.
#

[heading]First[/]
---
[text]Second[/]

> [text]  A long [term]paragraph[/term]
> that continues.[/]

---
"""


def styles_of(line: Text) -> list[tuple[str, str]]:
    return [(line.plain[span.start : span.end], str(span.style)) for span in line.spans]


@pytest.mark.tabsdatak
def test_sections_skip_comments_and_split_on_separators():
    assert tabsdata.sections(SOURCE) == [
        ["[heading]First[/]"],
        [
            "[text]Second[/]",
            "",
            "> [text]  A long [term]paragraph[/term]",
            "> that continues.[/]",
        ],
        [],
    ]


@pytest.mark.tabsdatak
def test_paragraph_rewraps_keeping_indent_and_styles():
    lines = ["> [text]  A long [term]paragraph[/term]", "> that continues.[/]"]
    rendered = tabsdata.section(lines, 16).plain.split("\n")
    assert rendered == ["  A long", "  paragraph that", "  continues."]


@pytest.mark.tabsdatak
def test_rewrapped_lines_carry_their_styles():
    paragraph = Text.from_markup("[text]  one [term]two three[/term] four[/]")
    lines = tabsdata.rewrapped(paragraph, 10)
    assert [line.plain for line in lines] == ["  one two", "  three", "  four"]
    assert ("two", "term") in styles_of(lines[0])
    assert ("three", "term") in styles_of(lines[1])
    assert all(line.plain.startswith("  ") for line in lines)


@pytest.mark.tabsdatak
def test_lines_without_prefix_are_kept_as_written():
    lines = ["[command]  tdk login   [/][text]Connect[/]"]
    assert tabsdata.section(lines, 20).plain == "  tdk login   Connect"


@pytest.mark.tabsdatak
def test_installed_page_uses_only_defined_styles():
    source = tabsdata.page_source()
    if source is None:
        pytest.skip("The page resource is not reachable from this environment")
    defined = set(logo.styles())
    if not defined:
        pytest.skip("The artwork resource is not reachable from this environment")
    used = set(re.findall(r"(?<!\\)\[([a-z]+)\]", source))
    assert used <= defined
