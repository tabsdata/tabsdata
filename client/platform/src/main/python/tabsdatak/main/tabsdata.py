#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import shutil
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rich.text import Text

    from tabsdatak.main.logo import Glyphs, Palette

MINIMUM_WIDTH = 80
MAXIMUM_WIDTH = 118
FRAME = 4
RULE_INSET = 2
IMAGE_ROW_OFFSET = 2

PAGE_RESOURCE = "resources/main/tabsdata.page"
COMMENT_PREFIX = "#"
SECTION_SEPARATOR = "---"
PARAGRAPH_PREFIX = "> "

UNKNOWN_VERSION = "unknown"
FALLBACK_VERSION = "2.0.0"


def version() -> str:
    from importlib.metadata import PackageNotFoundError, version as installed

    try:
        return installed("tabsdata")
    except PackageNotFoundError:
        return UNKNOWN_VERSION


def page_source() -> str | None:
    try:
        from tabsdatak.common._resources.reader import td_resource

        return td_resource(PAGE_RESOURCE).read_text(encoding="utf-8")
    except Exception:
        return None


def terminal_width() -> int:
    columns = shutil.get_terminal_size(fallback=(MINIMUM_WIDTH, 24)).columns
    return max(MINIMUM_WIDTH, min(columns, MAXIMUM_WIDTH))


def sections(source: str) -> list[list[str]]:
    found: list[list[str]] = [[]]
    for line in source.splitlines():
        if line.startswith(COMMENT_PREFIX):
            continue
        if line == SECTION_SEPARATOR:
            found.append([])
            continue
        found[-1].append(line)
    return [trimmed(section) for section in found]


def trimmed(lines: list[str]) -> list[str]:
    start = 0
    end = len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return lines[start:end]


def rewrapped(paragraph: Text, inner: int) -> list[Text]:
    from textwrap import wrap

    plain = paragraph.plain
    depth = len(plain) - len(plain.lstrip(" "))
    indent = paragraph[:depth]
    body = paragraph[depth:]
    lines = []
    cursor = 0
    for line in wrap(body.plain, max(1, inner - depth)):
        start = body.plain.index(line, cursor)
        cursor = start + len(line)
        lines.append(indent.copy() + body[start:cursor])
    return lines


def section(lines: list[str], inner: int) -> Text:
    from rich.text import Text

    rendered: list[Text] = []
    paragraph: list[str] = []

    def flush() -> None:
        if paragraph:
            markup = Text.from_markup(" ".join(paragraph), emoji=False)
            rendered.extend(rewrapped(markup, inner))
            paragraph.clear()

    for line in lines:
        if line.startswith(PARAGRAPH_PREFIX):
            paragraph.append(line.removeprefix(PARAGRAPH_PREFIX))
            continue
        flush()
        rendered.append(Text.from_markup(line, emoji=False))
    flush()
    return Text("\n").join(rendered)


def build(
    width: int,
    page: list[list[str]],
    banner: list[Text],
    image: list[str] | None,
    palette: Palette,
    glyphs: Glyphs,
):
    from rich.text import Text

    inner = width - FRAME

    def rule() -> Text:
        return Text(
            " " * RULE_INSET + glyphs.rule * max(0, inner - RULE_INSET * 2),
            style=palette.rule,
        )

    group = [Text() for _ in image] if image else list(banner)
    for lines in page:
        if group:
            group.extend([Text(), rule(), Text()])
        group.append(section(lines, inner))
    return group


def main() -> int:
    from string import Template

    from rich.color import ColorSystem
    from rich.console import Console, Group
    from rich.panel import Panel
    from rich.style import Style
    from rich.theme import Theme

    from tabsdatak.main import logo

    source = page_source()
    if source is None:
        print(
            f"tabsdata: missing {PAGE_RESOURCE} in this installation", file=sys.stderr
        )
        return 1

    width = terminal_width()
    palette = logo.palette()
    glyphs = logo.glyphs()
    console = Console(
        width=width,
        color_system="truecolor",
        theme=Theme(logo.styles(), inherit=False),
    )

    release = version()
    page = sections(
        Template(source).safe_substitute(
            release=release,
            pin=release if release != UNKNOWN_VERSION else FALLBACK_VERSION,
            arrow=glyphs.arrow,
        )
    )

    banner, image = logo.emblem(width - FRAME)
    panel = Panel(
        Group(*build(width, page, banner, image, palette, glyphs)),
        border_style=palette.border,
        padding=(1, 1),
        width=width,
    )

    console.print()
    if image is None:
        console.print(panel)
        console.print()
        return 0

    with console.capture() as captured:
        console.print(panel)
    rendered = captured.get().rstrip("\n").split("\n")

    edge = Style.parse(palette.border).render(
        panel.box.mid_left, color_system=ColorSystem.TRUECOLOR
    )
    for offset, row in enumerate(image):
        rendered[IMAGE_ROW_OFFSET + offset] = f"{edge}{row}\r\x1b[{width - 1}C{edge}"
    console.file.write("\n".join(rendered) + "\n\n")
    console.file.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
