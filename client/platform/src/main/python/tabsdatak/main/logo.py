#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, fields
from functools import cache
from pathlib import Path

from rich.text import Text

LOGO_IMAGE_ASSET = os.path.join("assets", "manifest", "LOGO.png")
LOGO_ARTWORK_ASSET = os.path.join("assets", "manifest", "LOGO.toml")

LETTER_SPACING_VARIABLE = "TD_LETTER_SPACING"


@dataclass(frozen=True)
class Palette:
    accent: str = ""
    border: str = ""
    brand: str = ""
    bright: str = ""
    keyword: str = ""
    letter: str = ""
    rule: str = ""
    soft: str = ""
    url: str = ""


@dataclass(frozen=True)
class Glyphs:
    arrow: str = "->"
    rule: str = "-"


@dataclass(frozen=True)
class Mark:
    glyph: str
    blank: str
    colours: dict[str, str]
    rows: tuple[str, ...]


@dataclass(frozen=True)
class Wordmark:
    word: str
    glyph: str
    ink: str
    blank: str
    colour: str
    spacing: int
    letters: dict[str, tuple[str, ...]]


@dataclass(frozen=True)
class Artwork:
    palette: Palette
    glyphs: Glyphs
    styles: dict[str, str]
    mark: Mark
    wordmark: Wordmark
    gap: int
    image_width: int


def resource(asset: str) -> Path | None:
    try:
        from tabsdatak.common._constants import TABSDATA_REGION
        from tabsdatak.common._resources.reader import td_resource

        path = td_resource(asset, region=TABSDATA_REGION)
    except Exception:
        return None
    return path if path.is_file() else None


@cache
def artwork() -> Artwork | None:
    path = resource(LOGO_ARTWORK_ASSET)
    if path is None:
        return None
    try:
        with path.open("rb") as file:
            data = tomllib.load(file)
        palette = Palette(**data["palette"])
        mark = data["mark"]
        wordmark = data["wordmark"]
        return Artwork(
            palette=palette,
            glyphs=Glyphs(**data["glyphs"]),
            styles={
                name: coloured(style, palette) for name, style in data["styles"].items()
            },
            mark=Mark(
                glyph=mark["glyph"],
                blank=mark["blank"],
                colours={
                    cell: getattr(palette, colour)
                    for cell, colour in mark["colours"].items()
                },
                rows=tuple(mark["rows"]),
            ),
            wordmark=Wordmark(
                word=wordmark["word"],
                glyph=wordmark["glyph"],
                ink=wordmark["ink"],
                blank=wordmark["blank"],
                colour=getattr(palette, wordmark["colour"]),
                spacing=int(
                    os.environ.get(LETTER_SPACING_VARIABLE, wordmark["spacing"])
                ),
                letters={
                    letter: tuple(rows) for letter, rows in wordmark["letters"].items()
                },
            ),
            gap=int(data["lockup"]["gap"]),
            image_width=int(data["image"]["width"]),
        )
    except (
        OSError,
        AttributeError,
        KeyError,
        TypeError,
        ValueError,
        tomllib.TOMLDecodeError,
    ):
        return None


def coloured(style: str, palette: Palette) -> str:
    colours = {field.name for field in fields(palette)}
    return " ".join(
        getattr(palette, word) if word in colours else word for word in style.split()
    )


def palette() -> Palette:
    drawing = artwork()
    return Palette() if drawing is None else drawing.palette


def glyphs() -> Glyphs:
    drawing = artwork()
    return Glyphs() if drawing is None else drawing.glyphs


def styles() -> dict[str, str]:
    drawing = artwork()
    return {} if drawing is None else drawing.styles


def mark(art: Mark) -> list[Text]:
    lines = []
    for row in art.rows:
        line = Text()
        for cell in row:
            if cell == art.blank:
                line.append(" ")
            else:
                line.append(art.glyph, style=art.colours[cell])
        lines.append(line)
    return lines


def trimmed(letters: Wordmark, character: str) -> tuple[str, ...]:
    art = letters.letters[character]
    keep = max(len(row.rstrip(letters.blank)) for row in art)
    return tuple(row[:keep] for row in art)


def wordmark(letters: Wordmark) -> list[Text]:
    word = letters.word
    rows = [Text() for _ in range(len(letters.letters[word[0]]))]
    for position, character in enumerate(word):
        for index, row in enumerate(trimmed(letters, character)):
            rows[index].append(
                "".join(letters.glyph if cell == letters.ink else " " for cell in row),
                style=letters.colour,
            )
            if position < len(word) - 1:
                rows[index].append(" " * letters.spacing)
    return rows


def centred(lines: list[Text], width: int) -> list[Text]:
    if not lines:
        return []
    block = max(len(line.plain.rstrip()) for line in lines)
    padding = max(0, (width - block) // 2)
    return [Text(" " * padding) + line for line in lines]


def lockup(art: list[Text], letters: list[Text], gap: int) -> list[Text]:
    art_width = max(len(line.plain) for line in art)
    top = (len(art) - len(letters)) // 2
    rows = []
    for index, line in enumerate(art):
        row = line.copy()
        row.append(" " * (art_width - len(line.plain) + gap))
        offset = index - top
        if 0 <= offset < len(letters):
            row.append_text(letters[offset])
        rows.append(row)
    return rows


def emblem(width: int) -> tuple[list[Text], list[str] | None]:
    drawing = artwork()
    if drawing is None:
        return [], None
    text = banner(drawing, width)
    image = resource(LOGO_IMAGE_ASSET)
    if image is None:
        return text, None
    try:
        from tabsdata.expansions.foundation.figure import figure  # noqa: TID251
    except Exception:
        return text, None
    columns = min(drawing.image_width, max(1, width - 2))
    drawn = figure(image, text, width, columns)
    return drawn.text, drawn.pixels


def banner(drawing: Artwork, width: int) -> list[Text]:
    art = mark(drawing.mark)
    letters = wordmark(drawing.wordmark)
    art_width = max(len(line.plain.rstrip()) for line in art)
    word_width = max(len(line.plain.rstrip()) for line in letters)

    if art_width + drawing.gap + word_width <= width:
        return centred(lockup(art, letters, drawing.gap), width)
    if word_width <= width:
        return centred(art, width) + [Text()] + centred(letters, width)
    return centred(art, width)
