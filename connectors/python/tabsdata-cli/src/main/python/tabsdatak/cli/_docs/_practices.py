#
# Copyright 2026 Tabsdata Inc.
#

"""The bundled practices under ``tdresources/practices``: one Markdown page
per practice (``<slug>.md``), opened by a YAML front matter block naming its
title, summary, the symbols it applies to and the examples that follow it."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

PRACTICES_RESOURCE = "tdresources/practices"
PRACTICE_KEYS = ("title", "summary", "applies_to", "examples")

_PAGE = re.compile(
    r"\A\s*(?:<!--.*?-->\s*)?---\n(?P<meta>.*?)\n---\n(?P<body>.*)\Z", re.DOTALL
)


@dataclass(frozen=True)
class Practice:
    slug: str
    title: str
    summary: str
    applies_to: tuple[str, ...]
    examples: tuple[str, ...]
    body: str


def practices_root() -> Path:
    # noinspection PyProtectedMember
    from tabsdatak.common._resources.reader import td_resource

    return td_resource(PRACTICES_RESOURCE)


def parse_practice(path: Path) -> Practice:
    """The practice in ``path``; raises ``ValueError`` when its front matter
    is missing or incomplete."""
    from ruamel.yaml import YAML

    match = _PAGE.match(path.read_text(encoding="utf-8").replace("\r\n", "\n"))
    if match is None:
        raise ValueError(f"{path.name}: no front matter.")
    metadata = YAML(typ="safe").load(match["meta"])
    if not isinstance(metadata, dict):
        raise ValueError(f"{path.name}: the front matter is not a YAML mapping.")
    missing = [key for key in PRACTICE_KEYS if key not in metadata]
    if missing:
        keys = ", ".join(missing)
        raise ValueError(f"{path.name}: missing front matter keys {keys}.")
    return Practice(
        slug=path.stem,
        title=str(metadata["title"]).strip(),
        summary=str(metadata["summary"]).strip(),
        applies_to=tuple(metadata["applies_to"] or ()),
        examples=tuple(metadata["examples"] or ()),
        body=match["body"].strip(),
    )


def load_practices(root: Path | None = None) -> list[Practice]:
    root = root or practices_root()
    return [parse_practice(path) for path in sorted(root.glob("*.md"))]
