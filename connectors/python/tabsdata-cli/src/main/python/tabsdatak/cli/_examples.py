#
# Copyright 2026 Tabsdata Inc.
#

"""The bundled examples under ``tdresources/examples``: single-file snippets
(``<name>.py`` whose module docstring is YAML metadata) and project folders
(``<name>/`` described by ``_meta.yaml``)."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from tabsdatak.cli._errors import TdkException

EXAMPLES_RESOURCE = "tdresources/examples"
META_FILE = "_meta.yaml"
README_FILE = "README.txt"
FUNCTIONS_DIR = "functions"
SNIPPET = "snippet"
PROJECT = "project"
FUNCTION_KINDS = ("publisher", "stream_publisher", "transformer", "subscriber")
KINDS = (*FUNCTION_KINDS, "flow")
SNIPPET_KEYS = ("title", "kind", "connectors", "tags", "summary")


@dataclass(frozen=True)
class Example:
    name: str
    form: str
    title: str
    kind: str | None
    connectors: tuple[str, ...]
    tags: tuple[str, ...]
    summary: str
    path: Path

    def brief(self) -> dict:
        return {
            "name": self.name,
            "form": self.form,
            "title": self.title,
            "kind": self.kind,
            "connectors": list(self.connectors),
            "tags": list(self.tags),
            "summary": self.summary,
        }

    def files(self) -> list[Path]:
        if self.form == SNIPPET:
            return [self.path]
        readme = self.path / README_FILE
        functions = self.path / FUNCTIONS_DIR
        return [
            *([readme] if readme.is_file() else []),
            *(sorted(functions.glob("*.py")) if functions.is_dir() else []),
        ]

    def read_files(self) -> list[dict]:
        """``{"path", "content"}`` per file, paths relative to the snippet's
        folder or the project's root."""
        base = self.path.parent if self.form == SNIPPET else self.path
        return [
            {
                "path": file.relative_to(base).as_posix(),
                "content": file.read_text(encoding="utf-8"),
            }
            for file in self.files()
        ]


def matches(
    brief: dict, kind: str | None, connector: str | None, tag: str | None
) -> bool:
    """Whether an example's brief, bundled or custom, passes the filters."""
    return (
        (kind is None or brief["kind"] == kind)
        and (connector is None or connector in brief["connectors"])
        and (tag is None or tag in brief["tags"])
    )


def examples_root() -> Path:
    # noinspection PyProtectedMember
    from tabsdatak.common._resources.reader import td_resource

    try:
        return td_resource(EXAMPLES_RESOURCE)
    except FileNotFoundError as error:
        raise TdkException(
            "internal",
            f"The bundled examples are missing ({error}).",
            hint="Reinstall the tabsdata package.",
        )


def _yaml(text: str):
    from ruamel.yaml import YAML

    return YAML(typ="safe").load(text)


def snippet_metadata(path: Path) -> dict:
    """The YAML metadata in a snippet's module docstring; raises
    ``ValueError`` when it is missing or incomplete."""
    docstring = ast.get_docstring(ast.parse(path.read_text(encoding="utf-8")))
    if not docstring:
        raise ValueError(f"{path.name}: no module docstring.")
    metadata = _yaml(docstring)
    if not isinstance(metadata, dict):
        raise ValueError(f"{path.name}: the module docstring is not a YAML mapping.")
    missing = [key for key in SNIPPET_KEYS if key not in metadata]
    if missing:
        raise ValueError(f"{path.name}: missing metadata keys {', '.join(missing)}.")
    if metadata["kind"] not in KINDS:
        raise ValueError(
            f"{path.name}: kind '{metadata['kind']}' is not one of {', '.join(KINDS)}."
        )
    return metadata


def _snippet(path: Path) -> Example:
    metadata = snippet_metadata(path)
    return Example(
        name=path.stem,
        form=SNIPPET,
        title=str(metadata["title"]).strip(),
        kind=metadata["kind"],
        connectors=tuple(metadata["connectors"] or ()),
        tags=tuple(metadata["tags"] or ()),
        summary=str(metadata["summary"]).strip(),
        path=path,
    )


def _project(path: Path) -> Example:
    meta_path = path / META_FILE
    metadata = (
        _yaml(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else None
    )
    metadata = metadata if isinstance(metadata, dict) else {}
    return Example(
        name=path.name,
        form=PROJECT,
        title=str(metadata.get("title", path.name)).strip(),
        kind=None,
        connectors=(),
        tags=tuple(metadata.get("tags") or ()),
        summary=str(metadata.get("description", "")).strip(),
        path=path,
    )


def load_examples(root: Path) -> list[Example]:
    examples = []
    for entry in sorted(root.iterdir()):
        if entry.name.startswith((".", "_")):
            continue
        if entry.is_dir():
            examples.append(_project(entry))
        elif entry.suffix == ".py":
            examples.append(_snippet(entry))
    return examples


def find_example(root: Path, name: str) -> Example:
    for example in load_examples(root):
        if example.name == name:
            return example
    raise TdkException(
        "not_found",
        f"No example '{name}'.",
        hint="List them with 'tdk examples list'.",
    )
