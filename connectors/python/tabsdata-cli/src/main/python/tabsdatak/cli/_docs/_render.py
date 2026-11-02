#
# Copyright 2026 Tabsdata Inc.
#

"""Markdown page for one ``tdk docs`` entry."""

from __future__ import annotations

from tabsdatak.cli._docs._catalog import DocEntry


def _cell(value) -> str:
    if value is None or value == "":
        return "—"
    text = value if isinstance(value, str) else repr(value)
    return " ".join(text.split()).replace("|", "\\|")


def _heading(entry: DocEntry) -> str:
    module = entry.qualified_name.rpartition(".")[0]
    role = entry.extra.get("role", "").replace("_", " ")
    parts = [entry.category, *([role] if role else [entry.kind]), module]
    return f"# {entry.symbol}  ({' · '.join(parts)})"


def _connection_lines(entry: DocEntry) -> list[str]:
    if not entry.connection_types:
        return []
    types = ", ".join(f"`{t}`" for t in entry.connection_types)
    group = entry.extra.get("collection_group")
    lines = [
        f"Connection type: {types}"
        + (f" — collection group `{group}`." if group else ".")
    ]
    if entry.extra.get("connection_class"):
        lines.append(f"Connection class: `{entry.extra['connection_class']}`")
    return [*lines, ""]


def _parameters_table(parameters: list[dict]) -> list[str]:
    if not parameters:
        return []
    lines = [
        "## Parameters",
        "",
        "| name | type | required | default | description |",
        "|---|---|---|---|---|",
    ]
    for p in parameters:
        default = None if p["required"] else p["default"]
        lines.append(
            f"| {p['name']} | {_cell(p['type'])} | {'yes' if p['required'] else 'no'} "
            f"| {_cell(default)} | {_cell(p['description'])} |"
        )
    return [*lines, ""]


def _members_list(members: list[dict]) -> list[str]:
    if not members:
        return []
    lines = ["## Members", ""]
    for member in members:
        summary = f" — {member['summary']}" if member["summary"] else ""
        lines.append(f"- `{member['signature']}`{summary}")
    return [*lines, ""]


def _statuses_table(entry: DocEntry) -> list[str]:
    statuses = entry.extra.get("statuses")
    if not statuses:
        return []
    lines = [
        "## Statuses",
        "",
        "| label | name | health | final |",
        "|---|---|---|---|",
    ]
    for s in statuses:
        lines.append(
            f"| {s['label']} | {s['name']} | {s['health']} | "
            f"{'yes' if s['final'] else 'no'} |"
        )
    lines.append("")
    triggers = entry.extra.get("trigger_types")
    if triggers:
        lines += ["## Trigger types", "", "| label | name |", "|---|---|"]
        lines += [f"| {t['label']} | {t['name']} |" for t in triggers]
        lines.append("")
    return lines


def _error_lines(entry: DocEntry) -> list[str]:
    if entry.kind != "error":
        return []
    return [
        f"Message: `{entry.extra['message']}`",
        f"Raised as: `{entry.extra['exception']}` ({entry.extra['enum']})",
        "",
    ]


def markdown(entry: DocEntry) -> str:
    lines = [_heading(entry), ""]
    lines += _connection_lines(entry)
    if entry.signature:
        lines += ["```python", entry.signature, "```", ""]
    if entry.description:
        lines += [entry.description, ""]
    elif entry.summary:
        lines += [entry.summary, ""]
    lines += _error_lines(entry)
    lines += _parameters_table(entry.parameters)
    lines += _members_list(entry.members)
    lines += _statuses_table(entry)
    if entry.see_also:
        lines += ["## See also", ""]
        lines += [f"- `{hint}`" for hint in entry.see_also]
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
