#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from pathlib import Path

from rich import box
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from tabsdatak.server._tdserver.render.assess import (
    DELIMITER_STYLE,
    DETAIL_STYLE,
    LABEL_STYLE,
    titled,
)
from tabsdatak.server._tdserver.render.assess.core.provider import provider_of
from tabsdatak.server._tdserver.render.assess.core.task import Target

SETTINGS_FILE = ".settings.json"

CONCERN = "Instance"

UNSET = "not set"


def instance_folder(instance: str) -> Path:
    return Path.home() / ".tabsdata" / "instances" / instance


def settings_of(instance: str) -> dict:
    path = instance_folder(instance) / SETTINGS_FILE
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except json.JSONDecodeError:
        return {}


def kubeconfig_of(target: Target) -> str | None:
    if target.kubeconfig:
        return target.kubeconfig
    return None


def described(target: Target) -> list[tuple[str, str]]:
    settings = settings_of(target.instance)
    cluster = settings.get("cluster") or {}
    adjusted = cluster.get("adjusted") or {}
    provided = cluster.get("provided") or {}

    provider = target.provider or settings.get("provider")
    context = target.context or adjusted.get("context") or provided.get("context")
    namespace = (
        target.namespace or adjusted.get("namespace") or provided.get("namespace")
    )
    kubeconfig = (
        kubeconfig_of(target)
        or adjusted.get("kubeconfig")
        or provided.get("kubeconfig")
    )

    known = provider_of(provider)
    hosted = known is None or known.hosted

    rows: list[tuple[str, str]] = [("instance", target.instance)]
    if provider:
        rows.append(("provider", provider))
    if settings.get("state"):
        rows.append(("state", settings["state"]))
    if settings.get("version"):
        rows.append(("version", settings["version"]))
    if settings.get("edition"):
        rows.append(("edition", settings["edition"]))

    folder = instance_folder(target.instance)
    if folder.is_dir():
        rows.append(("folder", str(folder)))

    if hosted:
        rows.append(("kubeconfig", str(kubeconfig) if kubeconfig else UNSET))
        if target.current and not context:
            rows.append(("context", "the current one"))
        else:
            rows.append(("context", context or UNSET))
        rows.append(("namespace", namespace or UNSET))

    ports = settings.get("ports") or []
    if ports and not hosted:
        first = min(entry.get("port", 0) for entry in ports)
        last = max(entry.get("port", 0) for entry in ports)
        rows.append(("ports", f"{first} to {last}"))

    return rows


def subject(target: Target) -> Panel:
    rows = described(target)
    width = max(len(label) for label, _ in rows) + 2

    table = Table.grid(padding=(0, 1))
    table.add_column(no_wrap=True)
    table.add_column(overflow="fold")
    for label, value in rows:
        leader = f"{label} " + "." * (width - len(label) - 1)
        table.add_row(
            Text(leader, style=DETAIL_STYLE),
            Text(value, style=LABEL_STYLE if value != UNSET else DETAIL_STYLE),
        )

    return Panel(
        table,
        title=titled(CONCERN),
        title_align="left",
        border_style=DELIMITER_STYLE,
        box=box.SQUARE,
        padding=(0, 1),
    )
