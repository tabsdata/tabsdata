#
# Copyright 2026 Tabs Data Inc.
#

"""Diff two manifests: the Polars version, the Polars signatures (added /
removed / changed), and the curated whitelist and hook edits."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from tabsdatax.generator.tableframe.model.model import Report


@dataclass
class Change:
    id: str
    old: str
    new: str


@dataclass
class Diff:
    old_polars_version: str
    new_polars_version: str
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    signature_changed: list[Change] = field(default_factory=list)
    whitelist_changed: list[Change] = field(default_factory=list)
    hook_changed: list[Change] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @property
    def empty(self) -> bool:
        return not (
            self.added
            or self.removed
            or self.signature_changed
            or self.whitelist_changed
            or self.hook_changed
            or self.old_polars_version != self.new_polars_version
        )


def compute_diff(old: Report, new: Report) -> Diff:
    old_symbols = old.by_id()
    new_symbols = new.by_id()
    old_ids = set(old_symbols)
    new_ids = set(new_symbols)
    diff = Diff(old.polars_version, new.polars_version)
    diff.added = sorted(new_ids - old_ids)
    diff.removed = sorted(old_ids - new_ids)
    for symbol_id in sorted(old_ids & new_ids):
        old_symbol = old_symbols[symbol_id]
        new_symbol = new_symbols[symbol_id]
        if (old_symbol.signature, old_symbol.returns) != (
            new_symbol.signature,
            new_symbol.returns,
        ):
            diff.signature_changed.append(
                Change(
                    symbol_id,
                    f"{old_symbol.signature} -> {old_symbol.returns}",
                    f"{new_symbol.signature} -> {new_symbol.returns}",
                )
            )
        if old_symbol.whitelisted != new_symbol.whitelisted:
            diff.whitelist_changed.append(
                Change(
                    symbol_id,
                    str(old_symbol.whitelisted),
                    str(new_symbol.whitelisted),
                )
            )
        if old_symbol.hook_name != new_symbol.hook_name:
            diff.hook_changed.append(
                Change(symbol_id, old_symbol.hook_name, new_symbol.hook_name)
            )
    return diff


def perform(old: Path, new: Path, output: Path) -> tuple[Diff, str]:
    diff = compute_diff(
        Report.from_json(old.read_text()),
        Report.from_json(new.read_text()),
    )
    content = diff.to_json()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content)
    return diff, content
