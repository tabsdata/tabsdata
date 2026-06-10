#
# Copyright 2026 Tabs Data Inc.
#

"""Diff two signature reports.

Surfaces what changed between two snapshots: the Polars version, the Polars
signatures (added / removed / changed), and the user's whitelist/hook_name edits.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field

from .model import Report


@dataclass
class Change:
    id: str
    old: str
    new: str


@dataclass
class Diff:
    old_polars_version: str
    new_polars_version: str
    added: list[str] = field(default_factory=list)  # ids only in new
    removed: list[str] = field(default_factory=list)  # ids only in old
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
    o, n = old.by_id(), new.by_id()
    o_ids, n_ids = set(o), set(n)
    diff = Diff(old.polars_version, new.polars_version)
    diff.added = sorted(n_ids - o_ids)
    diff.removed = sorted(o_ids - n_ids)
    for sid in sorted(o_ids & n_ids):
        a, b = o[sid], n[sid]
        if (a.signature, a.returns) != (b.signature, b.returns):
            diff.signature_changed.append(
                Change(
                    sid,
                    f"{a.signature} -> {a.returns}",
                    f"{b.signature} -> {b.returns}",
                )
            )
        if a.whitelisted != b.whitelisted:
            diff.whitelist_changed.append(
                Change(sid, str(a.whitelisted), str(b.whitelisted))
            )
        if a.hook_name != b.hook_name:
            diff.hook_changed.append(Change(sid, a.hook_name, b.hook_name))
    return diff
