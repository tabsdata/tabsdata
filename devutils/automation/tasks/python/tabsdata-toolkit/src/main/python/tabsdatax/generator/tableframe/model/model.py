#
# Copyright 2026 Tabsdata Inc.
#

"""The manifest data model.

A manifest is a snapshot of the delegatable Polars API for a given Polars
version, mapped onto a target root module. It doubles as the whitelist: each
symbol carries a ``whitelisted`` flag (default ``True``) and a ``hook_name``
(default ``"-"`` = no hook, transparent wrapping) that the user edits to shape
generation.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field


@dataclass
class Symbol:
    """One delegatable method, property, or function."""

    id: str  # Polars-side identity, e.g. "LazyFrame.join" / "functions.col".
    polars_qualname: str  # Full dotted path of the definition in Polars.
    target_module: (
        str  # Full module the wrapper lands in, e.g. "tabsdatak.tableframe.expr".
    )
    target_class: str | None  # Wrapper class name, or None for free functions.
    signature: str  # The Polars signature, as text.
    returns: str  # The return annotation, as text ("" if none).
    is_property: bool
    whitelisted: bool = True  # Default: generate it.
    # The hook the generated op dispatches (run_hook op name) / generation template.
    # "-" means no hook (transparent wrapping).
    hook_name: str = "-"


@dataclass
class Report:
    polars_version: str
    root_module: str
    symbols: list[Symbol] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, text: str) -> "Report":
        data = json.loads(text)
        symbols = [Symbol(**entry) for entry in data.pop("symbols", [])]
        return cls(symbols=symbols, **data)

    def by_id(self) -> dict[str, Symbol]:
        return {symbol.id: symbol for symbol in self.symbols}
