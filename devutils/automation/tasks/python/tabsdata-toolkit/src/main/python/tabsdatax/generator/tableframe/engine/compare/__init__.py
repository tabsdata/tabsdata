#
# Copyright 2026 Tabsdata Inc.
#

"""Diff two manifests: the Polars version, the Polars signatures (added /
removed / changed), and the curated whitelist and hook edits."""

from __future__ import annotations

import ast
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


@dataclass(frozen=True)
class ParameterChange:
    marker: str  # "+" added, "-" removed, "~" changed
    text: str


def parameter_changes(old: str, new: str) -> list[ParameterChange] | None:
    """What changed between two ``Change`` signatures (``(params) -> ann ->
    returns``), parameter by parameter; ``None`` if either cannot be parsed."""
    old_parameters, old_returns = _parse(old)
    new_parameters, new_returns = _parse(new)
    if old_parameters is None or new_parameters is None:
        return None
    changes = [
        ParameterChange("-", text)
        for name, text in old_parameters.items()
        if name not in new_parameters
    ]
    for name, text in new_parameters.items():
        if name not in old_parameters:
            changes.append(ParameterChange("+", text))
        elif old_parameters[name] != text:
            new_part = text.removeprefix(name).removeprefix(": ").strip()
            changes.append(
                ParameterChange("~", f"{old_parameters[name]}  →  {new_part}")
            )
    if old_returns != new_returns:
        changes.append(ParameterChange("~", f"returns {old_returns}  →  {new_returns}"))
    return changes


def _parse(signature: str) -> tuple[dict[str, str] | None, str]:
    parameters, _, returns = signature.rpartition(" -> ")
    try:
        function = ast.parse(f"def _f{parameters}: ...").body[0]
    except SyntaxError:
        return None, returns
    arguments = function.args  # type: ignore[attr-defined]
    positional = arguments.posonlyargs + arguments.args
    defaults = [None] * (len(positional) - len(arguments.defaults))
    defaults += arguments.defaults
    texts: dict[str, str] = {}
    for argument, default in zip(positional, defaults):
        texts[argument.arg] = _parameter(argument, default)
    if arguments.vararg is not None:
        texts[arguments.vararg.arg] = "*" + _parameter(arguments.vararg, None)
    for argument, default in zip(arguments.kwonlyargs, arguments.kw_defaults):
        texts[argument.arg] = _parameter(argument, default)
    if arguments.kwarg is not None:
        texts[arguments.kwarg.arg] = "**" + _parameter(arguments.kwarg, None)
    return texts, returns


def _parameter(argument: ast.arg, default: ast.expr | None) -> str:
    text = argument.arg
    annotation = argument.annotation
    if annotation is not None:
        # Polars signatures quote their annotations; show them unquoted.
        if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
            text += f": {annotation.value}"
        else:
            text += f": {ast.unparse(annotation)}"
    if default is not None:
        text += f" = {ast.unparse(default)}"
    return text


def perform(old: Path, new: Path, output: Path) -> tuple[Diff, str]:
    diff = compute_diff(
        Report.from_json(old.read_text()),
        Report.from_json(new.read_text()),
    )
    content = diff.to_json()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content)
    return diff, content
