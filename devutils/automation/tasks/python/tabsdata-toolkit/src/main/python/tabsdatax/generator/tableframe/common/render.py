#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

if TYPE_CHECKING:
    from tabsdatax.generator.tableframe.engine.compare import Change, Diff
    from tabsdatax.generator.tableframe.engine.generate import GenResult
    from tabsdatax.generator.tableframe.model.model import Report

# Width follows the terminal (80 columns when not attached to one).
console = Console()

_MARKER_STYLES = {"+": "green", "-": "red", "~": "yellow"}


def render_inspect(report: "Report", *, skipped: list[str] | None = None) -> None:
    console.print()
    console.print(
        Panel(
            f"[bold]Polars[/] [cyan]{report.polars_version}[/]  ➜  "
            f"Module [cyan]{report.root_module}[/]",
            box=box.SQUARE,
            expand=False,
        )
    )

    counts: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
    for symbol in report.symbols:
        bucket = counts[(symbol.target_module, symbol.target_class or "—")]
        bucket[0] += 1
        bucket[1] += 1 if symbol.whitelisted else 0

    table = Table(title="Delegatable API", box=box.SQUARE)
    table.add_column("Target module", style="cyan")
    table.add_column("Class")
    table.add_column("Symbols", justify="right")
    table.add_column("Whitelisted", justify="right")
    for (module, class_name), (total, whitelisted) in sorted(counts.items()):
        style = "green" if whitelisted == total else "yellow"
        table.add_row(
            module, class_name, str(total), f"[{style}]{whitelisted}/{total}[/]"
        )
    console.print()
    console.print(table)

    footer = f"Total symbols: [bold]{len(report.symbols)}[/]"
    if skipped:
        footer += f"\n[yellow]Skipped (no introspectable signature):[/] {len(skipped)}"
    console.print(Panel(footer, box=box.SQUARE, expand=False))
    console.print()


def render_compare(diff: "Diff") -> None:
    version_changed = diff.old_polars_version != diff.new_polars_version
    style = "bold red" if version_changed else "dim"
    console.print()
    console.print(
        Panel(
            f"[bold]Polars[/] "
            f"[{style}]{diff.old_polars_version}  ➜  {diff.new_polars_version}[/]",
            box=box.SQUARE,
            expand=False,
        )
    )

    if diff.empty:
        console.print(Panel("[green]No differences[/]", box=box.SQUARE, expand=False))
        console.print()
        return

    for title, ids, marker, color in [
        ("Added (new in polars)", diff.added, "+", "green"),
        ("Removed (gone from polars)", diff.removed, "-", "red"),
    ]:
        if ids:
            console.print(f"[{color}]{title}: {len(ids)}[/]")
            for sid in ids:
                console.print(f"  [{color}]{marker}[/] {sid}")

    if diff.signature_changed:
        console.print()
        console.print(_signature_table(diff.signature_changed))

    for title, changes in [
        ("Whitelist Changes", diff.whitelist_changed),
        ("Hook Changes", diff.hook_changed),
    ]:
        if changes:
            table = Table(title=f"{title}: {len(changes)}", box=box.SQUARE)
            table.add_column("Symbol", style="cyan", overflow="fold")
            table.add_column("Old")
            table.add_column("New")
            for change in changes:
                table.add_row(change.id, change.old, change.new)
            console.print()
            console.print(table)
    console.print()


def _signature_table(changes: list["Change"]) -> Table:
    """One row per distinct change: symbols whose parameters changed the same
    way (e.g. Expr / Then / ChainedThen) share a row."""
    from tabsdatax.generator.tableframe.engine.compare import (
        ParameterChange,
        parameter_changes,
    )

    grouped: dict[tuple[ParameterChange, ...], list[str]] = defaultdict(list)
    for change in changes:
        parameters = parameter_changes(change.old, change.new)
        if parameters is None:
            parameters = [
                ParameterChange("-", change.old),
                ParameterChange("+", change.new),
            ]
        grouped[tuple(parameters)].append(change.id)

    table = Table(
        title=f"Signature Changes: {len(changes)}",
        box=box.SQUARE,
        show_lines=True,
    )
    table.add_column("Symbols", style="cyan", overflow="fold")
    table.add_column("Changes", overflow="fold")
    for parameters, ids in sorted(grouped.items(), key=lambda item: item[1][0]):
        text = Text()
        for index, parameter in enumerate(parameters):
            if index:
                text.append("\n")
            style = _MARKER_STYLES[parameter.marker]
            text.append(f"{parameter.marker} ", style=f"bold {style}")
            text.append(parameter.text, style=style)
        table.add_row("\n".join(ids), text)
    return table


def render_generate(result: "GenResult") -> None:
    console.print()
    summary = (
        f"[bold]Generated[/] [cyan]{result.symbol_count}[/] symbols across "
        f"[cyan]{len(result.modules)}[/] modules"
    )
    if result.tests:
        summary += f"\n[bold]Generated tests:[/] [cyan]{len(result.tests)}[/] files"
    console.print(Panel(summary, box=box.SQUARE, expand=False))

    for module in result.modules:
        console.print(f"  [green]✓[/] {module}")
    for reason, count in sorted(result.deferred.items()):
        console.print(f"  [yellow]Deferred {count}:[/] {reason}")
    for module, symbols in sorted(result.dropped.items()):
        console.print(f"  [red]Dropped from {module}.pyi:[/] {', '.join(symbols)}")
    console.print()
