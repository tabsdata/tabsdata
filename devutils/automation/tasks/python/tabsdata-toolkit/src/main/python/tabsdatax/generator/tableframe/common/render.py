#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.table import Table

if TYPE_CHECKING:
    from tabsdatax.generator.tableframe.engine.compare import Diff
    from tabsdatax.generator.tableframe.engine.generate import GenResult
    from tabsdatax.generator.tableframe.model.model import Report

THEME = "gruvbox-dark"
DELIMITER_STYLE = "bold #2f7fd1"
RENDER_WIDTH = 1024

console = Console(width=RENDER_WIDTH)


def show_json(title: str, content: str) -> None:
    console.print(Rule(title=title, style=DELIMITER_STYLE, align="left"))
    console.print(Syntax(content, "json", theme=THEME, background_color="default"))
    console.print(Rule(style=DELIMITER_STYLE))


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

    for title, changes in [
        ("Signature Changes", diff.signature_changed),
        ("Whitelist Changes", diff.whitelist_changed),
        ("Hook Changes", diff.hook_changed),
    ]:
        if changes:
            table = Table(title=f"{title}: {len(changes)}", box=box.SQUARE)
            table.add_column("Symbol", style="cyan")
            table.add_column("Old")
            table.add_column("New")
            for change in changes:
                table.add_row(change.id, change.old, change.new)
            console.print()
            console.print(table)
    console.print()


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
