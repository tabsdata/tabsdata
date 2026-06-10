#
# Copyright 2026 Tabs Data Inc.
#

"""Rich rendering of reports for the console."""

from __future__ import annotations

from collections import defaultdict

from rich.console import Console
from rich.table import Table

from .emit.generate import GenResult
from .model.compare import Diff
from .model.model import Report


def render_report(
    report: Report, console: Console, *, skipped: list[str] | None = None
) -> None:
    console.print(
        f"[bold]Polars[/] [cyan]{report.polars_version}[/]  →  "
        f"root module [cyan]{report.root_module}[/]"
    )

    counts: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
    for s in report.symbols:
        bucket = counts[(s.target_module, s.target_class or "—")]
        bucket[0] += 1
        bucket[1] += 1 if s.whitelisted else 0

    table = Table(title="Delegatable surface")
    table.add_column("target module", style="cyan")
    table.add_column("class")
    table.add_column("symbols", justify="right")
    table.add_column("whitelisted", justify="right")
    for (module, klass), (total, white) in sorted(counts.items()):
        style = "green" if white == total else "yellow"
        table.add_row(module, klass, str(total), f"[{style}]{white}/{total}[/]")
    console.print(table)

    console.print(f"total symbols: [bold]{len(report.symbols)}[/]")
    if skipped:
        console.print(
            f"[yellow]skipped (no introspectable signature):[/] {len(skipped)}"
        )


def render_diff(diff: Diff, console: Console) -> None:
    version_changed = diff.old_polars_version != diff.new_polars_version
    style = "bold red" if version_changed else "dim"
    console.print(
        f"[bold]Polars[/] "
        f"[{style}]{diff.old_polars_version} → {diff.new_polars_version}[/]"
    )

    if diff.empty:
        console.print("[green]no differences[/]")
        return

    for title, ids, marker, color in [
        ("added (new in polars)", diff.added, "+", "green"),
        ("removed (gone from polars)", diff.removed, "-", "red"),
    ]:
        if ids:
            console.print(f"[{color}]{title}: {len(ids)}[/]")
            for sid in ids:
                console.print(f"  [{color}]{marker}[/] {sid}")

    for title, changes in [
        ("signature changes", diff.signature_changed),
        ("whitelist changes", diff.whitelist_changed),
        ("hook changes", diff.hook_changed),
    ]:
        if changes:
            table = Table(title=f"{title}: {len(changes)}")
            table.add_column("symbol", style="cyan")
            table.add_column("old")
            table.add_column("new")
            for change in changes:
                table.add_row(change.id, change.old, change.new)
            console.print(table)


def render_generate(result: GenResult, console: Console) -> None:
    console.print(
        f"[bold]generated[/] [cyan]{result.symbol_count}[/] symbols across "
        f"[cyan]{len(result.modules)}[/] modules"
    )
    for module in result.modules:
        console.print(f"  [green]✓[/] {module}")
    for reason, count in sorted(result.deferred.items()):
        console.print(f"  [yellow]deferred {count}:[/] {reason}")
    for module, symbols in sorted(result.dropped.items()):
        console.print(f"  [red]dropped from {module}.pyi:[/] {', '.join(symbols)}")
    if result.tests:
        console.print(f"[bold]generated tests:[/] [cyan]{len(result.tests)}[/] files")
