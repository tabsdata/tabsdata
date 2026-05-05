#
#  Copyright 2026. Tabs Data Inc.
#
"""Terminal UX — renders :class:`UX` output with ``rich``.

:class:`UXTerminal` is the interactive concrete implementation the
``tdconn`` CLI uses. Every command prints a framed wireframe table
with the touched connection's ``name`` / ``conn_type`` / ``conn_mode``
so the user can verify the action took effect against the right
record. Footer lines annotate whether connectivity was verified, a
record was deleted, and so on.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import logging
from pathlib import Path

from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

from tabsdata._cli.tdconn._internal.os.logic import Logic
from tabsdata._cli.tdconn._internal.ux.api import UX, identity
from tabsdata._io.connections.named_connection.named_connection import _ConnDef

_NAME_STYLE = "cyan"
_MUTED = "dim"
_OK = "green"
_ERR = "red"


@contextlib.contextmanager
def _quiet_probe():
    """Silence DEBUG/INFO log traffic while a ``check_connect`` probe runs.

    Drivers pulled in by the extractors — snowflake-connector-python,
    boto3, google-cloud-*, pymongo, sqlalchemy, etc. — dump large
    amounts of DEBUG/INFO to whichever handler the root logger has
    (tabsdata and the connectors both raise some sub-loggers to
    DEBUG). Inside a tight verify loop that noise drowns the result
    the user cares about, so the UX layer globally masks everything
    below WARNING for the duration of the probe and restores the
    previous disable level on exit. Real failures still surface —
    extractors raise Python exceptions on connectivity errors; we
    catch those in the caller.
    """
    previous = logging.root.manager.disable
    logging.disable(logging.INFO)
    try:
        yield
    finally:
        logging.disable(previous)


class UXTerminal(UX):
    """Concrete :class:`UX` that renders to a terminal via ``rich``."""

    def __init__(
        self,
        logic: Logic,
        console: Console | None = None,
        debug: bool = False,
    ):
        super().__init__(logic, debug=debug)
        self.console = console or Console()

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    def add(self, conn_file: Path) -> None:
        # ``logic.add`` only validates + persists the JSON; it never
        # opens a real session. Run an explicit live probe here so
        # the footer's ``✓/✗ connectivity`` claim matches reality.
        name, stored = self.logic.add(conn_file, debug=self.debug)
        self._render_identity(
            stored,
            title=f"Added connection {name!r}",
            footer=self._probe_footer(name),
        )

    def update(self, name: str, conn_file: Path) -> None:
        stored = self.logic.update(name, conn_file, debug=self.debug)
        self._render_identity(
            stored,
            title=f"Updated connection {name!r}",
            footer=self._probe_footer(name),
        )

    def delete(self, name: str) -> None:
        # Fetch the identity *before* deleting so the confirmation line can
        # include the conn_type/conn_mode of the record that just went away.
        stored = self.logic.get(name, debug=self.debug)
        self.logic.remove(name, debug=self.debug)
        self._render_identity(
            stored,
            title=f"Deleted connection {name!r}",
            footer=f"[{_ERR}]✗[/] deleted",
        )

    def info(
        self,
        name: str,
        verify: bool = False,
        details: bool = False,
    ) -> None:
        stored = self.logic.get(name, debug=self.debug)
        footer = None
        if verify:
            # Mirror ``list -V``: don't let a driver exception abort
            # the render — report it as ``✗ connectivity failed`` in
            # the footer instead. The user still sees the identity
            # table, which is the whole point of ``info``.
            footer = self._probe_footer(name)
        self._render_identity(stored, title=f"Connection {name!r}", footer=footer)
        if details:
            self._render_details(stored)

    def list(self, verify: bool = False) -> None:
        defs = self.logic.list(debug=self.debug)
        title = "Connections (verified)" if verify else "Connections"
        table = Table(title=title)
        table.add_column("Name", style=_NAME_STYLE, no_wrap=True)
        table.add_column("Type")
        table.add_column("Mode")
        if verify:
            table.add_column("Verified", justify="center")
        verified_count = 0
        for conn_def in defs:
            n, t, m = identity(conn_def)
            row = [n, t, m]
            if verify:
                try:
                    with _quiet_probe():
                        self.logic.verify(conn_def.name, debug=self.debug)
                except Exception:
                    # Don't abort the whole listing on a single probe
                    # failure — mark the row and keep going.
                    row.append(f"[{_ERR}]✗[/]")
                else:
                    row.append(f"[{_OK}]✓[/]")
                    verified_count += 1
            table.add_row(*row)
        total = len(defs)
        self.console.print()
        self.console.print(table)
        if verify:
            self.console.print(
                f"[{_MUTED}]Total: {total} — verified {verified_count} / {total}[/]",
            )
        else:
            self.console.print(f"[{_MUTED}]Total: {total}[/]")
        self.console.print()

    def refresh(self, name: str) -> None:
        # The ``refresh`` verb at the CLI maps straight to
        # :meth:`Logic.enrich`: the old refresh contract (re-validate
        # the stored JSON) is now folded into enrich on the base
        # layer, and AI-backed logic additionally re-extracts the
        # metadata catalog. ``enrich`` itself does not live-probe
        # ``check_connect``; its metadata-extraction step is
        # best-effort and only warns, so we call ``verify`` explicitly
        # here to keep the footer's ``✓/✗ connectivity`` claim truthful.
        stored, _enriched = self.logic.enrich(name, debug=self.debug)
        self._render_identity(
            stored,
            title=f"Refreshed connection {name!r}",
            footer=self._probe_footer(name),
        )

    def refresh_all(self) -> None:
        results = self.logic.enrich_all(debug=self.debug)
        if not results:
            self.console.print("[dim]No connections to refresh.[/]")
            return
        table = Table(title=f"Refreshed {len(results)} connection(s)")
        table.add_column("Name", style=_NAME_STYLE, no_wrap=True)
        table.add_column("Type")
        table.add_column("Mode")
        table.add_column("Verified", justify="center")
        verified_count = 0
        for conn_def, _enriched in results:
            n, t, m = identity(conn_def)
            try:
                with _quiet_probe():
                    self.logic.verify(conn_def.name, debug=self.debug)
            except Exception:
                table.add_row(n, t, m, f"[{_ERR}]✗[/]")
            else:
                table.add_row(n, t, m, f"[{_OK}]✓[/]")
                verified_count += 1
        total = len(results)
        self.console.print()
        self.console.print(table)
        self.console.print(
            f"[{_MUTED}]Refreshed {total} — verified {verified_count} / {total}[/]",
        )
        self.console.print()

    # ------------------------------------------------------------------
    # Rendering helpers
    # ------------------------------------------------------------------

    def _probe_footer(self, name: str) -> str:
        """Run a live ``verify`` probe and return a footer string.

        Centralizes the try/except dance so ``info -V``, ``refresh``,
        and anything else that wants a "did the DB answer?" footer
        shares one truthful implementation. Driver exceptions (pymysql,
        snowflake-connector, boto3, …) inherit from arbitrary bases —
        catch ``Exception`` here rather than try to enumerate them.
        """
        try:
            with _quiet_probe():
                self.logic.verify(name, debug=self.debug)
        except Exception as exc:
            return f"[{_ERR}]✗[/] connectivity failed: {exc}"
        return f"[{_OK}]✓[/] connectivity verified"

    def _render_identity(
        self,
        conn_def: _ConnDef,
        *,
        title: str,
        footer: str | None = None,
    ) -> None:
        """Render one connection's identity as a two-column table.

        The single-row table doubles as the "wireframe" the user asked
        for — same visual vocabulary as the multi-row list output so
        commands look consistent side by side.
        """
        n, t, m = identity(conn_def)
        table = Table(title=title, show_header=False)
        table.add_column("Field", style=_NAME_STYLE, no_wrap=True)
        table.add_column("Value")
        table.add_row("Name", n)
        table.add_row("Type", t)
        table.add_row("Mode", m)
        self.console.print()
        self.console.print(table)
        if footer:
            self.console.print(footer)
        self.console.print()

    def _render_details(self, conn_def: _ConnDef) -> None:
        """Render the full connection JSON as a syntax-highlighted block.

        Uses :class:`rich.syntax.Syntax` so ``env:`` keys and nested
        objects are easy to scan. Kept inside the terminal UX layer
        rather than printing raw ``json.dumps`` because the rest of
        the CLI already speaks "rich" — mixing styles would feel off.
        """
        # ConnMode is a ``str, Enum`` so json.dumps serializes it as a
        # bare string; asdict() preserves that. Keeps the helper free
        # of custom encoder plumbing.
        pretty = json.dumps(
            dataclasses.asdict(conn_def),
            indent=2,
            sort_keys=False,
        )
        self.console.print(
            Syntax(pretty, "json", theme="ansi_dark", background_color="default"),
        )
        self.console.print()
