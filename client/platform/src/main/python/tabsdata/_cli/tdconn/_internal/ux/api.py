#
#  Copyright 2026. Tabs Data Inc.
#
"""Abstract UX surface the CLI funnels through.

:class:`UX` is the adapter every CLI verb goes through; each method
wraps one or more :class:`~tabsdata._cli.tdconn._internal.os.logic.Logic` calls and
is responsible for presenting the outcome to the user. Concrete
implementations (see :mod:`terminal`) decide *how* to render — a
terminal with ``rich`` tables today, a JSON wire protocol or web UI
later. Keeping UX separate from :class:`Logic` lets alternative
front-ends plug in without re-implementing how commands drive storage.
"""

from __future__ import annotations

import abc
from pathlib import Path

from tabsdata._cli.tdconn._internal.os.logic import Logic
from tabsdata._io.connections.named_connection.named_connection import _ConnDef


def identity(conn_def: _ConnDef) -> tuple[str, str, str]:
    """Pull ``(name, conn_type, conn_mode)`` out of a :class:`_ConnDef`.

    Exposed at the package level so alternative UX implementations can
    reuse the same field-extraction convention without replicating the
    attribute list. ``conn_mode`` is a ``ConnMode`` str-enum; the
    ``.value`` strips the enum wrapper so downstream rendering gets a
    plain string.
    """
    return (
        conn_def.name,
        conn_def.conn_type,
        conn_def.conn_mode.value,
    )


class UX(abc.ABC):
    """Abstract UX adapter over a :class:`Logic` instance.

    Methods mirror the CLI verbs; each returns ``None`` because the
    contract is side-effectful (rendered output). A single UX method
    may invoke ``self.logic`` several times — for example :meth:`add`
    calls :meth:`Logic.add` which returns the persisted
    :class:`_ConnDef` so ``conn_type`` / ``conn_mode`` can be
    displayed without an extra round-trip through :meth:`Logic.get`.
    """

    def __init__(self, logic: Logic, debug: bool = False):
        self.logic = logic
        # ``debug`` is plumbed all the way down from the CLI so each
        # layer can print what it's doing. Stored on the UX instead of
        # threaded through every verb — the CLI sets it once at
        # construction time and every UX method forwards it straight
        # to ``logic`` calls.
        self.debug = debug

    @abc.abstractmethod
    def add(self, conn_file: Path) -> None:
        """Persist the connection in *conn_file* and confirm connectivity."""

    @abc.abstractmethod
    def update(self, name: str, conn_file: Path) -> None:
        """Replace the stored connection *name* with *conn_file* and confirm."""

    @abc.abstractmethod
    def delete(self, name: str) -> None:
        """Delete the connection *name* and confirm what was removed."""

    @abc.abstractmethod
    def info(
        self,
        name: str,
        verify: bool = False,
        details: bool = False,
    ) -> None:
        """Render the identity of the connection *name*.

        When *verify* is ``True``, open a live connection via the
        matching extractor's :meth:`check_connect` before rendering so
        the footer can confirm real connectivity (not just stored-JSON
        structural validity).

        When *details* is ``True``, the full stored JSON is rendered
        below the identity table — useful for inspecting
        ``conn_configs`` (including which fields are ``env:``
        references) without shelling out to ``cat`` / ``jq``.
        """

    @abc.abstractmethod
    def list(self, verify: bool = False) -> None:
        """Render the identity of every stored connection.

        When *verify* is ``True``, each row also includes a
        connectivity status column — the UX runs the registered
        extractor's :meth:`check_connect` per entry and reports ``✓``
        for success, ``✗`` for failure. The list still renders
        completely even when some probes fail.
        """

    @abc.abstractmethod
    def refresh(self, name: str) -> None:
        """Re-validate the connection *name* and confirm."""

    @abc.abstractmethod
    def refresh_all(self) -> None:
        """Re-validate every stored connection and confirm."""
