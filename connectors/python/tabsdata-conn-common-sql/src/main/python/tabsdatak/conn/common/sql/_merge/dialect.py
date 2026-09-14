#
# Copyright 2026 Tabsdata Inc.
#

"""The hooks the shared merge driver needs from a destination.

:class:`MergeDialect` is the engine-facing half of
``if_table_exists="merge"``: staging a batch where the statement can read
it, bringing the target into existence, evolving its schema, and running
SQL. The ordering, the CDC coalesce and the ``MERGE`` itself belong to
the driver (``_driver.merge_batch``) and are the same everywhere.
"""

from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

__all__ = [
    "MergeDialect",
    "MergeErrors",
    "MergeSyntax",
    "Staged",
]


@dataclass(frozen=True)
class Staged:
    """A batch landed where the ``MERGE`` can read it.

    Args:
        relation: The staged rows, spelled the way the statement's ``USING``
            clause needs them -- already quoted, since only the dialect knows
            how.
        columns: ``(name, type)`` per column, in the batch's own order, with
            the names as the engine reports them. The marker column is among
            them: the clauses read it from here.
    """

    relation: str
    columns: list[tuple[str, str]]

    def names(self) -> list[str]:
        return [name for name, _ in self.columns]


@dataclass(frozen=True)
class MergeSyntax:
    """Where the dialects' ``MERGE`` grammars differ.

    Everything not named here is the same in all of them, including the order
    of the clauses: the DELETE precedes the UPDATE because every engine in this
    family takes the first ``WHEN MATCHED`` that claims a row.

    Args:
        target_alias: What the target is called inside the statement.
        source_alias: What the staged rows are called inside the statement.
        evolution_modifier: Placed directly after ``MERGE``. A dialect that
            evolves the target from inside the statement puts its modifier
            here; one that evolves beforehand with ``ALTER`` leaves it empty.
        not_matched: The keyword opening the insert clause -- some dialects
            require the ``BY TARGET`` qualifier that others reject.
        wildcard_writes: The projection an update and an insert write, for a
            dialect that supports a wildcard. ``None`` spells both write lists
            out column by column, which is the only way to keep the marker out
            of a target whose grammar has no ``EXCEPT``.
    """

    target_alias: str = "target"
    source_alias: str = "source"
    evolution_modifier: str = ""
    not_matched: str = "WHEN NOT MATCHED"
    wildcard_writes: str | None = None


@dataclass(frozen=True)
class MergeErrors:
    """The connector's own exceptions for the five ways a merge is refused.

    The driver raises through these rather than importing a connector, so a
    failure carries the destination's error code and message however deep in
    the shared machinery it was detected. Each is called with keywords and must
    return the exception to raise, not raise it.

    Args:
        marker_absent: ``(column, table)`` -- the batch has no marker column,
            so no row's fate can be read.
        keys_absent: ``(columns, table)`` -- the batch is missing key columns,
            named as the user spelled them.
        no_shared_columns: ``(table)`` -- the batch and the target have no
            column in common, so the statement would write nothing.
        ambiguous_column: ``(column, table, matches)`` -- the batch has more
            than one column matching a wanted name once folded.
    """

    marker_absent: Callable[..., Exception]
    keys_absent: Callable[..., Exception]
    no_shared_columns: Callable[..., Exception]
    ambiguous_column: Callable[..., Exception]


class MergeDialect(ABC):
    """Per-destination hooks the shared merge driver needs.

    :categories: spi

    Each SQL destination supplies one concrete subclass. A dialect owns its
    engine's vocabulary -- identifier quoting and folding, where a batch is
    staged, how a target is created, whether and how a schema is evolved --
    and owns nothing about the order those happen in.
    """

    @property
    @abstractmethod
    def syntax(self) -> MergeSyntax:
        """The shape of this engine's ``MERGE`` statement."""

    @property
    @abstractmethod
    def errors(self) -> MergeErrors:
        """The destination's exceptions for a refused merge."""

    @abstractmethod
    def quote_column(self, name: str) -> str:
        """Reference a column in emitted SQL."""

    @abstractmethod
    def fold(self, name: str) -> str:
        """The name's comparable form, per the engine's identifier folding.

        Every set operation the driver performs -- which columns both sides
        have, whether the marker is present -- compares folded names, so a
        target holding a column in a different case than the batch counts as
        having it.
        """

    @abstractmethod
    def staging(self, target: str, batch: Path) -> contextlib.AbstractContextManager:
        """Land ``batch`` where the statement can read it, yielding a
        :class:`Staged`.

        Whatever the engine needs -- an upload, a load job, a scratch table --
        and its teardown. The context manager is exited however the merge ends,
        so a dialect that leaves an object behind drops it here.
        """

    @abstractmethod
    def ensure_target(self, target: str, staged: Staged, marker: str) -> None:
        """Create ``target`` if it does not exist, without the marker column.

        The marker says what to do with a row and is never part of one, so a
        target brought into being here must not carry it: nothing would ever
        write it and every row would read NULL.
        """

    @abstractmethod
    def target_columns(self, target: str) -> list[tuple[str, str]]:
        """``(name, type)`` per column the target currently has."""

    @abstractmethod
    def execute(self, statement: str) -> None:
        """Run one statement against the destination."""

    def evolve(self, target: str, staged: Staged, marker: str) -> list[str]:
        """Bring ``target``'s schema up to what the batch wants, returning the
        columns changed.

        Called only under ``schema_evolution="update"``. The default does
        nothing and reports nothing, which is right for an engine that evolves
        the target from inside the ``MERGE`` -- such a dialect carries its
        modifier in :attr:`MergeSyntax.evolution_modifier` instead. A dialect
        that evolves with ``ALTER`` statements overrides this, and owns the
        policy for a change the engine cannot make safely.
        """
        return []
