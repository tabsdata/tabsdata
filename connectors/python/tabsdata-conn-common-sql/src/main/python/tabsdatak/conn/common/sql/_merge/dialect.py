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
from typing import TYPE_CHECKING, Literal

from tabsdatak.conn.common.sql._merge import WatermarkCfg

if TYPE_CHECKING:
    import polars as pl


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
        update_qualified: Whether an update item's left-hand side carries the
            target alias. Some dialects require the bare column name there --
            an update item can only ever resolve against the target, so they
            read a qualified name as an error rather than as noise.
        replace: How a batch that loads or truncates the table replaces the
            target's rows with the staged ones, in one statement so no reader
            sees the table emptied. ``"merge"`` is a ``MERGE ... ON FALSE``
            that inserts every staged row and deletes every target row through
            ``WHEN NOT MATCHED BY SOURCE``; ``"overwrite"`` is an
            ``INSERT OVERWRITE INTO ... SELECT``. ``None`` refuses such a
            batch rather than merging it as if nothing had been emptied.
    """

    target_alias: str = "target"
    source_alias: str = "source"
    evolution_modifier: str = ""
    not_matched: str = "WHEN NOT MATCHED"
    wildcard_writes: str | None = None
    update_qualified: bool = True
    replace: Literal["merge", "overwrite"] | None = None


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
        keys_absent_from_target: ``(columns, table)`` -- the target is missing
            key columns, so no incoming row can be matched against it. Optional:
            a dialect that leaves it unset is not checked, which is right for one
            whose engine reports the same thing clearly enough on its own.
    """

    marker_absent: Callable[..., Exception]
    keys_absent: Callable[..., Exception]
    no_shared_columns: Callable[..., Exception]
    ambiguous_column: Callable[..., Exception]
    keys_absent_from_target: Callable[..., Exception] | None = None


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

    def check_watermark_column(self, target: str, watermark_cfg: WatermarkCfg) -> bool:
        """Whether ``target`` already carries rows stamped with
        ``watermark_cfg.transaction_id``, meaning this batch has been merged
        before and merging it again would double-apply it.

        Answering ``False`` is always safe -- it merges -- so the default is to
        say so, and a dialect whose engine can be asked overrides it. A target
        that does not exist, or that has no watermark column yet, has never been
        stamped and so answers ``False`` too.
        """
        return False

    def adapt_batch(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        """The batch as this engine can read it.

        Folded into the coalesce's rewrite.
        """
        return frame

    @abstractmethod
    def staging(
        self, target: str, batch: Path, watermark_cfg: WatermarkCfg | None = None
    ) -> contextlib.AbstractContextManager:
        """Land ``batch`` where the statement can read it, yielding a
        :class:`Staged`.

        Whatever the engine needs -- an upload, a load job, a scratch table --
        and its teardown. The context manager is exited however the merge ends,
        so a dialect that leaves an object behind drops it here.

        When ``watermark_cfg`` is given, the staged relation gains that column
        stamped with the transaction's id, so the rows the merge writes carry
        the stamp :meth:`check_watermark_column` later reads.
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

    def ensure_watermark_column(
        self, target: str, watermark_cfg: WatermarkCfg | None
    ) -> list[str]:
        """Give ``target`` the watermark column if it has not got one, returning
        the columns added.

        Called in :meth:`evolve`'s place under ``schema_evolution="strict"``,
        where the flow has asked for the target's schema to be held still. The
        watermark is the one exception the flow did ask for: without the column
        nothing is ever stamped, so the rerun guard could never fire and naming
        a watermark would quietly do nothing. Under ``"update"`` there is
        nothing to do here -- :meth:`evolve` already sees the column among the
        staged ones.

        The default does nothing, which is right for an engine whose ``MERGE``
        brings the column with it.
        """
        return []
