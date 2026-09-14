#
# Copyright 2026 Tabsdata Inc.
#

"""The merge configuration shared by SQL destinations.

:class:`MergeCfg` is the dialect-agnostic half of
``if_table_exists="merge"``: which columns incoming rows are matched by,
and which column carries each row's ``i`` / ``u`` / ``d`` marker. The
structure and its invariants live here so every SQL destination spells
them the same way.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, TypeAlias

from pydantic import AfterValidator

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.common.sql.error import SqlCommonErrorCode


def _column_validate(value: str) -> str:
    """Reject a name no dialect could resolve.

    Deliberately weak: identifier shape is a dialect question (quoting
    style, legal characters, whether a dot qualifies), so a connector
    tightens this by re-annotating the fields with its own spec rather
    than having a guessed pattern rejected here.
    """
    if not value.strip():
        raise ValueError("a merge column name cannot be empty or blank")
    if "\x00" in value:
        raise ValueError(f"a merge column name cannot hold a NUL byte: {value!r}")
    return value


MERGE_COLUMN_VALIDATOR = AfterValidator(_column_validate)

MergeColumnSpec: TypeAlias = Annotated[str, MERGE_COLUMN_VALIDATOR]
"""A single column named by a `MergeCfg`, in whatever way the target's
dialect spells one."""

MergeColumnsSpec: TypeAlias = list[MergeColumnSpec]
"""A list of `MergeColumnSpec`."""


@_api.dataclass(SqlCommonErrorCode.SQL_COMMON_4, kw_only=True)
class MergeCfg(_api.DescriptorBase):
    """How `if_table_exists="merge"` matches incoming rows onto a target table.

    :categories: destination

    Every incoming row carries a marker in `op_column` saying what to do with
    it, and the marker is what decides which clause of the merge claims the row:

    * `'i'` -- insert the row, if it matches no key.
    * `'u'` -- update the row it matches.
    * `'d'` -- delete the row it matches.

    Anything else matches no clause and the row is passed over.

    An update writes every column both sides share except the keys, which are
    what the row matched on; an insert writes those and the keys. Columns the
    target has and the incoming data does not keep their value on an update and
    take a NULL on an insert.

    Args:
        on: The key columns rows are matched by, joined with `AND`. Must be
            present on both sides.
        op_column: The column holding each row's `i` / `u` / `d` marker. Read
            to decide each row's fate and never written, so the target does not
            need the column and will not gain it.
        watermark_column: The column each row the merge writes is stamped with
            the current transaction's id in, making the merge idempotent: a
            rerun that finds the target already carrying this trx's id writes
            nothing rather than applying the same batch twice.

            Not named here by a user. Every connector exposes the watermark on
            its destination instead -- it belongs to the write rather than to
            the way one table's rows are matched -- and stamps the choice onto
            each config on the way past. This field is what the driver reads.

    Examples:
        A CDC feed whose `op` column says what to do with each row:

        .. code-block:: python

            MergeCfg(on=["order_id", "customer_id"], op_column="op")
    """

    __kind__ = "sqlMergeCfg"

    on: MergeColumnsSpec
    op_column: MergeColumnSpec
    watermark_column: MergeColumnSpec | None = None

    def _fold(self, name: str) -> str:
        """The name's stored form in the target, per the dialect's identifier
        folding. Verbatim here; a connector subclass overrides it."""
        return name

    def _keys(self) -> list[str]:
        return [self._fold(name) for name in self.on]

    def _op_column(self) -> str:
        return self._fold(self.op_column)

    def _watermark_column(self) -> str | None:
        return self._fold(self.watermark_column) if self.watermark_column else None

    def validate(self) -> None:
        """Field validation: `on` is non-empty, and neither `op_column` nor
        `watermark_column` collides with a key column or with each other.

        Both names are compared in their folded form, so a subclass whose
        dialect folds case catches a marker that collides with a key under a
        different spelling.

        Called by the framework; raises `SqlCommonValidateException`.
        """
        if not self.on:
            # noinspection PyProtectedMember
            raise SqlCommonErrorCode.SQL_COMMON_5.exception()
        if self._op_column() in set(self._keys()):
            # noinspection PyProtectedMember
            raise SqlCommonErrorCode.SQL_COMMON_6.exception(column=self.op_column)
        if self.watermark_column and self._watermark_column() in set(self._keys()):
            # noinspection PyProtectedMember
            raise SqlCommonErrorCode.SQL_COMMON_7.exception(
                column=self.watermark_column
            )
        if self.watermark_column and self._watermark_column() == self._op_column():
            # noinspection PyProtectedMember
            raise SqlCommonErrorCode.SQL_COMMON_8.exception(
                column=self.watermark_column
            )


@dataclass(frozen=True)
class WatermarkCfg:
    """The watermark column and the current transaction's id.

    Args:
        column: The column holding the watermark.
        transaction_id: The current transaction's id.
    """

    column: str
    transaction_id: str
