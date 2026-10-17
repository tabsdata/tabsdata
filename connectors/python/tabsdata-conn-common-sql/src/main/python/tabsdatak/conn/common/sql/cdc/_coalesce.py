#
# Copyright 2026 Tabsdata Inc.
#

"""Reduce a CDC batch to one row per key, ready to merge into a target table.

A captured batch may carry several change records for the same key. A
merge statement cannot consume that: it matches each incoming row against
the target once, so every key must arrive as a single record -- one
operation plus one payload. :func:`coalesce_cdc_batch` performs that
reduction as a pure, uncollected ``LazyFrame`` transformation.

Its output is what a destination merging with
``tabsdatak.conn.common.sql._merge.MergeCfg`` expects: pass the same
columns as that config's ``on`` and ``op_column``.

The reduction is **net state**, not a fold over consecutive operation
pairs. For one key, only the two endpoints of its record sequence matter:

* it existed before the batch <=> its first marker is not an insert
* it exists after the batch <=> its last marker is not a delete

which gives insert (born in this batch), update (pre-existing and still
present) or delete (gone). Folding the pairwise rules instead
(``i -> u == i``, ``u -> d == d``, ``d -> i == u``, ``i -> d == d``) loses
track of whether the row pre-existed the batch, so
``i -> u -> d -> i`` folds to an update for a key the target has never
seen; net state correctly reports an insert.

A sequence that both starts with an insert and ends with a delete
describes a row that only ever existed inside the batch. Its net effect
on the target is nothing, so **the key is dropped**: issuing a tombstone
for a key the target has never seen is a no-op at best and noise in the
merge statistics. Callers wanting the audit-grain reading -- the batch did
contain a delete, so record it -- cannot get it from this function.

The markers are :class:`CdcOperation`'s -- the row operations ``"i"``,
``"u"``, ``"d"`` and the control operations ``"l"`` (load) and ``"t"``
(truncate) -- the same ones every CDC source in this package emits, read
case-insensitively so a feed spelling them ``"I"`` / ``"U"`` / ``"D"``
works unchanged. The resolved marker is always written back in lower
case, so the output is normalized whatever the input looked like.

A control record **resets** the table: it is emptied at that position.
Only the batch's last reset matters, since it voids everything before it
for every key, so the records up to and including it are discarded and
what follows is reduced against an empty table -- every key that
survives is an insert, and a key whose last record is a delete is
dropped. Whether a batch resets at all is not visible in the output; ask
:func:`cdc_batch_resets`, and empty the target before applying a batch
that does.

A record carrying anything else -- an unrecognized marker, or none at all
-- is dropped whole, payload included, before any of the reduction runs.
An unreadable marker means the record's intent is unknown, and every way
of guessing it writes something wrong: treating it as an update invents a
change, ignoring only the marker keeps a payload nothing asked for. A key
whose every record is unreadable therefore leaves the output entirely.
Matching is exact beyond case -- a padded ``" d "`` is not a delete -- so
a feed spelling its markers differently has to map them onto
`CdcOperation` first.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tabsdatak.conn.common.sql.cdc.constants import (
    OP_DELETE,
    OP_INSERT,
    OP_LOAD,
    OP_TRUNCATE,
    OP_UPDATE,
)
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode

if TYPE_CHECKING:
    import polars as pl

__all__ = ["cdc_batch_resets", "coalesce_cdc_batch"]


# Helper columns live in the reserved ``@td.cdc.`` namespace so they
# cannot collide with a data column. The check that would otherwise be
# needed -- "is this name already in the frame?" -- requires resolving the
# schema, which this function must not do; a collision would silently
# overwrite the user's column and then drop it.
_COALESCE_PREFIX = "@td.cdc.coalesce."

_SEQ = _COALESCE_PREFIX + "seq"
_KEEP = _COALESCE_PREFIX + "keep"
_OP = _COALESCE_PREFIX + "op"
_EPHEMERAL = _COALESCE_PREFIX + "ephemeral"
_LOWER = _COALESCE_PREFIX + "lower"
_RESET = _COALESCE_PREFIX + "reset"

# ``CdcOperation`` subclasses ``str``, but ``str()`` on a 3.12 enum member
# yields ``"CdcOperation.INSERT"``, so the stored text is taken explicitly.
_INSERT = OP_INSERT.value
_UPDATE = OP_UPDATE.value
_DELETE = OP_DELETE.value

_MARKERS = [_INSERT, _UPDATE, _DELETE]
_RESETS = [OP_LOAD.value, OP_TRUNCATE.value]


def _lowered(op_column: str) -> pl.Expr:
    """The marker as lower-case text. ``cast(String)`` is what lets the
    ``str`` namespace reach an Enum / Categorical marker."""
    import polars as pl

    return pl.col(op_column).cast(pl.String).str.to_lowercase()


def cdc_batch_resets(lf: pl.LazyFrame, op_column: str) -> pl.LazyFrame:
    """Whether the batch carries a control record that empties the table.

    Stays lazy like :func:`coalesce_cdc_batch`: the result is a one-row,
    one-column boolean frame for the caller to collect. A batch that resets
    has to be applied onto an emptied target, since the coalesced rows
    describe the whole table from the last reset on, not changes to it.

    Args:
        lf: The batch, as passed to :func:`coalesce_cdc_batch`.
        op_column: Column holding each record's `CdcOperation` marker,
            matched case-insensitively.
    """
    return lf.select(_lowered(op_column).is_in(_RESETS).any())


def coalesce_cdc_batch(
    lf: pl.LazyFrame,
    key_columns: list[str],
    op_column: str,
    *,
    order_column: str | None = None,
    op_dtype: pl.DataType | None = None,
) -> pl.LazyFrame:
    """Reduce a CDC batch to one row per key, resolving each key's records
    to a single operation and payload.

    Stays lazy: the frame is never collected and its schema is never
    resolved, so the result composes into a larger plan and can be built
    over a scan that has not run.

    Each surviving row keeps the payload of its key's *latest* record,
    except for a resolved delete, which keeps the *last known image* --
    the last non-delete record, or the first record when every record is a
    delete. Delete records are assumed to carry no usable payload; where
    the capture format populates them with a full before-image, this
    exception is harmless (it picks the same values).

    Keys whose records start with an insert and end with a delete are
    dropped entirely -- see the module docstring.

    A load or truncate record voids every record up to and including the
    batch's last one, and what follows is resolved against an empty table
    -- see the module docstring.

    Args:
        lf: The batch. Must carry the key, operation and (when given)
            order columns; every other column is passed through
            untouched.
        key_columns: The column names forming the primary key that
            records are grouped by. A list even for a single key column,
            so that a bare `str` -- which would group by its characters
            -- is a type error rather than a puzzle at collect time.
        op_column: Column holding each record's `CdcOperation` marker --
            `"i"`, `"u"`, `"d"`, `"l"` or `"t"`, matched case-insensitively.
            A record carrying anything else is dropped, and so is every load
            or truncate record once it has cut the batch. Rewritten in place
            with the resolved operation in lower case, keeping its dtype
            (see `op_dtype`) and its position in the frame.
        order_column: Column establishing the arrival order of records
            *within* a key, and of a reset against every record -- a log
            sequence number, binlog offset or commit sequence. Must be
            unique inside a key group, and order the records following a
            reset strictly after it: rows are selected by comparing against
            this value, so a tie inside one key emits several rows. Defaults
            to `None`, which numbers the frame positionally; that is well
            defined for a single materialized batch, but downstream of a
            partitioned or streaming scan the physical order need not be
            commit order, and the semantics are asymmetric (``i -> d`` and
            ``d -> i`` differ), so passing a real sequence column is
            strongly preferred wherever the format carries one.
        op_dtype: Dtype to write the resolved operation as. Needed only
            when `op_column` is an `Enum` or `Categorical`, whose dtype
            cannot be read here without resolving the schema and which a
            plain literal would widen to `String`. Its categories have to
            admit the lower-case markers, since that is what gets written.
            Defaults to `None`, i.e. `String`.

    Returns:
        A `LazyFrame` with the same schema as `lf`, holding at most one
        row per key -- and no row for a key whose every record was
        dropped -- in the relative order the surviving rows had in the
        input.

    Raises:
        SqlCdcException: If `key_columns` is empty, if `op_column` or
            `order_column` is also a key column, or if `order_column` is
            the `op_column`.
    """
    import polars as pl

    keys = list(key_columns)
    if not keys:
        raise SqlCdcErrorCode.CDC_16.exception()
    if op_column in keys:
        raise SqlCdcErrorCode.CDC_17.exception(column=op_column)
    if order_column is not None:
        if order_column in keys:
            raise SqlCdcErrorCode.CDC_18.exception(column=order_column)
        if order_column == op_column:
            raise SqlCdcErrorCode.CDC_19.exception(column=order_column)

    # The marker is normalized once into a helper column so every
    # expression below reads the same text, and so the value the filter
    # accepts is the value the reduction matches on.
    out = lf.with_columns(_lowered(op_column).alias(_LOWER))

    # Unreadable records go before any window sees them, so they cannot
    # sway a key's endpoints or leave their payload behind. ``is_in``
    # yields null for a null marker, which this filter drops too.
    out = out.filter(pl.col(_LOWER).is_in(_MARKERS + _RESETS))

    # The ordinal is materialized as a column because several expressions
    # read it. ``int_range(len())`` rather than ``with_row_index`` so the
    # only frame methods used are with_columns / filter / drop -- none of
    # which resolve the schema. It numbers what survived the filter, which
    # preserves arrival order because a filter cannot reorder rows.
    ordinal = (
        pl.col(order_column) if order_column is not None else pl.int_range(pl.len())
    )
    out = out.with_columns(ordinal.alias(_SEQ))

    seq = pl.col(_SEQ)
    op = pl.col(_LOWER)

    # The batch's last reset, taken over the whole frame rather than per
    # key: it empties the table, so it voids every key's earlier records.
    # The reset records themselves go too, since each sits at or before it.
    last_reset = pl.when(op.is_in(_RESETS)).then(seq).otherwise(None).max()
    out = out.with_columns(last_reset.alias(_RESET))
    out = out.filter(pl.col(_RESET).is_null() | (seq > pl.col(_RESET)))

    # Ordering is by the ordinal alone. The marker must not enter the sort
    # key: `i(A) -> d(B)` and `d(A) -> i(B)` resolve differently yet hold
    # the same multiset of markers, so any order keyed on the marker maps
    # both to one sequence and cannot tell them apart.
    first_op = op.sort_by(seq).first().over(keys)
    last_op = op.sort_by(seq).last().over(keys)

    # Plain comparisons: the filters leave `_LOWER` holding one of the
    # three row markers and nothing else, so no null can reach here to
    # propagate through a comparison. After a reset the table is empty, so
    # no key pre-existed what is left of the batch.
    is_insert = (first_op == _INSERT) | pl.col(_RESET).is_not_null()
    is_delete = last_op == _DELETE

    # Latest record of the key, or -- when the key resolves to a delete --
    # its last non-delete record. The fallback covers an all-delete
    # sequence, where no non-delete ordinal exists.
    keep = (
        pl
        .when(op != _DELETE)
        .then(seq)
        .otherwise(None)
        .max()
        .over(keys)
        .fill_null(seq.min().over(keys))
    )

    resolved = (
        pl
        .when(is_delete)
        .then(pl.lit(_DELETE, dtype=op_dtype))
        .when(is_insert)
        .then(pl.lit(_INSERT, dtype=op_dtype))
        .otherwise(pl.lit(_UPDATE, dtype=op_dtype))
    )

    # Every window is materialized before the filter. Evaluated after it,
    # they would run over the already-collapsed frame -- where
    # `first_op == last_op` -- silently degrading each multi-record key to
    # the surviving record's own marker.
    out = out.with_columns(
        keep.alias(_KEEP),
        resolved.alias(_OP),
        (is_insert & is_delete).alias(_EPHEMERAL),
    )
    out = out.filter(seq == pl.col(_KEEP)).filter(~pl.col(_EPHEMERAL))

    # ``with_columns`` on an existing name replaces in place and keeps the
    # column's position, so restoring the marker needs no column list and
    # therefore no schema.
    out = out.with_columns(pl.col(_OP).alias(op_column))
    return out.drop(_SEQ, _KEEP, _OP, _EPHEMERAL, _LOWER, _RESET)
