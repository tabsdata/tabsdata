#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for
:func:`~tabsdatak.conn.common.sql.cdc._coalesce.coalesce_cdc_batch`.

Covers the four specified pairwise rules and the fold counterexample they
break on, net-state resolution (all-delete, single-record and
unrecognized-marker sequences), payload selection (latest record, or last
known image for a delete), dropping of ephemeral keys, ordering by an
explicit sequence column versus positionally, the load and truncate
records that reset the table, schema preservation including `Enum`
markers and composite keys, the laziness contract (no `collect`, no
`collect_schema`), the fixed `CdcOperation` markers, and argument
validation.

The semantics are pinned twice: once by hand-written cases, and once by
cross-checking every resolution against a reference implementation that
literally replays a key's records against a simulated target table
(`existed` / `alive` / `image`) over thousands of randomized batches.
"""

from __future__ import annotations

import random

import polars as pl
import pytest

from tabsdatak.conn.common.sql.cdc import cdc_batch_resets, coalesce_cdc_batch
from tabsdatak.conn.common.sql.cdc.constants import CdcOperation
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode, SqlCdcException

INS, UPD, DEL = "i", "u", "d"
LOAD, TRUNC = "l", "t"


# --- Helpers ---------------------------------------------------------


def _frame(records: list[tuple], columns: tuple[str, ...] = ("pk", "op", "v")):
    """Build a batch frame from ``(pk, op, v)`` tuples."""
    return pl.DataFrame(
        {name: [rec[i] for rec in records] for i, name in enumerate(columns)},
        schema={"pk": pl.String, "op": pl.String, "v": pl.Int64},
    )


def _coalesce(records: list[tuple], **kwargs) -> pl.DataFrame:
    kwargs.setdefault("key_columns", ["pk"])
    kwargs.setdefault("op_column", "op")
    return coalesce_cdc_batch(_frame(records).lazy(), **kwargs).collect()


def _rows(records: list[tuple], **kwargs) -> list[tuple]:
    """Coalesce and return the result sorted by key, for stable comparison."""
    return sorted(_coalesce(records, **kwargs).rows())


def _one(records: list[tuple], **kwargs) -> tuple:
    """Coalesce a single-key batch and return its one surviving row."""
    rows = _coalesce(records, **kwargs).rows()
    assert len(rows) == 1, f"expected exactly one row, got {rows}"
    return rows[0]


def _reference(records: list[tuple], *, after_reset: bool = False) -> tuple | None:
    """Replay one key's records against a simulated target table.

    Independent of the expression implementation: it walks the sequence
    keeping the target's state (`alive`) and the row's last usable payload
    (`image`), rather than reading the sequence's two endpoints.

    `after_reset` replays against a target just emptied, which held nothing
    whatever the records open with.

    Returns the ``(operation, record)`` the target should end up with, or
    `None` when the key must be dropped.
    """
    # Unreadable records never reach the target, so they never reach the
    # simulation either.
    records = [
        rec
        for rec in records
        if rec[1] is not None and rec[1].lower() in (INS, UPD, DEL)
    ]
    if not records:
        return None
    records = [(rec[0], rec[1].lower(), *rec[2:]) for rec in records]
    # The target held this key unless the batch opens by inserting it.
    existed = records[0][1] != INS and not after_reset
    alive = existed
    image = records[0]
    for rec in records:
        if rec[1] == DEL:
            # A delete record carries no usable payload, so the image is
            # left at the last one that did.
            alive = False
        else:
            alive = True
            image = rec
    if not alive:
        # Born and gone inside the batch: no net effect on the target.
        return None if not existed else (DEL, image)
    return (INS if not existed else UPD), image


# --- The four specified pairwise rules -------------------------------


class TestSpecifiedRules:
    def test_insert_then_update_is_an_insert_of_the_later_row(self):
        assert _one([("A", INS, 1), ("A", UPD, 2)]) == ("A", INS, 2)

    def test_update_then_delete_is_a_delete_of_the_earlier_row(self):
        assert _one([("A", UPD, 1), ("A", DEL, 2)]) == ("A", DEL, 1)

    def test_delete_then_insert_is_an_update_of_the_later_row(self):
        assert _one([("A", DEL, 1), ("A", INS, 2)]) == ("A", UPD, 2)

    def test_insert_then_delete_drops_the_key(self):
        # The fourth rule, `i(A) -> d(B) ==> d(A)`, resolved the other way:
        # the row never reached the target, so nothing is emitted.
        assert _rows([("A", INS, 1), ("A", DEL, 2)]) == []


class TestNetStateBeatsFold:
    def test_insert_update_delete_insert_is_an_insert(self):
        """The counterexample a left-to-right fold of the pairwise rules
        gets wrong: folding yields an update for a key the target has
        never seen."""
        records = [("A", INS, 1), ("A", UPD, 2), ("A", DEL, 3), ("A", INS, 4)]
        assert _one(records) == ("A", INS, 4)

    def test_long_alternating_sequence_ending_alive(self):
        records = [("A", DEL, 1), ("A", INS, 2), ("A", DEL, 3), ("A", UPD, 4)]
        # First marker is not an insert, so the row pre-existed the batch.
        assert _one(records) == ("A", UPD, 4)


# --- Net-state edge cases --------------------------------------------


class TestNetState:
    def test_single_insert(self):
        assert _one([("A", INS, 1)]) == ("A", INS, 1)

    def test_single_update(self):
        assert _one([("A", UPD, 9)]) == ("A", UPD, 9)

    def test_single_delete(self):
        assert _one([("A", DEL, 7)]) == ("A", DEL, 7)

    def test_all_delete_group_keeps_the_first_record(self):
        """No non-delete record exists, so the payload window has nothing
        to select and falls back to the earliest record."""
        assert _one([("A", DEL, 1), ("A", DEL, 2), ("A", DEL, 3)]) == ("A", DEL, 1)

    def test_repeated_updates_keep_the_latest(self):
        assert _one([("A", UPD, 1), ("A", UPD, 2), ("A", UPD, 3)]) == ("A", UPD, 3)

    def test_delete_then_updates_is_an_update(self):
        assert _one([("A", DEL, 1), ("A", UPD, 2), ("A", UPD, 3)]) == ("A", UPD, 3)

    def test_a_key_with_only_an_unrecognized_marker_is_dropped(self):
        assert _rows([("A", "r", 5)]) == []

    def test_a_key_with_only_a_null_marker_is_dropped(self):
        assert _rows([("A", None, 5)]) == []

    def test_an_unrecognized_record_does_not_contribute_its_payload(self):
        """Dropped whole, not just stripped of its marker -- otherwise the key
        would keep a payload nothing asked for."""
        assert _one([("A", INS, 1), ("A", None, 2)]) == ("A", INS, 1)

    def test_an_unrecognized_record_between_two_readable_ones_is_ignored(self):
        assert _one([("A", INS, 1), ("A", "zz", 2), ("A", UPD, 3)]) == ("A", INS, 3)

    def test_an_unrecognized_record_cannot_suppress_a_delete(self):
        assert _one([("A", None, 1), ("A", DEL, 2)]) == ("A", DEL, 2)

    def test_keys_resolve_independently(self):
        records = [
            ("A", INS, 1),
            ("B", UPD, 2),
            ("A", UPD, 3),
            ("C", INS, 4),
            ("A", DEL, 5),
            ("C", UPD, 6),
            ("A", INS, 7),
        ]
        assert _rows(records) == [("A", INS, 7), ("B", UPD, 2), ("C", INS, 6)]

    def test_key_records_need_not_be_contiguous(self):
        """Every window reads the ordinal explicitly, so adjacency is
        never load-bearing."""
        interleaved = [("A", INS, 1), ("B", INS, 10), ("A", UPD, 2), ("B", DEL, 11)]
        assert _rows(interleaved) == [("A", INS, 2)]

    def test_null_key_forms_its_own_group(self):
        records = [(None, INS, 1), (None, UPD, 2), ("A", UPD, 3)]
        # Compared as a set: a null key cannot be sorted against a string.
        assert set(_coalesce(records).rows()) == {(None, INS, 2), ("A", UPD, 3)}


class TestEphemeral:
    def test_insert_delete_is_dropped(self):
        assert _rows([("A", INS, 1), ("A", DEL, 2)]) == []

    def test_insert_update_delete_is_dropped(self):
        assert _rows([("A", INS, 1), ("A", UPD, 2), ("A", DEL, 3)]) == []

    def test_insert_delete_insert_delete_is_dropped(self):
        records = [("A", INS, 1), ("A", DEL, 2), ("A", INS, 3), ("A", DEL, 4)]
        assert _rows(records) == []

    def test_pre_existing_delete_survives(self):
        """Only a key born *inside* the batch is dropped; a tombstone for
        a row the target already held must reach the merge."""
        assert _one([("A", UPD, 1), ("A", DEL, 2)]) == ("A", DEL, 1)

    def test_dropping_one_key_leaves_the_others(self):
        records = [("A", INS, 1), ("B", UPD, 2), ("A", DEL, 3)]
        assert _rows(records) == [("B", UPD, 2)]

    def test_every_key_ephemeral_yields_an_empty_frame(self):
        got = _coalesce([("A", INS, 1), ("A", DEL, 2), ("B", INS, 3), ("B", DEL, 4)])
        assert got.is_empty()
        assert got.columns == ["pk", "op", "v"]


# --- Payload selection -----------------------------------------------


class TestPayload:
    def test_live_key_keeps_the_latest_record(self):
        records = [("A", INS, 1), ("A", UPD, 2), ("A", UPD, 3)]
        assert _one(records) == ("A", INS, 3)

    def test_deleted_key_keeps_the_last_non_delete_record(self):
        records = [("A", UPD, 1), ("A", UPD, 2), ("A", DEL, 3)]
        assert _one(records) == ("A", DEL, 2)

    def test_deleted_key_skips_trailing_deletes(self):
        records = [("A", UPD, 1), ("A", DEL, 2), ("A", DEL, 3)]
        assert _one(records) == ("A", DEL, 1)

    def test_delete_payload_is_used_when_the_record_carries_one(self):
        """Where a delete record holds a full before-image, taking the
        last non-delete record picks the same values, so the exception is
        harmless."""
        records = [("A", UPD, 42), ("A", DEL, 42)]
        assert _one(records) == ("A", DEL, 42)

    def test_all_non_key_columns_are_carried_through(self):
        lf = pl.DataFrame({
            "pk": ["A", "A"],
            "op": [INS, UPD],
            "name": ["old", "new"],
            "amount": [1.5, 2.5],
        }).lazy()
        got = coalesce_cdc_batch(lf, ["pk"], "op").collect()
        assert got.rows() == [("A", INS, "new", 2.5)]


# --- Ordering --------------------------------------------------------


class TestOrdering:
    def test_positional_order_is_used_by_default(self):
        assert _one([("A", INS, 1), ("A", UPD, 2)]) == ("A", INS, 2)

    def test_explicit_order_column_overrides_physical_order(self):
        """The batch arrives with the update physically first; the
        sequence column says it came second."""
        lf = pl.DataFrame({
            "pk": ["A", "A"],
            "op": [UPD, INS],
            "v": [2, 1],
            "lsn": [20, 10],
        }).lazy()
        got = coalesce_cdc_batch(lf, ["pk"], "op", order_column="lsn").collect()
        assert got.rows() == [("A", INS, 2, 20)]

    def test_result_is_invariant_under_physical_reordering(self):
        """With a real sequence column the answer cannot depend on how the
        rows happen to be laid out."""
        records = [
            ("A", INS, 1, 1),
            ("A", UPD, 2, 2),
            ("A", DEL, 3, 3),
            ("A", INS, 4, 4),
            ("B", UPD, 5, 1),
            ("B", DEL, 6, 2),
        ]
        expected = None
        rng = random.Random(20260812)
        for _ in range(20):
            shuffled = records[:]
            rng.shuffle(shuffled)
            lf = pl.DataFrame({
                "pk": [r[0] for r in shuffled],
                "op": [r[1] for r in shuffled],
                "v": [r[2] for r in shuffled],
                "lsn": [r[3] for r in shuffled],
            }).lazy()
            got = sorted(
                coalesce_cdc_batch(lf, ["pk"], "op", order_column="lsn").collect().rows()
            )
            if expected is None:
                expected = got
            assert got == expected
        assert expected == [("A", INS, 4, 4), ("B", DEL, 5, 1)]

    def test_insert_delete_and_delete_insert_are_distinguished(self):
        """Both hold the marker multiset ``{i, d}``; any order keyed on the
        marker would map them to one sequence."""
        assert _rows([("A", INS, 1), ("A", DEL, 2)]) == []
        assert _rows([("A", DEL, 1), ("A", INS, 2)]) == [("A", UPD, 2)]

    def test_string_order_column(self):
        lf = pl.DataFrame({
            "pk": ["A", "A"],
            "op": [UPD, INS],
            "lsn": ["0002", "0001"],
        }).lazy()
        got = coalesce_cdc_batch(lf, ["pk"], "op", order_column="lsn").collect()
        assert got.rows() == [("A", INS, "0002")]

    def test_tied_order_values_emit_several_rows(self):
        """Documented consequence of selecting rows by value: the sequence
        column has to be unique within a key."""
        lf = pl.DataFrame({"pk": ["A", "A"], "op": [UPD, UPD], "lsn": [1, 1]}).lazy()
        got = coalesce_cdc_batch(lf, ["pk"], "op", order_column="lsn").collect()
        assert got.height == 2


# --- Resets ----------------------------------------------------------


def _resets(records: list[tuple]) -> bool:
    return cdc_batch_resets(_frame(records).lazy(), "op").collect().item()


class TestResets:
    def test_a_load_voids_what_came_before_it(self):
        records = [("A", INS, 1), ("B", UPD, 2), (None, LOAD, None), ("A", INS, 3)]
        assert _rows(records) == [("A", INS, 3)]

    def test_a_truncate_voids_what_came_before_it(self):
        records = [("A", UPD, 1), (None, TRUNC, None), ("B", INS, 2)]
        assert _rows(records) == [("B", INS, 2)]

    def test_a_reset_at_the_end_leaves_nothing(self):
        got = _coalesce([("A", INS, 1), ("B", UPD, 2), (None, TRUNC, None)])
        assert got.is_empty()
        assert got.columns == ["pk", "op", "v"]

    def test_a_lone_reset_leaves_nothing(self):
        assert _rows([(None, LOAD, None)]) == []

    def test_only_the_last_reset_counts(self):
        records = [
            ("A", INS, 1),
            (None, LOAD, None),
            ("B", INS, 2),
            (None, TRUNC, None),
            ("C", INS, 3),
        ]
        assert _rows(records) == [("C", INS, 3)]

    def test_a_load_followed_by_its_snapshot_inserts_everything(self):
        records = [(None, LOAD, None), ("A", INS, 1), ("B", INS, 2)]
        assert _rows(records) == [("A", INS, 1), ("B", INS, 2)]

    def test_an_update_after_a_reset_is_an_insert(self):
        """The table was emptied, so nothing is left for the update to match:
        the row it carries has to be inserted."""
        records = [(None, LOAD, None), ("A", INS, 1), ("A", UPD, 2), ("B", UPD, 3)]
        assert _rows(records) == [("A", INS, 2), ("B", INS, 3)]

    def test_a_delete_after_a_reset_drops_the_key(self):
        """The row is gone either way: emptied by the reset, or born and
        deleted after it."""
        records = [("A", INS, 1), (None, TRUNC, None), ("A", DEL, 2), ("B", INS, 3)]
        assert _rows(records) == [("B", INS, 3)]
        records = [(None, LOAD, None), ("A", INS, 1), ("A", DEL, 2)]
        assert _rows(records) == []

    def test_a_delete_then_insert_after_a_reset_is_an_insert(self):
        records = [(None, TRUNC, None), ("A", DEL, 1), ("A", INS, 2)]
        assert _rows(records) == [("A", INS, 2)]

    def test_upper_case_resets_are_read_the_same(self):
        records = [("A", INS, 1), (None, "L", None), ("B", INS, 2)]
        assert _rows(records) == [("B", INS, 2)]
        records = [("A", INS, 1), (None, "T", None)]
        assert _rows(records) == []

    def test_unreadable_records_do_not_shift_the_cut(self):
        records = [("A", INS, 1), ("B", "r", 2), (None, LOAD, None), ("C", None, 3)]
        assert _rows(records + [("D", INS, 4)]) == [("D", INS, 4)]

    def test_order_column_places_the_reset(self):
        """Physically last, but by the sequence column the reset comes
        first: everything after it survives."""
        lf = pl.DataFrame({
            "pk": ["A", "B", None],
            "op": [INS, UPD, LOAD],
            "v": [1, 2, None],
            "lsn": [2, 3, 1],
        }).lazy()
        got = coalesce_cdc_batch(lf, ["pk"], "op", order_column="lsn").collect()
        assert sorted(got.rows()) == [("A", INS, 1, 2), ("B", INS, 2, 3)]

    def test_no_reset_leaves_the_reduction_alone(self):
        assert _one([("A", UPD, 1)]) == ("A", UPD, 1)

    @pytest.mark.parametrize(
        "records, expected",
        [
            ([("A", INS, 1)], False),
            ([("A", INS, 1), (None, LOAD, None)], True),
            ([(None, TRUNC, None), ("A", INS, 1)], True),
            ([(None, "L", None)], True),
            ([("A", "r", 1), ("A", None, 2)], False),
        ],
    )
    def test_cdc_batch_resets(self, records, expected):
        assert _resets(records) is expected

    def test_cdc_batch_resets_on_an_empty_batch(self):
        lf = pl.DataFrame(
            schema={"pk": pl.String, "op": pl.String, "v": pl.Int64}
        ).lazy()
        assert cdc_batch_resets(lf, "op").collect().item() is False

    def test_cdc_batch_resets_stays_lazy(self):
        lf = pl.scan_csv("/nonexistent/never/created.csv")
        assert isinstance(cdc_batch_resets(lf, "op"), pl.LazyFrame)


# --- Schema ----------------------------------------------------------


class TestSchema:
    def test_schema_round_trips(self):
        lf = pl.DataFrame({
            "db": ["d1", "d1"],
            "pk": [1, 1],
            "op": [INS, UPD],
            "name": ["a", "b"],
            "amount": [1.0, 2.0],
            "lsn": [1, 2],
        }).lazy()
        result = coalesce_cdc_batch(
            lf, ["db", "pk"], "op", order_column="lsn"
        ).collect()
        assert result.schema == lf.collect().schema

    def test_marker_column_keeps_its_position(self):
        lf = pl.DataFrame({"op": [INS, UPD], "pk": ["A", "A"], "v": [1, 2]}).lazy()
        got = coalesce_cdc_batch(lf, ["pk"], "op").collect()
        assert got.columns == ["op", "pk", "v"]
        assert got.rows() == [(INS, "A", 2)]

    def test_no_helper_columns_leak(self):
        got = _coalesce([("A", INS, 1)])
        assert not any(name.startswith("@td.cdc.coalesce.") for name in got.columns)

    def test_composite_key(self):
        lf = pl.DataFrame({
            "db": ["d1", "d1", "d2", "d2"],
            "pk": [1, 1, 1, 1],
            "op": [INS, UPD, UPD, DEL],
            "v": [1, 2, 3, 4],
        }).lazy()
        got = sorted(coalesce_cdc_batch(lf, ["db", "pk"], "op").collect().rows())
        # Same `pk` in two databases: the composite key keeps them apart.
        assert got == [("d1", 1, INS, 2), ("d2", 1, DEL, 3)]

    def test_enum_marker_preserved_with_op_dtype(self):
        dtype = pl.Enum([INS, UPD, DEL])
        lf = (
            pl
            .DataFrame({"pk": ["A", "A"], "op": [DEL, INS], "v": [1, 2]})
            .with_columns(pl.col("op").cast(dtype))
            .lazy()
        )
        got = coalesce_cdc_batch(lf, ["pk"], "op", op_dtype=dtype).collect()
        assert got.schema["op"] == dtype
        assert got.rows() == [("A", UPD, 2)]

    def test_enum_marker_widens_to_string_without_op_dtype(self):
        """Why `op_dtype` exists: a bare literal cannot know the column's
        dtype, and the dtype cannot be read without resolving the schema."""
        lf = (
            pl
            .DataFrame({"pk": ["A"], "op": [UPD], "v": [1]})
            .with_columns(pl.col("op").cast(pl.Enum([INS, UPD, DEL])))
            .lazy()
        )
        got = coalesce_cdc_batch(lf, ["pk"], "op").collect()
        assert got.schema["op"] == pl.String

    def test_categorical_marker_preserved_with_op_dtype(self):
        lf = (
            pl
            .DataFrame({"pk": ["A", "A"], "op": [INS, UPD], "v": [1, 2]})
            .with_columns(pl.col("op").cast(pl.Categorical))
            .lazy()
        )
        got = coalesce_cdc_batch(lf, ["pk"], "op", op_dtype=pl.Categorical).collect()
        assert got.schema["op"] == pl.Categorical
        assert got.rows() == [("A", INS, 2)]

    def test_empty_batch_yields_an_empty_frame_with_the_same_schema(self):
        lf = pl.DataFrame(
            schema={"pk": pl.String, "op": pl.String, "v": pl.Int64}
        ).lazy()
        got = coalesce_cdc_batch(lf, ["pk"], "op").collect()
        assert got.is_empty()
        assert got.schema == lf.collect().schema


# --- Laziness --------------------------------------------------------


class TestLaziness:
    def test_returns_a_lazyframe_over_a_path_that_does_not_exist(self):
        lf = pl.scan_csv("/nonexistent/never/created.csv")
        out = coalesce_cdc_batch(lf, ["pk"], "op")
        assert isinstance(out, pl.LazyFrame)

    def test_neither_collect_nor_collect_schema_is_called(self, monkeypatch):
        def forbidden(self, *args, **kwargs):
            raise AssertionError("the transformation must stay lazy")

        monkeypatch.setattr(pl.LazyFrame, "collect", forbidden)
        monkeypatch.setattr(pl.LazyFrame, "collect_schema", forbidden)
        lf = pl.scan_csv("/nonexistent/never/created.csv")
        coalesce_cdc_batch(lf, ["a", "b"], "op", order_column="lsn")

    def test_composes_into_a_larger_plan(self):
        lf = _frame([("A", INS, 1), ("A", UPD, 2), ("B", UPD, 3)]).lazy()
        out = coalesce_cdc_batch(lf, ["pk"], "op").filter(pl.col("v") > 2)
        assert out.collect().rows() == [("B", UPD, 3)]


# --- Markers ---------------------------------------------------------


class TestMarkers:
    def test_markers_are_cdc_operations(self):
        """The three markers are fixed: a feed spelling them any other way has
        to map them onto `CdcOperation` before coalescing."""
        assert (CdcOperation.INSERT.value, CdcOperation.UPDATE.value) == (INS, UPD)
        assert CdcOperation.DELETE.value == DEL
        assert _one([("A", INS, 1), ("A", UPD, 2)]) == ("A", INS, 2)

    def test_upper_case_markers_are_read_the_same(self):
        assert _one([("A", "I", 1), ("A", "U", 2)]) == ("A", INS, 2)

    def test_upper_case_delete_deletes(self):
        assert _one([("A", "U", 1), ("A", "D", 2)]) == ("A", DEL, 1)

    def test_mixed_case_within_one_key(self):
        """Case folding happens before the reduction, so a key spelling its
        markers inconsistently still reads as insert-then-delete."""
        assert _rows([("A", "I", 1), ("A", UPD, 2), ("A", "D", 3)]) == []

    def test_the_resolved_marker_is_written_in_lower_case(self):
        """Whatever case came in, the output is normalized."""
        assert _one([("A", "D", 1)]) == ("A", DEL, 1)
        assert _one([("A", "I", 1)]) == ("A", INS, 1)
        assert _one([("A", "U", 1)]) == ("A", UPD, 1)

    def test_a_foreign_insert_marker_is_dropped(self):
        """Debezium's ``c`` for create is not `CdcOperation.INSERT`, so that
        record goes; the key resolves from what is left."""
        assert _one([("A", "c", 1), ("A", UPD, 2)]) == ("A", UPD, 2)

    def test_a_foreign_delete_marker_is_dropped_with_its_payload(self):
        assert _one([("A", UPD, 1), ("A", "X", 2)]) == ("A", UPD, 1)

    @pytest.mark.parametrize("bad", ["", " ", " d ", "dd", "id", "r", "c", "0", None])
    def test_only_the_three_markers_survive(self, bad):
        """Matching is exact beyond case -- padding or a near-miss is not a
        marker."""
        assert _rows([("A", bad, 1)]) == []


# --- Validation ------------------------------------------------------


class TestValidation:
    def _lf(self):
        return _frame([("A", INS, 1)]).lazy()

    def test_no_key_columns_raises_cdc_16(self):
        with pytest.raises(SqlCdcException) as ei:
            coalesce_cdc_batch(self._lf(), [], "op")
        assert ei.value.api_error is SqlCdcErrorCode.CDC_16

    def test_op_column_as_key_raises_cdc_17(self):
        with pytest.raises(SqlCdcException) as ei:
            coalesce_cdc_batch(self._lf(), ["pk", "op"], "op")
        assert ei.value.api_error is SqlCdcErrorCode.CDC_17
        assert "'op'" in str(ei.value)

    def test_op_column_as_the_only_key_raises_cdc_17(self):
        with pytest.raises(SqlCdcException) as ei:
            coalesce_cdc_batch(self._lf(), ["op"], "op")
        assert ei.value.api_error is SqlCdcErrorCode.CDC_17

    def test_order_column_as_key_raises_cdc_18(self):
        with pytest.raises(SqlCdcException) as ei:
            coalesce_cdc_batch(self._lf(), ["pk"], "op", order_column="pk")
        assert ei.value.api_error is SqlCdcErrorCode.CDC_18
        assert "'pk'" in str(ei.value)

    def test_order_column_as_op_column_raises_cdc_19(self):
        with pytest.raises(SqlCdcException) as ei:
            coalesce_cdc_batch(self._lf(), ["pk"], "op", order_column="op")
        assert ei.value.api_error is SqlCdcErrorCode.CDC_19

    def test_order_column_is_carried_through_like_any_other_column(self):
        lf = pl.DataFrame({"pk": ["A", "A"], "op": [INS, UPD], "lsn": [2, 1]}).lazy()
        got = coalesce_cdc_batch(lf, ["pk"], "op", order_column="lsn").collect()
        # By `lsn` the update comes first, so the key reads as pre-existing
        # and resolves to an update -- not to the insert that the physical
        # order would suggest. `lsn` itself survives into the result.
        assert got.rows() == [("A", UPD, 2)]


# --- Cross-check against a simulated target table --------------------


def _random_group(rng: random.Random, alphabet: list) -> list:
    return [rng.choice(alphabet) for _ in range(rng.randint(1, 14))]


def _interleave(rng: random.Random, groups: list[list[tuple]]) -> list[tuple]:
    """Merge groups into one batch, preserving each group's internal order.

    Shuffling the flattened records would not do: with positional
    ordering, a key's own record order *is* the semantics, and only the
    order *between* keys is free.
    """
    slots = [index for index, group in enumerate(groups) for _ in group]
    rng.shuffle(slots)
    cursors = [0] * len(groups)
    records = []
    for index in slots:
        records.append(groups[index][cursors[index]])
        cursors[index] += 1
    return records


class TestAgainstReference:
    @pytest.mark.parametrize(
        "alphabet",
        [
            [INS, UPD, DEL],
            # Case folding: the same three, spelled either way.
            [INS, UPD, DEL, "I", "U", "D"],
            # Records the reduction has to discard, mixed in with real ones.
            [INS, UPD, DEL, "I", "D", None, "r", ""],
        ],
    )
    def test_randomized_batches_match_the_simulation(self, alphabet):
        rng = random.Random(4242)
        groups: list[list[tuple]] = []
        expected: list[tuple] = []
        counter = 0
        # Every key is namespaced by batch, so the 2000 batches are
        # mutually independent yet resolve in a single pass -- and their
        # records interleave, which contiguity-dependent code would fail.
        for batch in range(2000):
            for key in range(4):
                pk = f"b{batch}-k{key}"
                group = []
                for op in _random_group(rng, alphabet):
                    group.append((pk, op, counter))
                    counter += 1
                groups.append(group)
                outcome = _reference(group)
                if outcome is not None:
                    op, image = outcome
                    expected.append((pk, op, image[2]))
        assert _rows(_interleave(rng, groups)) == sorted(expected)

    def test_randomized_batches_one_frame_per_batch(self):
        """The doc's original shape: one small frame per batch, keys
        reused across batches."""
        rng = random.Random(1234)
        for _ in range(400):
            records: list[tuple] = []
            expected: list[tuple] = []
            for key in "ABCD":
                ops = _random_group(rng, [INS, UPD, DEL])
                group = [(key, op, len(records) + i) for i, op in enumerate(ops)]
                records.extend(group)
                outcome = _reference(group)
                if outcome is not None:
                    op, image = outcome
                    expected.append((key, op, image[2]))
            rng.shuffle(records)
            # Re-derive the per-key order from the payload, which doubles
            # as the sequence number.
            lf = pl.DataFrame({
                "pk": [r[0] for r in records],
                "op": [r[1] for r in records],
                "v": [r[2] for r in records],
                "lsn": [r[2] for r in records],
            }).lazy()
            got = sorted(
                coalesce_cdc_batch(lf, ["pk"], "op", order_column="lsn")
                .collect()
                .drop("lsn")
                .rows()
            )
            assert got == sorted(expected)

    def test_randomized_batches_with_resets_match_the_simulation(self):
        """A reset cuts the whole batch, so these batches are resolved one at
        a time: the keys before the cut never reach the target, and the keys
        after it replay against an emptied one."""
        rng = random.Random(9090)
        for _ in range(300):
            groups = {key: [] for key in "ABCD"}
            records: list[tuple] = []
            last_reset = -1
            for position in range(rng.randint(1, 30)):
                if rng.random() < 0.08:
                    records.append((None, rng.choice([LOAD, TRUNC]), position))
                    last_reset = position
                    continue
                key = rng.choice("ABCD")
                record = (key, rng.choice([INS, UPD, DEL]), position)
                records.append(record)
                groups[key].append(record)
            expected = []
            for key, group in groups.items():
                group = [rec for rec in group if rec[2] > last_reset]
                outcome = _reference(group, after_reset=last_reset >= 0)
                if outcome is not None:
                    op, image = outcome
                    expected.append((key, op, image[2]))
            assert _rows(records) == sorted(expected)
            assert _resets(records) is (last_reset >= 0)

    def test_reference_agrees_on_the_specified_rules(self):
        """Guards the guard: the reference must reproduce the four rules
        the requirement states."""
        assert _reference([("A", INS, 1), ("A", UPD, 2)]) == (INS, ("A", UPD, 2))
        assert _reference([("A", UPD, 1), ("A", DEL, 2)]) == (DEL, ("A", UPD, 1))
        assert _reference([("A", DEL, 1), ("A", INS, 2)]) == (UPD, ("A", INS, 2))
        assert _reference([("A", INS, 1), ("A", DEL, 2)]) is None
