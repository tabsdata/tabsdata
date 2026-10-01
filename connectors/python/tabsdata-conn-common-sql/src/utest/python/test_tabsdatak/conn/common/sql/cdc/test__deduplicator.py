#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import itertools
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
import sqlalchemy.types as sa_types

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._deduplicator import (
    deduplicate_initial_load,
    deduplicate_initial_load_files,
)
from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion
from tabsdatak.conn.common.sql.cdc._model import TableObject, TrackedTable
from tabsdatak.conn.common.sql.cdc._schema import SchemaRegistry
from tabsdatak.conn.common.sql.cdc.constants import (
    CDC_DATA_COL_NEW_PREFIX,
    CDC_DATA_COL_OLD_PREFIX,
    CDC_DATA_ROW_NEW,
    CDC_DATA_ROW_OLD,
    CDC_META_FLAT,
    CDC_META_FMT,
    CDC_META_OP,
    CDC_META_ORD,
    CDC_META_SCH,
    CDC_META_SEG,
    CDC_META_SQ,
    CDC_META_TS,
    CDC_META_TX,
    CdcOperation,
)
from tabsdatak.conn.common.sql.cdc.error import SqlCdcException
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat

TABLE = "s.t"
SCH = "h"
COLUMNS = {"id": pl.Int64, "name": pl.String}

FLAT = CdcFormat(columns_format="columns", flatten_columns=True)
FORMATS = [
    FLAT,
    CdcFormat(columns_format="columns", flatten_columns=False),
    CdcFormat(columns_format="struct", flatten_columns=False),
]
FORMAT_IDS = ["flat", "columns", "struct"]


def schema(primary_key=("id",)) -> SchemaVersion:
    return SchemaVersion(
        version=0,
        raw=(),
        hash=SCH,
        columns=tuple(TrackedColumn(name=name, satype=None) for name in COLUMNS),
        pa_schema=None,
        large_columns=(),
        primary_key=primary_key,
    )


def frame(rows: list[tuple], cdc_format: CdcFormat) -> pl.DataFrame:
    """Rows of ``(op, new, old)``, each image a ``(id, name)`` tuple or
    ``None``, in the layout the stream writes in `cdc_format`, plus ``sch``
    (``SCH`` unless a row carries a fourth element) and ``seg`` / ``ord``
    (the row's index and 0 unless it carries a fifth, ``(seg, ord)``)."""
    ops = [row[0] for row in rows]
    schs = [row[3] if len(row) > 3 else SCH for row in rows]
    positions = [row[4] if len(row) > 4 else (i, 0) for i, row in enumerate(rows)]

    def image(index: int) -> dict[str, list]:
        return {
            name: [row[index][i] if row[index] else None for row in rows]
            for i, name in enumerate(COLUMNS)
        }

    new, old = image(1), image(2)
    data: dict[str, pl.Series] = {
        CDC_META_OP: pl.Series(ops, dtype=pl.String),
        CDC_META_SCH: pl.Series(schs, dtype=pl.String),
        CDC_META_SEG: pl.Series([seg for seg, _ in positions], dtype=pl.Int64),
        CDC_META_ORD: pl.Series([ord_ for _, ord_ in positions], dtype=pl.Int64),
    }
    struct = cdc_format.columns_format == "struct"
    if cdc_format.flatten_columns:
        data |= {n: pl.Series(new[n], dtype=t) for n, t in COLUMNS.items()}
    elif struct:
        data[CDC_DATA_ROW_NEW] = struct_series(new)
    else:
        data |= {
            f"{CDC_DATA_COL_NEW_PREFIX}.{n}": pl.Series(new[n], dtype=t)
            for n, t in COLUMNS.items()
        }
    if struct:
        data[CDC_DATA_ROW_OLD] = struct_series(old)
    else:
        data |= {
            f"{CDC_DATA_COL_OLD_PREFIX}.{n}": pl.Series(old[n], dtype=t)
            for n, t in COLUMNS.items()
        }
    return pl.DataFrame(data)


def struct_series(values: dict[str, list]) -> pl.Series:
    return pl.DataFrame({
        n: pl.Series(values[n], dtype=t) for n, t in COLUMNS.items()
    }).to_struct()


def load(*rows: tuple, cdc_format: CdcFormat = FLAT) -> pl.DataFrame:
    return frame([("l", None, None)] + [("i", row, None) for row in rows], cdc_format)


def dedup(
    loaded: pl.DataFrame,
    changes: pl.DataFrame,
    cdc_format: CdcFormat = FLAT,
    primary_key=("id",),
) -> pl.DataFrame:
    return deduplicate_initial_load(
        loaded.lazy(),
        changes.lazy(),
        table=TABLE,
        schema=schema(primary_key),
        cdc_format=cdc_format,
    ).collect()


def kept(loaded: pl.DataFrame, rows: list[tuple], cdc_format=FLAT) -> list[tuple]:
    """Which of `rows` are kept, as ``(op, new)``, checking that the result
    is exactly those rows, unchanged and in their original order."""
    changes = frame(rows, cdc_format)
    result = dedup(loaded, changes, cdc_format)
    assert result.columns == changes.columns
    remaining = result.rows()
    out = []
    for row, values in zip(rows, changes.rows()):
        if remaining and remaining[0] == values:
            out.append((row[0], row[1]))
            remaining.pop(0)
    assert not remaining, "the result holds rows the changes do not, or reordered"
    return out


# ---- the rules ---------------------------------------------------------------


@pytest.mark.parametrize("cdc_format", FORMATS, ids=FORMAT_IDS)
def test_an_insert_on_an_existing_row_is_dropped(cdc_format):
    loaded = load((1, "a"), cdc_format=cdc_format)
    assert kept(loaded, [("i", (1, "a"), None)], cdc_format) == []


@pytest.mark.parametrize("cdc_format", FORMATS, ids=FORMAT_IDS)
def test_an_insert_on_a_missing_row_is_kept(cdc_format):
    loaded = load((1, "a"), cdc_format=cdc_format)
    assert kept(loaded, [("i", (2, "b"), None)], cdc_format) == [("i", (2, "b"))]


@pytest.mark.parametrize("cdc_format", FORMATS, ids=FORMAT_IDS)
def test_a_delete_on_a_missing_row_is_dropped(cdc_format):
    loaded = load((1, "a"), cdc_format=cdc_format)
    assert kept(loaded, [("d", None, (2, "b"))], cdc_format) == []


@pytest.mark.parametrize("cdc_format", FORMATS, ids=FORMAT_IDS)
def test_an_update_with_the_same_contents_is_dropped(cdc_format):
    loaded = load((1, "a"), cdc_format=cdc_format)
    assert kept(loaded, [("u", (1, "a"), None)], cdc_format) == []


@pytest.mark.parametrize("cdc_format", FORMATS, ids=FORMAT_IDS)
def test_an_update_with_other_contents_is_kept(cdc_format):
    loaded = load((1, "a"), cdc_format=cdc_format)
    assert kept(loaded, [("u", (1, "b"), None)], cdc_format) == [("u", (1, "b"))]


def test_an_update_on_a_missing_row_is_dropped():
    assert kept(load((1, "a")), [("u", (2, "b"), None)]) == []


def test_every_change_that_applies_in_turn_is_kept():
    rows = [
        ("u", (1, "b"), None),
        ("u", (1, "c"), None),
        ("u", (1, "d"), None),
        ("d", None, (1, "d")),
    ]
    assert kept(load((1, "a")), rows) == [(op, new) for op, new, _ in rows]


def test_a_dropped_change_does_not_move_the_state():
    rows = [
        ("i", (7, "ann"), None),  # the load read it: dropped
        ("u", (7, "bob"), None),  # kept
        ("u", (7, "bob"), None),  # same contents: dropped
        ("d", None, (7, "bob")),  # kept
        ("u", (7, "cat"), None),  # missing row: dropped
        ("i", (7, "dan"), None),  # kept
    ]
    assert kept(load((7, "ann")), rows) == [
        ("u", (7, "bob")),
        ("d", None),
        ("i", (7, "dan")),
    ]


def test_a_delete_carrying_its_row_in_the_new_image_is_applied():
    assert kept(load((1, "a")), [("d", (1, "a"), None)]) == [("d", (1, "a"))]


def test_keys_are_independent():
    rows = [("i", (1, "a"), None), ("i", (2, "b"), None), ("d", None, (1, "a"))]
    assert kept(load((1, "a")), rows) == [("i", (2, "b")), ("d", None)]


def test_a_composite_key_matches_on_every_column():
    loaded = load((1, "a"))
    changes = frame([("i", (1, "b"), None), ("i", (1, "a"), None)], FLAT)
    result = dedup(loaded, changes, primary_key=("id", "name"))
    assert result.rows() == changes.rows()[:1]


def test_the_kept_changes_keep_their_original_order():
    rows = [
        ("i", (3, "c"), None),
        ("i", (1, "a"), None),
        ("i", (2, "b"), None),
        ("u", (1, "z"), None),
    ]
    assert kept(load((1, "a")), rows) == [
        ("i", (3, "c")),
        ("i", (2, "b")),
        ("u", (1, "z")),
    ]


# ---- key changes -------------------------------------------------------------


def test_a_key_change_from_a_loaded_row_to_a_free_key_is_kept():
    rows = [("u", (3, "a"), (1, "a"))]
    assert kept(load((1, "a")), rows) == [("u", (3, "a"))]


def test_a_key_change_the_load_saw_neither_side_of_is_kept():
    rows = [("u", (3, "a"), (1, "a")), ("u", (3, "b"), None)]
    assert kept(load(), rows) == [("u", (3, "a")), ("u", (3, "b"))]


def test_a_key_change_the_load_saw_done_is_dropped():
    rows = [("u", (3, "a"), (1, "a"))]
    assert kept(load((3, "a")), rows) == []


def test_a_key_change_onto_a_held_key_fails():
    changes = frame([("u", (2, "a"), (1, "a"))], FLAT)
    with pytest.raises(SqlCdcException, match="CDC_23"):
        dedup(load((1, "a"), (2, "b")), changes)


def test_a_key_swap_through_a_free_key_is_applied_in_sequence():
    rows = [
        ("u", (3, "a"), (1, "a")),
        ("u", (1, "b"), (2, "b")),
        ("u", (2, "a"), (3, "a")),
    ]
    assert kept(load((1, "a"), (2, "b")), rows) == [(op, new) for op, new, _ in rows]


def test_a_key_loaded_twice_fails():
    with pytest.raises(SqlCdcException, match="CDC_23"):
        dedup(load((1, "a"), (1, "b")), frame([("i", (1, "c"), None)], FLAT))


# ---- what fails and what passes through ------------------------------------------


def test_a_truncate_fails():
    with pytest.raises(SqlCdcException, match="CDC_24"):
        dedup(load((1, "a")), frame([("t", None, None)], FLAT))


def test_changes_out_of_order_fail():
    rows = [("i", (2, "b"), None, SCH, (5, 0)), ("i", (3, "c"), None, SCH, (4, 9))]
    with pytest.raises(SqlCdcException, match="CDC_28"):
        dedup(load((1, "a")), frame(rows, FLAT))


def test_changes_of_one_transaction_out_of_order_fail():
    rows = [("i", (2, "b"), None, SCH, (5, 1)), ("i", (3, "c"), None, SCH, (5, 0))]
    with pytest.raises(SqlCdcException, match="CDC_28"):
        dedup(load((1, "a")), frame(rows, FLAT))


def test_an_unknown_operation_fails():
    with pytest.raises(SqlCdcException, match="CDC_27"):
        dedup(load((1, "a")), frame([("x", (1, "b"), None)], FLAT))


def test_a_table_without_a_primary_key_still_fails_on_a_truncate():
    with pytest.raises(SqlCdcException, match="CDC_24"):
        dedup(load((1, "a")), frame([("t", None, None)], FLAT), primary_key=())


def test_a_table_without_a_primary_key_still_fails_on_an_unknown_operation():
    with pytest.raises(SqlCdcException, match="CDC_27"):
        dedup(load((1, "a")), frame([("x", (1, "b"), None)], FLAT), primary_key=())


# What the deduplicator does with each operation, starting from a load of row
# (1, "a"): "kept" for a change it keeps, or the error code it fails with. A
# new CdcOperation fails the first test until the deduplicator handles it and
# it is added here.
OPERATION_OUTCOMES = {
    CdcOperation.INSERT: (("i", (2, "b"), None), "kept"),
    CdcOperation.UPDATE: (("u", (1, "b"), None), "kept"),
    CdcOperation.DELETE: (("d", None, (1, "a")), "kept"),
    CdcOperation.TRUNCATE: (("t", None, None), "CDC_24"),
    CdcOperation.LOAD: (("l", None, None), "CDC_27"),
}


def test_every_operation_has_a_defined_outcome():
    assert set(OPERATION_OUTCOMES) == set(CdcOperation), (
        "a CdcOperation was added or removed: make the initial-load "
        "deduplicator handle it, then list its outcome in OPERATION_OUTCOMES"
    )


@pytest.mark.parametrize(
    "operation", list(OPERATION_OUTCOMES), ids=[op.name for op in OPERATION_OUTCOMES]
)
def test_every_operation_is_handled(operation):
    change, outcome = OPERATION_OUTCOMES[operation]
    assert change[0] == operation.value
    if outcome == "kept":
        assert kept(load((1, "a")), [change]) == [(change[0], change[1])]
    else:
        with pytest.raises(SqlCdcException, match=outcome):
            dedup(load((1, "a")), frame([change], FLAT))


def test_a_sequence_of_every_row_operation_is_deduplicated():
    rows = [
        ("i", (1, "a"), None),  # the load read it: dropped
        ("u", (1, "b"), None),
        ("i", (2, "c"), None),
        ("u", (3, "c"), (2, "c")),
        ("d", None, (1, "b")),
        ("d", None, (1, "b")),  # already gone: dropped
    ]
    assert kept(load((1, "a")), rows) == [
        ("u", (1, "b")),
        ("i", (2, "c")),
        ("u", (3, "c")),
        ("d", None),
    ]


def test_changes_all_in_another_schema_than_the_load_fail():
    rows = [("i", (2, "b"), None, "other"), ("i", (3, "c"), None, "other")]
    changes = frame(rows, FLAT)
    with pytest.raises(SqlCdcException, match="CDC_25"):
        dedup(load((1, "a")), changes)


def test_changes_in_two_schemas_fail():
    changes = frame([("i", (2, "b"), None), ("i", (3, "c"), None, "other")], FLAT)
    with pytest.raises(SqlCdcException, match="CDC_25"):
        dedup(load((1, "a")), changes)


def test_the_map_format_fails():
    with pytest.raises(SqlCdcException, match="CDC_26"):
        deduplicate_initial_load(
            pl.LazyFrame(),
            pl.LazyFrame(),
            table=TABLE,
            schema=schema(),
            cdc_format=CdcFormat(columns_format="map", flatten_columns=False),
        )


def test_a_key_column_missing_from_the_schema_fails():
    with pytest.raises(SqlCdcException, match="CDC_22"):
        dedup(load((1, "a")), frame([("i", (1, "a"), None)], FLAT), primary_key=("x",))


def test_a_table_without_a_primary_key_is_not_deduplicated():
    changes = frame([("i", (1, "a"), None)], FLAT)
    assert dedup(load((1, "a")), changes, primary_key=()).rows() == changes.rows()


def test_changes_kept_as_read_for_a_table_without_a_key_are_warned_of(caplog):
    changes = frame([("i", (1, "a"), None), ("i", (2, "b"), None)], FLAT)
    with caplog.at_level("WARNING"):
        dedup(load((1, "a")), changes, primary_key=())
    assert "2 change(s)" in caplog.text


def test_a_table_without_a_key_and_no_changes_is_not_warned_of(caplog):
    with caplog.at_level("WARNING"):
        dedup(load((1, "a")), frame([], FLAT), primary_key=())
    assert caplog.text == ""


def test_no_changes_yield_no_changes():
    changes = frame([], FLAT)
    assert dedup(load((1, "a")), changes).height == 0


def test_the_load_is_not_evaluated_without_a_primary_key():
    changes = frame([("i", (1, "a"), None)], FLAT)
    unreadable = pl.LazyFrame({"x": [1]}).select(pl.col("missing"))
    result = deduplicate_initial_load(
        unreadable, changes.lazy(), table=TABLE, schema=schema(()), cdc_format=FLAT
    )
    assert result.collect().rows() == changes.rows()


# ---- files ------------------------------------------------------------------------


def registered(table: str, pk: list[str]) -> tuple[TrackedTable, SchemaVersion]:
    tracked = TrackedTable(
        data_table=TableObject(schema="s", table=table),
        cdc_table=TableObject(schema="s", table=table),
        columns=[
            TrackedColumn(name="id", satype=sa_types.BigInteger()),
            TrackedColumn(name="name", satype=sa_types.String()),
        ],
        pk=pk,
    )
    registry = SchemaRegistry(
        type_mapper=lambda t: t, large_column_classifier=lambda t: None, cdc_format=FLAT
    )
    version = registry.register_from_catalog(f"s.{table}", tracked)
    return tracked, version


def written(
    folder: Path, name: str, rows: list[tuple], version: SchemaVersion, start: int = 0
) -> Path:
    """`rows` as a file the stream writes: every meta column filled, positions
    counted from `start`, and cast to the schema version's own Arrow schema."""
    positions = pl.Series(range(start, start + len(rows)), dtype=pl.Decimal(38, 0))
    data = frame(rows, FLAT).with_columns(
        pl.lit(version.hash).alias(CDC_META_SCH),
        pl.lit("tx").alias(CDC_META_TX),
        pl.Series(CDC_META_SQ, range(len(rows)), dtype=pl.Int64),
        pl.lit(datetime(2026, 1, 1, tzinfo=timezone.utc)).alias(CDC_META_TS),
        positions.alias(CDC_META_SEG),
        positions.alias(CDC_META_ORD),
        pl.lit("columns").alias(CDC_META_FMT),
        pl.lit(True).alias(CDC_META_FLAT),
    )
    pa_schema = version.pa_schema
    path = folder / f"{name}.parquet"
    pq.write_table(data.to_arrow().select(pa_schema.names).cast(pa_schema), path)
    return path


def dedup_files(tmp_path: Path, slots: list[tuple[str, list[str], list, list]]):
    """Each slot as ``(table, pk, loaded rows, change files)``, each change
    file a list of rows, with positions continuing from one file to the
    next."""
    source = tmp_path / "source"
    source.mkdir()
    landing = tmp_path / "landing"
    landing.mkdir()
    tables, schemas, loaded, changes = {}, {}, [], []
    for table, pk, load_rows, change_rows in slots:
        tracked, version = registered(table, pk)
        tables[f"s.{table}"] = tracked
        schemas[f"s.{table}"] = version
        load_rows = [("l", None, None)] + [("i", row, None) for row in load_rows]
        loaded.append([written(source, f"{table}_load", load_rows, version)])
        files, start = [], 0
        for index, file_rows in enumerate(change_rows):
            files.append(
                written(source, f"{table}_changes_{index}", file_rows, version, start)
            )
            start += len(file_rows)
        changes.append(files)
    manifest = deduplicate_initial_load_files(
        loaded=loaded,
        changes=changes,
        tables=tables,
        schemas=schemas,
        cdc_format=FLAT,
        landing=landing,
    )
    return manifest, schemas, landing


def test_the_kept_changes_of_every_table_are_written_as_the_stream_writes_them(
    tmp_path,
):
    manifest, schemas, _ = dedup_files(
        tmp_path,
        [
            (
                "keyed",
                ["id"],
                [(1, "a")],
                [[("i", (1, "a"), None), ("i", (2, "b"), None)]],
            ),
            ("keyless", [], [(1, "a")], [[("i", (1, "a"), None)]]),
        ],
    )
    keyed, keyless = manifest
    assert len(keyed) == 1
    assert len(keyless) == 1
    keyed_rows = pq.read_table(keyed[0])
    assert keyed_rows.schema == schemas["s.keyed"].pa_schema
    assert keyed_rows.column("id").to_pylist() == [2]
    assert pq.read_table(keyless[0]).column("id").to_pylist() == [1]


def test_a_table_without_changes_gets_no_file(tmp_path):
    manifest, _, landing = dedup_files(
        tmp_path,
        [
            ("quiet", ["id"], [(1, "a")], []),
            ("replayed", ["id"], [(1, "a")], [[("i", (1, "a"), None)]]),
        ],
    )
    assert manifest == [[], []]
    assert list(landing.iterdir()) == []


def test_changes_over_several_files_keep_their_original_order(tmp_path):
    manifest, _, _ = dedup_files(
        tmp_path,
        [
            (
                "split",
                ["id"],
                [(1, "a")],
                [
                    [("i", (3, "c"), None), ("i", (1, "a"), None)],
                    [("i", (2, "b"), None)],
                    [("u", (1, "z"), None), ("i", (4, "d"), None)],
                ],
            )
        ],
    )
    (files,) = manifest
    assert [pq.read_table(file).column("id").to_pylist() for file in files] == [
        [3, 2, 1, 4]
    ]


def test_a_failing_table_leaves_no_file_behind(tmp_path):
    with pytest.raises(SqlCdcException, match="CDC_24"):
        dedup_files(
            tmp_path,
            [
                ("fine", ["id"], [(1, "a")], [[("i", (2, "b"), None)]]),
                ("truncated", ["id"], [(1, "a")], [[("t", None, None)]]),
            ],
        )
    assert list((tmp_path / "landing").iterdir()) == []


# ---- histories ----------------------------------------------------------------
#
# A table's history is simulated at the source: its rows before the window, and
# the changes committed while the load read it. The load reads each key once, at
# its own moment of that history (Db2), or every key at one moment (Postgres).
# Deduplicating the window's changes against that load, then applying the load
# and the kept changes the way a strict consumer does, must end exactly at the
# source's final rows. The only way out is failing with CDC_23, which a key
# change the engine cannot place leads to (a row read before and after its key
# changed, or only at a key it reached later): safe, the load runs again.
#
# A source change is ("i", key, value), ("u", key, value), ("d", key) or
# ("m", old_key, new_key, value): an update that moves the row to a free key.

History = list[dict[int, tuple[int, str]]]


def simulate(initial: dict[int, str], changes: list[tuple]) -> History:
    """The source's rows, ``key -> (identity, value)``, before every change
    and after the last one. The identity follows a row through key changes,
    which lets a test report a row the load read twice."""
    rows = {
        key: (identity, value) for identity, (key, value) in enumerate(initial.items())
    }
    identities = len(rows)
    states = [dict(rows)]
    for change in changes:
        match change:
            case ("i", key, value):
                assert key not in rows, f"the source cannot insert existing {key}"
                identities += 1
                rows[key] = (identities, value)
            case ("u", key, value):
                assert key in rows, f"the source cannot update missing {key}"
                rows[key] = (rows[key][0], value)
            case ("d", key):
                assert key in rows, f"the source cannot delete missing {key}"
                del rows[key]
            case ("m", old, new, value):
                assert old in rows and new not in rows, f"bad move {old} -> {new}"
                rows[new] = (rows.pop(old)[0], value)
        states.append(dict(rows))
    return states


def change_rows(
    states: History, changes: list[tuple], *, deletes: str, old_images: bool
) -> list[tuple]:
    """The change rows the stream delivers for `changes`: ``(op, new, old)``.
    `deletes` puts a deleted row in the old image (Postgres) or the new one
    (Db2); `old_images` records an update's old values, which a key change
    always carries."""
    rows = []
    for before, change in zip(states, changes):
        match change:
            case ("i", key, value):
                rows.append(("i", (key, value), None))
            case ("u", key, value):
                old = (key, before[key][1]) if old_images else None
                rows.append(("u", (key, value), old))
            case ("d", key):
                image = (key, before[key][1])
                rows.append(
                    ("d", None, image) if deletes == "old" else ("d", image, None)
                )
            case ("m", old, new, value):
                rows.append(("u", (new, value), (old, before[old][1])))
    return rows


def read_by_the_load(states: History, reads: dict[int, int]) -> dict[int, tuple]:
    """Each key as the load read it: at its moment of the history."""
    return {
        key: states[moment][key]
        for key, moment in reads.items()
        if key in states[moment]
    }


def replay_strictly(loaded: dict[int, str], kept: pl.DataFrame) -> dict[int, str]:
    """`loaded`, then every `kept` change applied as a strict consumer does: an
    insert on an existing row, an update or delete of a missing one, or a key
    change onto a held key is a duplicate or a gap."""
    rows = dict(loaded)
    old_id = f"{CDC_DATA_COL_OLD_PREFIX}.id"
    for change in kept.iter_rows(named=True):
        op, key, value = change[CDC_META_OP], change["id"], change["name"]
        old = change[old_id]
        match op:
            case "i":
                assert key not in rows, f"insert of existing row {key}"
                rows[key] = value
            case "u" if old is not None and old != key:
                rows.pop(old, None)
                assert key not in rows, f"key change onto held row {key}"
                rows[key] = value
            case "u":
                assert key in rows, f"update of missing row {key}"
                rows[key] = value
            case "d":
                target = old if old is not None else key
                assert target in rows, f"delete of missing row {target}"
                del rows[target]
    return rows


def check_history(
    initial: dict[int, str],
    changes: list[tuple],
    reads: dict[int, int],
    *,
    deletes: str = "old",
    old_images: bool = True,
) -> bool:
    """Whether deduplicating the window succeeded; when it did, it ended at
    the source's final rows."""
    states = simulate(initial, changes)
    read = read_by_the_load(states, reads)
    identities = [identity for identity, _ in read.values()]
    read_twice = len(identities) != len(set(identities))
    loaded = {key: value for key, (_, value) in read.items()}
    window = frame(
        change_rows(states, changes, deletes=deletes, old_images=old_images), FLAT
    )
    context = (
        f"changes={changes} reads={reads} deletes={deletes} "
        f"old_images={old_images} read_twice={read_twice}"
    )
    try:
        kept = dedup(load(*sorted(loaded.items())), window)
    except SqlCdcException as error:
        assert "CDC_23" in str(error), f"{context}: {error}"
        assert any(change[0] == "m" for change in changes), (
            f"{context}: failed with no key change in the window"
        )
        return False
    final = {key: value for key, (_, value) in states[-1].items()}
    assert replay_strictly(loaded, kept) == final, context
    return True


def every_read(keys: list[int], moments: int):
    """Every way the load can have read `keys`, one moment each."""
    for combination in itertools.product(range(moments + 1), repeat=len(keys)):
        yield dict(zip(keys, combination))


IMAGE_STYLES = [("old", True), ("old", False), ("new", True), ("new", False)]
IMAGE_STYLE_IDS = ["pg-full", "pg-default", "db2-full", "db2-default"]

BIZARRE_HISTORIES = {
    "born and dead twice, then born": (
        {},
        [("i", 1, "a"), ("d", 1), ("i", 1, "a"), ("d", 1), ("i", 1, "b")],
    ),
    "update ping-pong": (
        {1: "a"},
        [("u", 1, "b"), ("u", 1, "a"), ("u", 1, "b"), ("u", 1, "a")],
    ),
    "delete and reinsert the same contents": (
        {1: "a"},
        [("d", 1), ("i", 1, "a"), ("u", 1, "a"), ("d", 1), ("i", 1, "a")],
    ),
    "repeated no-op updates": (
        {1: "a"},
        [("u", 1, "a"), ("u", 1, "a"), ("u", 1, "b"), ("u", 1, "b")],
    ),
    "born, changed and dead": ({}, [("i", 1, "a"), ("u", 1, "b"), ("d", 1)]),
    "dead, then updates of a reborn row": (
        {1: "a"},
        [("d", 1), ("i", 1, "b"), ("u", 1, "c"), ("u", 1, "b"), ("u", 1, "a")],
    ),
    "two keys interleaved": (
        {1: "a", 2: "b"},
        [
            ("u", 1, "x"),
            ("d", 2),
            ("i", 2, "y"),
            ("u", 1, "a"),
            ("d", 1),
            ("u", 2, "z"),
            ("i", 1, "q"),
        ],
    ),
    "a key emptied and refilled by another's values": (
        {1: "a", 2: "b"},
        [("d", 1), ("u", 2, "a"), ("i", 1, "b"), ("d", 2), ("i", 2, "a")],
    ),
}

KEY_CHANGE_HISTORIES = {
    "a move to a free key": ({1: "a"}, [("m", 1, 3, "a")]),
    "a move with new contents, then updated": (
        {1: "a"},
        [("m", 1, 2, "b"), ("u", 2, "c")],
    ),
    "a chain of moves": ({1: "a"}, [("m", 1, 2, "a"), ("m", 2, 3, "a")]),
    "a swap through a spare key": (
        {1: "a", 2: "b"},
        [("m", 1, 3, "a"), ("m", 2, 1, "b"), ("m", 3, 2, "a")],
    ),
    "moved away and back, then deleted and reborn": (
        {1: "a"},
        [("m", 1, 2, "b"), ("u", 2, "c"), ("m", 2, 1, "c"), ("d", 1), ("i", 2, "d")],
    ),
    "born inside the window, moved and dead": (
        {},
        [("i", 1, "a"), ("m", 1, 2, "a"), ("d", 2)],
    ),
}


def keys_of(initial: dict[int, str], changes: list[tuple]) -> list[int]:
    keys = set(initial)
    for change in changes:
        keys.update(part for part in change[1:] if isinstance(part, int))
        if change[0] == "m":
            keys.add(change[2])
    return sorted(keys)


@pytest.mark.parametrize("style", IMAGE_STYLES, ids=IMAGE_STYLE_IDS)
@pytest.mark.parametrize("name", list(BIZARRE_HISTORIES))
def test_a_bizarre_history_ends_at_the_source_whenever_the_load_read_it(name, style):
    initial, changes = BIZARRE_HISTORIES[name]
    deletes, old_images = style
    for reads in every_read(keys_of(initial, changes), len(changes)):
        assert check_history(
            initial, changes, reads, deletes=deletes, old_images=old_images
        )


@pytest.mark.parametrize("style", IMAGE_STYLES, ids=IMAGE_STYLE_IDS)
@pytest.mark.parametrize("name", list(KEY_CHANGE_HISTORIES))
def test_a_key_change_ends_at_the_source_or_fails_safely(name, style):
    initial, changes = KEY_CHANGE_HISTORIES[name]
    deletes, old_images = style
    outcomes = [
        check_history(initial, changes, reads, deletes=deletes, old_images=old_images)
        for reads in every_read(keys_of(initial, changes), len(changes))
    ]
    assert any(outcomes), "no way the load could have read it was deduplicated"


def random_history(rnd: random.Random, keys: int, length: int):
    initial = {key: f"v{rnd.randrange(3)}" for key in range(keys) if rnd.random() < 0.5}
    rows = dict(initial)
    changes: list[tuple] = []
    while len(changes) < length:
        free = [key for key in range(keys) if key not in rows]
        held = list(rows)
        match rnd.choice(["i", "u", "u", "d", "m"]):
            case "i" if free:
                key = rnd.choice(free)
                rows[key] = f"v{rnd.randrange(3)}"
                changes.append(("i", key, rows[key]))
            case "u" if held:
                key = rnd.choice(held)
                rows[key] = f"v{rnd.randrange(3)}"
                changes.append(("u", key, rows[key]))
            case "d" if held:
                key = rnd.choice(held)
                del rows[key]
                changes.append(("d", key))
            case "m" if held and free:
                old, new = rnd.choice(held), rnd.choice(free)
                rows[new] = f"v{rnd.randrange(3)}"
                del rows[old]
                changes.append(("m", old, new, rows[new]))
    return initial, changes


@pytest.mark.parametrize("seed", range(20))
def test_random_histories_end_at_the_source(seed):
    rnd = random.Random(20260930 + seed)
    for _ in range(25):
        initial, changes = random_history(rnd, keys=5, length=rnd.randrange(1, 13))
        keys = keys_of(initial, changes)
        if rnd.random() < 0.5:
            reads = {key: rnd.randrange(len(changes) + 1) for key in keys}
        else:
            moment = rnd.randrange(len(changes) + 1)
            reads = dict.fromkeys(keys, moment)
        deletes, old_images = rnd.choice(IMAGE_STYLES)
        check_history(initial, changes, reads, deletes=deletes, old_images=old_images)


def test_a_point_in_time_load_ends_at_the_source_or_fails_safely():
    rnd = random.Random(7)
    for _ in range(200):
        initial, changes = random_history(rnd, keys=5, length=rnd.randrange(1, 13))
        moment = rnd.randrange(len(changes) + 1)
        reads = dict.fromkeys(keys_of(initial, changes), moment)
        check_history(initial, changes, reads)


def test_a_key_taken_again_after_its_row_moved_fails_the_load_safely():
    """Read after both changes, key 1 holds the new row and key 2 the moved
    one, so when the key change comes up both hold a row, as they would for a
    row read twice: the load fails rather than guess which it is."""
    initial = {1: "a"}
    changes = [("m", 1, 2, "a"), ("i", 1, "b")]
    assert check_history(initial, changes, {1: 2, 2: 2}) is False
    assert check_history(initial, changes, {1: 0, 2: 0}) is True


# ---- exactly the changes that apply ----------------------------------------------
#
# The rules, written plainly: which of a window's changes apply, in turn, to
# what the load read. The engine must keep exactly these, in every layout.


def changes_that_apply(loaded: dict[int, str], rows: list[tuple]) -> list[int] | None:
    """The positions of the `rows` that apply in turn to `loaded`, or ``None``
    when a key change would put a row on a held key."""
    state = dict(loaded)
    applying = []
    for position, (op, new, old) in enumerate(rows):
        if op == "i":
            applies = new[0] not in state
            if applies:
                state[new[0]] = new[1]
        elif op == "d":
            key = (old or new)[0]
            applies = key in state
            if applies:
                del state[key]
        elif old is None or old[0] == new[0]:
            applies = new[0] in state and state[new[0]] != new[1]
            if applies:
                state[new[0]] = new[1]
        else:
            if old[0] in state and new[0] in state:
                return None
            applies = old[0] in state or new[0] not in state
            if applies:
                state.pop(old[0], None)
                state[new[0]] = new[1]
        if applies:
            applying.append(position)
    return applying


def kept_positions(
    loaded: dict[int, str], rows: list[tuple], cdc_format: CdcFormat
) -> list[int] | None:
    changes = frame(rows, cdc_format)
    try:
        result = dedup(
            load(*sorted(loaded.items()), cdc_format=cdc_format), changes, cdc_format
        )
    except SqlCdcException as error:
        assert "CDC_23" in str(error), error
        return None
    positions = [row[changes.columns.index(CDC_META_SEG)] for row in result.rows()]
    assert result.rows() == [changes.rows()[position] for position in positions]
    return positions


def windows_of(histories: dict[str, tuple]):
    """Every window a load of `histories` can see: each history, every way the
    load can have read it, every image style."""
    for initial, changes in histories.values():
        states = simulate(initial, changes)
        for reads in every_read(keys_of(initial, changes), len(changes)):
            loaded = {
                key: value
                for key, (_, value) in read_by_the_load(states, reads).items()
            }
            for deletes, old_images in IMAGE_STYLES:
                rows = change_rows(
                    states, changes, deletes=deletes, old_images=old_images
                )
                yield loaded, rows


@pytest.mark.parametrize("cdc_format", FORMATS, ids=FORMAT_IDS)
def test_exactly_the_changes_that_apply_are_kept(cdc_format):
    for loaded, rows in windows_of(BIZARRE_HISTORIES | KEY_CHANGE_HISTORIES):
        assert kept_positions(loaded, rows, cdc_format) == changes_that_apply(
            loaded, rows
        ), (loaded, rows)


@pytest.mark.parametrize("cdc_format", FORMATS, ids=FORMAT_IDS)
def test_exactly_the_changes_that_apply_are_kept_in_random_windows(cdc_format):
    rnd = random.Random(31)
    for _ in range(150):
        initial, changes = random_history(rnd, keys=5, length=rnd.randrange(1, 13))
        states = simulate(initial, changes)
        reads = {
            key: rnd.randrange(len(changes) + 1) for key in keys_of(initial, changes)
        }
        loaded = {
            key: value for key, (_, value) in read_by_the_load(states, reads).items()
        }
        deletes, old_images = rnd.choice(IMAGE_STYLES)
        rows = change_rows(states, changes, deletes=deletes, old_images=old_images)
        assert kept_positions(loaded, rows, cdc_format) == changes_that_apply(
            loaded, rows
        ), (loaded, rows)


# Worked by hand: a key read at different moments of
# insert a, delete, insert a, delete, insert b.
BORN_AND_DEAD = [
    ("i", (1, "a"), None),
    ("d", None, (1, "a")),
    ("i", (1, "a"), None),
    ("d", None, (1, "a")),
    ("i", (1, "b"), None),
]


@pytest.mark.parametrize(
    ("loaded", "expected"),
    [
        ({}, [0, 1, 2, 3, 4]),  # read before the first insert: all apply
        ({1: "a"}, [1, 2, 3, 4]),  # read after an insert: that insert is dropped
        ({1: "b"}, [1, 2, 3, 4]),  # read at the end: replayed up to its value
    ],
    ids=["read-first", "read-after-an-insert", "read-last"],
)
def test_a_born_and_dead_key_keeps_exactly_what_applies(loaded, expected):
    assert kept_positions(loaded, BORN_AND_DEAD, FLAT) == expected


def test_an_update_ping_pong_keeps_exactly_what_applies():
    """a -> b -> a -> b: read while the row holds a (before the first update
    or after the second), every update applies; read while it holds b (after
    the first or the last), the first writes what the row already holds."""
    rows = [
        ("u", (1, "b"), (1, "a")),
        ("u", (1, "a"), (1, "b")),
        ("u", (1, "b"), (1, "a")),
    ]
    assert kept_positions({1: "a"}, rows, FLAT) == [0, 1, 2]
    assert kept_positions({1: "b"}, rows, FLAT) == [1, 2]


# ---- scale --------------------------------------------------------------------


def test_a_large_window_over_a_large_load_is_fast():
    loaded_rows, window_changes = 1_000_000, 100_000
    loaded = load(*((i, f"n{i}") for i in range(loaded_rows)))
    rnd = random.Random(7)
    rows = []
    for i in range(window_changes):
        k = rnd.randrange(loaded_rows)
        match rnd.randrange(4):
            case 0:
                rows.append(("i", (k, f"n{k}"), None))
            case 1:
                rows.append(("i", (loaded_rows + i, "new"), None))
            case 2:
                rows.append(("u", (k, f"x{i}"), None))
            case _:
                rows.append(("d", None, (k, f"n{k}")))
    changes = frame(rows, FLAT)

    started = time.perf_counter()
    result = dedup(loaded, changes)
    elapsed = time.perf_counter() - started

    assert 0 < result.height < window_changes
    assert elapsed < 10, f"deduplicating took {elapsed:.1f}s"
