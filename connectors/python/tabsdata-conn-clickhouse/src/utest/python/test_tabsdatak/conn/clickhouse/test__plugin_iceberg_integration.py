"""End-to-end Iceberg schema-evolution tests for the ClickHouse
destination plugin.

S3 only by design: the iceberg evolution logic is dialect-side
(:func:`tabsdatak.conn.clickhouse._dialect.ClickHouseDialect.compute_evolution_delta`
plus :class:`tabsdatak.conn.clickhouse._iceberg.ClickHouseRenderer`)
and entirely provider-agnostic -- it computes a
:class:`SchemaDelta`, renders ClickHouse DDL, and applies it via
the ClickHouse client. The cloud-staging path is what differs
across S3 / Azure / GCS, and the bread-and-butter scenarios in
``test__plugin_integration.py`` already cover that side end-to-end
for every provider.

Running this file against Azure or GCS would only re-validate
"cloud I/O still works" -- not iceberg semantics -- so the whole
module is skipped when ``CLICKHOUSE0__STAGING_PROVIDER`` is
anything other than ``s3``.

The detailed delta-algebra cases (compute_delta + renderer) are
unit-tested in ``test__iceberg.py``; this file verifies each
variant actually lands as a real ``ALTER TABLE`` on the live
server and the subsequent ``INSERT`` works.
"""

from __future__ import annotations

import os

import polars as pl
import pytest

from tabsdatak.conn.clickhouse import ClickHouseDest
# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import _plugin as plugin
from tabsdatak.conn.clickhouse.error import (
    ClickHouseErrorCode,
    ClickHouseRuntimeException,
)
# noinspection PyProtectedMember
from test_tabsdatak.conn.clickhouse._integration_helpers import (
    _ch_client,
    _column_types,
    _conn_from,
    _count_rows,
    _dest_ctx,
    _make_parquet,
)


_PROVIDER = (
    os.environ.get("CLICKHOUSE0__STAGING_PROVIDER", "s3")
    .strip()
    .lower()
)

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        _PROVIDER != "s3",
        reason=(
            "Iceberg integration tests are S3-only by design; the"
            " evolution logic is provider-agnostic and running it"
            " against Azure / GCS would only re-validate cloud I/O,"
            " not iceberg semantics."
        ),
    ), pytest.mark.xdist_group("clickhouse-plugin_iceberg_integration_seq")]


# ---------------------------------------------------------------------------
# Small helpers used by the scenarios below
# ---------------------------------------------------------------------------


def _write(fresh_clickhouse, work_dir, parquet_path, table, **dest_kwargs):
    """Shorthand: invoke the dest plugin once with the given parquet
    + destination spelling + optional dest_kwargs (e.g.
    ``schema_evolution="iceberg"``)."""
    plugin._ClickHouseDestPlugin().write_out(
        _dest_ctx(work_dir),
        ClickHouseDest(
            conn=_conn_from(fresh_clickhouse),
            tables=[table],
            **dest_kwargs,
        ),
        [parquet_path],
    )


def _write_polars(path, df: pl.DataFrame):
    """Write ``df`` as Parquet so the iceberg path picks up the
    exact polars schema (used by tests that need precise dtypes,
    e.g. Int32 vs Int64)."""
    df.write_parquet(path, use_pyarrow=True)
    return path


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def test_iceberg_adds_nullable_column(
    fresh_clickhouse, work_dir, fqn_table,
):
    """A new nullable column in the incoming parquet lands as
    ``ALTER TABLE ADD COLUMN`` and the row is inserted with the
    new column populated."""
    db, tbl = fqn_table

    # Seed: {id, name}.
    seed = _make_parquet(
        work_dir / "seed.parquet",
        [{"id": 1, "name": "alice"}],
    )
    _write(fresh_clickhouse, work_dir, seed, tbl)

    # Evolve: add a new ``email`` column.
    evo = _make_parquet(
        work_dir / "evo.parquet",
        [{"id": 2, "name": "bob", "email": "bob@example.com"}],
    )
    _write(
        fresh_clickhouse, work_dir, evo, tbl,
        schema_evolution="iceberg",
    )

    client = _ch_client(fresh_clickhouse)
    cols = _column_types(client, db, tbl)
    assert "email" in cols
    # The added column is nullable.
    assert cols["email"].startswith("Nullable(")
    assert _count_rows(client, db, tbl) == 2


def test_iceberg_drops_column(
    fresh_clickhouse, work_dir, fqn_table,
):
    """A column present in the existing table but absent from the
    incoming parquet lands as ``ALTER TABLE DROP COLUMN``."""
    db, tbl = fqn_table

    # Seed: {id, name, old_col}.
    seed = _make_parquet(
        work_dir / "seed.parquet",
        [{"id": 1, "name": "alice", "old_col": "value"}],
    )
    _write(fresh_clickhouse, work_dir, seed, tbl)

    # Evolve: drop ``old_col``.
    evo = _make_parquet(
        work_dir / "evo.parquet",
        [{"id": 2, "name": "bob"}],
    )
    _write(
        fresh_clickhouse, work_dir, evo, tbl,
        schema_evolution="iceberg",
    )

    client = _ch_client(fresh_clickhouse)
    cols = _column_types(client, db, tbl)
    assert "old_col" not in cols
    assert "name" in cols
    assert _count_rows(client, db, tbl) == 2


def test_iceberg_widens_int_type(
    fresh_clickhouse, work_dir, fqn_table,
):
    """Widening ``Int32 -> Int64`` is safe and lands as
    ``ALTER TABLE MODIFY COLUMN``."""
    db, tbl = fqn_table

    seed = _write_polars(
        work_dir / "seed.parquet",
        pl.DataFrame({"id": [1]}, schema={"id": pl.Int32}),
    )
    _write(fresh_clickhouse, work_dir, seed, tbl)

    evo = _write_polars(
        work_dir / "evo.parquet",
        pl.DataFrame({"id": [2]}, schema={"id": pl.Int64}),
    )
    _write(
        fresh_clickhouse, work_dir, evo, tbl,
        schema_evolution="iceberg",
    )

    client = _ch_client(fresh_clickhouse)
    cols = _column_types(client, db, tbl)
    # ClickHouse may wrap the type in Nullable(); check the inner
    # type token is now Int64 either way.
    assert "Int64" in cols["id"]
    assert _count_rows(client, db, tbl) == 2


def test_iceberg_rejects_unsafe_narrowing(
    fresh_clickhouse, work_dir, fqn_table,
):
    """Narrowing ``Int64 -> Int32`` is unsafe under Iceberg
    semantics. ``validate_delta(strict=True)`` raises
    ``UnsafeSchemaChangeError`` inside ``compute_evolution_delta``;
    the plugin wraps that as ``CLICKHOUSE_6`` and the existing
    table is left untouched."""
    db, tbl = fqn_table

    seed = _write_polars(
        work_dir / "seed.parquet",
        pl.DataFrame({"id": [1]}, schema={"id": pl.Int64}),
    )
    _write(fresh_clickhouse, work_dir, seed, tbl)

    # Try to evolve to Int32 -- unsafe narrowing.
    evo = _write_polars(
        work_dir / "evo.parquet",
        pl.DataFrame({"id": [2]}, schema={"id": pl.Int32}),
    )
    with pytest.raises(ClickHouseRuntimeException) as ei:
        _write(
            fresh_clickhouse, work_dir, evo, tbl,
            schema_evolution="iceberg",
        )
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_6

    # Existing table is unchanged: original row only, type unchanged.
    client = _ch_client(fresh_clickhouse)
    cols = _column_types(client, db, tbl)
    assert "Int64" in cols["id"]
    assert _count_rows(client, db, tbl) == 1


def test_iceberg_combined_evolution(
    fresh_clickhouse, work_dir, fqn_table,
):
    """One write with multiple safe changes in the same delta:

    - widen ``id`` from Int32 to Int64
    - drop ``old_col``
    - add nullable ``category``
    """
    db, tbl = fqn_table

    seed = _write_polars(
        work_dir / "seed.parquet",
        pl.DataFrame(
            {"id": [1], "name": ["alice"], "old_col": ["value"]},
            schema={
                "id": pl.Int32,
                "name": pl.Utf8,
                "old_col": pl.Utf8,
            },
        ),
    )
    _write(fresh_clickhouse, work_dir, seed, tbl)

    # Evolve: widen id, drop old_col, add category.
    evo = _write_polars(
        work_dir / "evo.parquet",
        pl.DataFrame(
            {
                "id": [2],
                "name": ["bob"],
                "category": ["staff"],
            },
            schema={
                "id": pl.Int64,
                "name": pl.Utf8,
                "category": pl.Utf8,
            },
        ),
    )
    _write(
        fresh_clickhouse, work_dir, evo, tbl,
        schema_evolution="iceberg",
    )

    client = _ch_client(fresh_clickhouse)
    cols = _column_types(client, db, tbl)
    assert "old_col" not in cols          # dropped
    assert "Int64" in cols["id"]          # widened
    assert "category" in cols             # added
    assert cols["category"].startswith("Nullable(")  # added as nullable
    assert _count_rows(client, db, tbl) == 2
