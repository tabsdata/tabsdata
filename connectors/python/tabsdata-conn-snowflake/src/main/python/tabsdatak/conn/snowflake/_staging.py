#
# Copyright 2026 Tabsdata Inc.
#

"""Putting a table on a stage as parquet parts and reading it back off one.

The steps every disposition shares, whichever of them is driving:
``_impl`` for an append or a replace, ``_merge`` for a merge. They
live here rather than in either so both spell them the same way -- and so
neither has to import the other.
"""

from __future__ import annotations

import contextlib
import io
import logging
import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

__all__ = [
    "PARQUET_FILE_FORMAT",
    "copy_into",
    "create_file_format",
    "first_part",
    "infer_schema_call",
    "inferred_columns",
    "inferred_schema_template",
    "parquet_parts",
    "put_frame_to_stage",
    "remove_from_stage",
    "staged_frame",
]

logger = logging.getLogger(__name__)

PART_BYTES = 256 * 1024 * 1024
PART_NAME = "part-{:05d}.parquet"
BATCH_ROWS = 65_536

# Session file format the staged parquet is read through: to template a
# relation from it, to infer its columns, and to load it. One definition for
# all three, so the load reads the file exactly as the inference that shaped
# its target did.
PARQUET_FILE_FORMAT = "tabsdata_parquet"


def create_file_format(cursor) -> None:
    """Create the named parquet format every ``infer_schema`` call and every
    ``COPY`` resolves.

    ``USE_LOGICAL_TYPE = TRUE`` is what makes Snowflake read a parquet logical
    type instead of the physical type beneath it. Without it a timestamp column
    is inferred as the ``NUMBER`` holding its epoch and a decimal arrives
    stripped of its precision and scale -- so a target templated from the
    inferred schema is built with the wrong types, and one that already has the
    right ones reads as unmergeable drift.

    The ``COPY`` has to read the file the same way, or the two disagree: a
    target inferred with logical types gets a ``TIMESTAMP_NTZ`` column, and a
    load without them hands that column the bare epoch count, which Snowflake
    takes for seconds and lands millions of years out (a ``TIME`` column refuses
    the count outright). ``copy_into`` names this format for that reason.

    ``TEMPORARY`` scopes the format to this session. Every statement that
    resolves the name runs on a cursor of the connection that created it, so
    another destination loading into the same schema can neither replace it
    under a running statement nor be refused for not owning it. It also shadows
    a permanent ``tabsdata_parquet`` an earlier connector version left in the
    schema, so a stale definition there is never the one read.
    """
    cursor.execute(
        f"CREATE TEMPORARY FILE FORMAT {PARQUET_FILE_FORMAT}"
        f" TYPE = PARQUET USE_LOGICAL_TYPE = TRUE"
    )


def parquet_parts(
    frame: pl.LazyFrame, part_bytes: int | None = None
) -> Iterator[io.BytesIO]:
    import polars as pl

    limit = PART_BYTES if part_bytes is None else part_bytes
    pending: list[pl.DataFrame] = []
    size: float = 0
    produced = False
    for batch in frame.collect_batches(chunk_size=BATCH_ROWS):
        pending.append(batch)
        size += batch.estimated_size()
        if size >= limit:
            yield _parquet(pending)
            pending, size, produced = [], 0, True
    if pending or not produced:
        yield _parquet(pending or [pl.DataFrame(schema=frame.collect_schema())])


def _parquet(frames: list[pl.DataFrame]) -> io.BytesIO:
    import polars as pl

    buffer = io.BytesIO()
    pl.concat(frames, how="vertical", rechunk=False).write_parquet(buffer)
    frames.clear()
    buffer.seek(0)
    return buffer


def put_frame_to_stage(cursor, frame: pl.LazyFrame, stage: str) -> str:
    """``PUT`` ``frame`` into a folder of its own on ``@stage``, as parquet
    parts of bounded size streamed from memory, and return the folder.

    No part touches the local disk, and at most one part is held at a time.
    """
    folder = uuid.uuid4().hex
    try:
        for index, part in enumerate(parquet_parts(frame)):
            name = PART_NAME.format(index)
            logger.debug("Putting %s on @%s/%s", name, stage, folder)
            cursor.execute(
                f"PUT 'file:///{name}' @{stage}/{folder} AUTO_COMPRESS = FALSE",
                file_stream=part,
            )
    except BaseException:
        remove_from_stage(cursor, stage, folder)
        raise
    return folder


@contextlib.contextmanager
def staged_frame(cursor, frame: pl.LazyFrame, stage: str) -> Iterator[str]:
    """Stage ``frame`` as :func:`put_frame_to_stage` does and yield its folder,
    removing the parts if the body fails -- a load that never ran has nothing
    to purge them."""
    folder = put_frame_to_stage(cursor, frame, stage)
    try:
        yield folder
    except BaseException:
        remove_from_stage(cursor, stage, folder)
        raise


def first_part(folder: str) -> str:
    return f"{folder}/{PART_NAME.format(0)}"


def remove_from_stage(cursor, stage: str, folder: str) -> None:
    try:
        cursor.execute(f"REMOVE @{stage}/{folder}/")
    except Exception:
        logger.warning(
            "Failed to remove the staged parts in @%s/%s", stage, folder, exc_info=True
        )


def infer_schema_call(stage: str, file_name: str) -> str:
    """The ``infer_schema`` table function reading the staged parquet file.

    ``ignore_case => true`` makes the inferred column names upper-folded, which
    is the case Snowflake stores an unquoted identifier under -- so the names it
    yields are the ones a templated relation ends up having.
    """
    return (
        f"infer_schema("
        f"location => '@{stage}/{file_name}', "
        f"file_format => '{PARQUET_FILE_FORMAT}', "
        f"ignore_case => true"
        f")"
    )


def inferred_columns(cursor, stage: str, file_name: str) -> list[tuple[str, str]]:
    """The ``(name, type)`` pairs ``infer_schema`` reports for the staged file.

    The types are ``infer_schema``'s own vocabulary -- ``TEXT`` where
    ``DESC TABLE`` says ``VARCHAR(16777216)`` for the same column -- so a
    caller diffing these against a table's own schema has to reconcile the two
    spellings first.

    Read before the ``COPY``, which purges the staged file: after that there is
    nothing left to infer from.
    """
    cursor.execute(
        f"SELECT COLUMN_NAME, TYPE FROM table ({infer_schema_call(stage, file_name)})"
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]


def inferred_schema_template(stage: str, file_name: str) -> str:
    """The ``USING template`` body describing the staged parquet file's schema."""
    return (
        f"SELECT array_agg(object_construct(*)) "
        f"FROM table ("
        f"{infer_schema_call(stage, file_name)}"
        f")"
    )


def copy_into(
    cursor,
    stage: str,
    folder: str,
    relation: str,
    *,
    match_by_column_name: str,
    purge: str,
) -> None:
    """``COPY`` every part staged in ``folder`` into ``relation`` -- the target
    itself for an append or a replace, the merge's scratch table for a merge.
    One statement, so the parts load together or not at all.

    Read through :data:`PARQUET_FILE_FORMAT`, the format that inferred the
    relation's columns, so the values arrive typed the way the columns were
    made -- see :func:`create_file_format`.

    ``purge`` removes the staged parts once they have been ingested, so nothing
    downstream can read them again.
    """
    cursor.execute(
        f"COPY INTO {relation} FROM @{stage}/{folder}/ "
        f"FILE_FORMAT = (FORMAT_NAME = '{PARQUET_FILE_FORMAT}') "
        f"MATCH_BY_COLUMN_NAME = {match_by_column_name} "
        f"PURGE = {purge}"
    )
