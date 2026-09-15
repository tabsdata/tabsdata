#
# Copyright 2026 Tabsdata Inc.
#

"""Putting a parquet file on a stage and reading it back off one.

The steps every disposition shares, whichever of them is driving:
``_impl`` for an append or a replace, ``_merge_dialect`` for a merge. They
live here rather than in either so both spell them the same way -- and so
neither has to import the other.
"""

from __future__ import annotations

from pathlib import Path

__all__ = [
    "PARQUET_FILE_FORMAT",
    "copy_into",
    "create_file_format",
    "infer_schema_call",
    "inferred_columns",
    "inferred_schema_template",
    "put_file_to_stage",
]

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


def put_file_to_stage(cursor, file_path: Path, stage: str) -> str:
    """``PUT`` ``file_path`` into ``@stage`` and return the basename
    snowflake will index it under."""
    # snowflake's PUT accepts a file:// URI; use absolute path so
    # the driver doesn't try to resolve against its own cwd.
    file_uri = f"file://{file_path.resolve()}"
    cursor.execute(f"PUT {file_uri} @{stage}")
    return file_path.name


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
    file_name: str,
    relation: str,
    *,
    match_by_column_name: str,
    purge: str,
) -> None:
    """``COPY`` the staged file into ``relation`` -- the target itself for an
    append or a replace, the merge's scratch table for a merge.

    Read through :data:`PARQUET_FILE_FORMAT`, the format that inferred the
    relation's columns, so the values arrive typed the way the columns were
    made -- see :func:`create_file_format`.

    ``purge`` removes the staged file once it has been ingested, so nothing
    downstream can read it again.
    """
    cursor.execute(
        f"COPY INTO {relation} FROM @{stage} FILES = ('{file_name}') "
        f"FILE_FORMAT = (FORMAT_NAME = '{PARQUET_FILE_FORMAT}') "
        f"MATCH_BY_COLUMN_NAME = {match_by_column_name} "
        f"PURGE = {purge}"
    )
