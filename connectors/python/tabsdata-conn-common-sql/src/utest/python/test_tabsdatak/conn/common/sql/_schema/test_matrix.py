#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema import matrix
from tabsdatak.conn.common.sql.error import (
    SqlCommonErrorCode,
    SqlCommonValidateException,
)

pytestmark = pytest.mark.sql_common

# The agreed design, transcribed by hand rather than derived from the module:
# destination -> disposition -> (engine, left out, strict, update,
# update + iceberg, any other engine).
_P = ("PLANNED",) * 6
_SQL = {
    "append": ("iceberg", "strict", "strict", "update/iceberg", "INVALID", "INVALID"),
    "replace": ("iceberg", "strict", "strict", "update/iceberg", "INVALID", "INVALID"),
    "merge": _P,
}
_WAREHOUSE = {
    "append": ("native", "strict", "strict", "update/native", "INVALID", "INVALID"),
    "replace": ("native", "strict", "strict", "update/native", "INVALID", "INVALID"),
    "merge": ("iceberg", "strict", "strict", "INVALID", "update/iceberg", "INVALID"),
}
DESIGN = {
    "bigquery": _WAREHOUSE,
    "snowflake": _WAREHOUSE,
    "databricks": {
        d: ("native", "strict", "strict", "update/native", "INVALID", "INVALID")
        for d in ("append", "replace", "merge")
    },
    "awsglue": {
        "append": _WAREHOUSE["append"],
        "replace": _WAREHOUSE["replace"],
    },
    "clickhouse": _SQL,
    "starrocks": _SQL,
    "postgres": _SQL,
    "mysql": _SQL,
    "mssql": _SQL,
    "oracle": _SQL,
    "db2": _SQL,
    "druid": {
        "append": (
            "native",
            "update/native",
            "INVALID",
            "update/native",
            "INVALID",
            "INVALID",
        ),
        "replace": (
            "native",
            "update/native",
            "INVALID",
            "update/native",
            "INVALID",
            "INVALID",
        ),
        "merge": ("-", "INVALID", "INVALID", "INVALID", "INVALID", "INVALID"),
    },
}

_CASES = [
    (destination, disposition)
    for destination, rows in DESIGN.items()
    for disposition in rows
]


def test_the_matrix_covers_exactly_the_designed_destinations():
    assert set(matrix.MATRIX) == set(DESIGN)
    for destination, entry in matrix.MATRIX.items():
        assert set(entry.rows) == set(DESIGN[destination]), destination


@pytest.mark.parametrize("destination,disposition", _CASES)
def test_every_cell_matches_the_design(destination, disposition):
    row = matrix.MATRIX[destination].rows[disposition]
    expected = DESIGN[destination][disposition]
    engine = "PLANNED" if row.status == "planned" else (row.engine or "-")
    assert engine == expected[0]
    assert [matrix.cell(row, w) for w in matrix.WRITTEN] == list(expected[1:])


def _written(policy: str | None, engine: str | None) -> str:
    """The table column a written policy and engine fall in."""
    if engine is None:
        return {None: matrix.LEFT_OUT, "strict": matrix.STRICT}.get(
            policy, matrix.UPDATE
        )
    if policy == "update" and engine == "iceberg":
        return matrix.ICEBERG
    return matrix.OTHER


_COMBINATIONS = [
    (destination, disposition, policy, engine)
    for destination, disposition in _CASES
    for policy in (None, "strict", "update")
    for engine in (None, "native", "iceberg", "spark")
]


@pytest.mark.parametrize("destination,disposition,policy,engine", _COMBINATIONS)
def test_resolve_agrees_with_the_design_for_every_written_combination(
    destination, disposition, policy, engine
):
    names = (matrix.LEFT_OUT, *matrix.WRITTEN[1:])
    expected = DESIGN[destination][disposition][
        1 + names.index(_written(policy, engine))
    ]
    if expected in ("INVALID", "PLANNED"):
        with pytest.raises(SqlCommonValidateException) as raised:
            matrix.resolve(destination, disposition, policy, engine)
        assert raised.value.api_error is SqlCommonErrorCode.SQL_COMMON_11
        return
    resolved = matrix.resolve(destination, disposition, policy, engine)
    assert resolved.if_table_exists == disposition
    if expected == "strict":
        assert resolved.schema_evolution == "strict"
        assert resolved.engine == DESIGN[destination][disposition][0]
    else:
        policy_part, engine_part = expected.split("/")
        assert (resolved.schema_evolution, resolved.engine) == (
            policy_part,
            engine_part,
        )


def test_resolution_options_name_what_a_write_applies():
    assert matrix.resolve("bigquery", "merge", "update", "iceberg").options() == {
        "if_table_exists": "merge",
        "schema_evolution": "update",
        "schema_evolution_engine": "iceberg",
    }


def _message(*args) -> str:
    with pytest.raises(SqlCommonValidateException) as raised:
        matrix.resolve(*args)
    return str(raised.value)


def test_update_on_a_warehouse_merge_names_the_key_to_write():
    for destination, key in (
        ("bigquery", "bigquery.schema_evolution_engine"),
        ("snowflake", "snowflake.schema_evolution_engine"),
    ):
        message = _message(destination, "merge", "update")
        assert "native evolution does not apply to MERGE" in message
        assert f"dest_cfg={{'{key}': 'iceberg'}}" in message


def test_iceberg_without_update_on_a_warehouse_merge_says_it_needs_update():
    message = _message("bigquery", "merge", "strict", "iceberg")
    assert "schema_evolution='update'" in message
    assert "bigquery.schema_evolution_engine='iceberg'" in message


def test_an_engine_where_none_is_written_says_to_leave_it_out():
    message = _message("clickhouse", "append", "update", "iceberg")
    assert "ClickHouse append evolves with its iceberg engine" in message
    assert "leave the engine out" in message


def test_strict_on_druid_says_update_is_its_only_policy():
    message = _message("druid", "append", "strict")
    assert "Druid cannot enforce a schema" in message
    assert "schema_evolution is 'update'" in message


def test_a_planned_merge_says_it_is_not_offered_yet():
    assert "merge is planned for Postgres but not offered yet" in _message(
        "postgres", "merge"
    )


def test_a_druid_merge_says_why_it_is_impossible():
    assert "Druid cannot merge: no keyed upsert" in _message("druid", "merge")


def test_a_disposition_a_destination_leaves_out_is_refused():
    assert "AWS Glue does not merge" in _message("awsglue", "merge")


def test_an_unknown_disposition_is_refused():
    assert "no such disposition" in _message("postgres", "upsert")


def test_the_druid_default_is_update():
    resolved = matrix.resolve("druid", "replace")
    assert (resolved.schema_evolution, resolved.engine) == ("update", "native")


@pytest.mark.parametrize("destination", sorted(DESIGN))
def test_a_docstring_table_fits_80_columns_inside_a_code_block(destination):
    # A docstring indents its code blocks 12 columns.
    for line in matrix.docstring_table(destination).splitlines():
        assert len(line) <= 68, line
        assert line.isascii(), line


def test_a_docstring_table_carries_the_notes_of_its_destination_only():
    table = matrix.docstring_table("bigquery")
    assert "[1]" in table and "[2]" in table
    assert "[3]" not in table and "[4]" not in table
    assert "dest_cfg={'bigquery.schema_evolution_engine': 'iceberg'}" in table
    assert "[3]" in matrix.docstring_table("postgres")
    assert "merge: planned -- backend can (MERGE 15+, ON CONFLICT)." in (
        matrix.docstring_table("postgres")
    )


def _package_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "TYPE_MAPPING.md").is_file():
            return parent
    raise AssertionError("tabsdata-conn-common-sql root not found")


def test_schema_evolution_md_is_the_rendered_matrix():
    path = _package_root() / "SCHEMA_EVOLUTION.md"
    assert path.read_text(encoding="utf-8") == matrix.markdown(), (
        "SCHEMA_EVOLUTION.md is stale; regenerate it as its header says"
    )


def test_the_markdown_marks_the_notes_where_the_design_places_them():
    lines = {
        tuple(cell.strip() for cell in line.strip("|").split("|"))[:2]: line
        for line in matrix.markdown().splitlines()
        if line.startswith("| ") and not line.startswith("| Destination")
    }
    assert "update · native ¹" in lines[("BigQuery", "append")]
    assert "INVALID ²" in lines[("Snowflake", "merge")]
    assert "update · iceberg ²" in lines[("BigQuery", "merge")]
    assert "iceberg (explicit)" in lines[("BigQuery", "merge")]
    assert "strict ³" in lines[("Db2", "replace")]
    assert "update · native ⁴" in lines[("Druid", "append")]
    assert "offered (time ranges)" in lines[("Druid", "replace")]
    assert "planned · semantics to define ⁵" in lines[("ClickHouse", "merge")]
    assert "⁶" in lines[("StarRocks", "merge")]
    assert "impossible · no keyed upsert" in lines[("Druid", "merge")]
    assert len(lines) == len(_CASES)


def test_every_mark_names_a_note_and_every_note_is_marked():
    marked = {
        mark
        for entry in matrix.MATRIX.values()
        for row in entry.rows.values()
        for mark in row.marks.values()
    }
    assert marked == set(matrix.NOTES)


def test_every_offered_row_has_an_engine_and_a_default_it_allows():
    for name, entry in matrix.MATRIX.items():
        for disposition, row in entry.rows.items():
            if row.status != "offered":
                assert row.engine is None, (name, disposition)
                continue
            assert row.engine in matrix.ENGINES, (name, disposition)
            assert row.default in row.policies, (name, disposition)


def test_only_a_destination_taking_an_explicit_engine_names_its_key():
    for name, entry in matrix.MATRIX.items():
        explicit = any(row.explicit for row in entry.rows.values())
        assert (entry.engine_key is not None) == explicit, name


class _Dest:
    def __init__(self, disposition, policy, dest_cfg=None):
        self.if_table_exists = disposition
        self.schema_evolution = policy
        self.dest_cfg = dest_cfg


def test_resolve_dest_reads_the_engine_off_the_key_of_its_destination():
    key = {"bigquery.schema_evolution_engine": "iceberg"}
    resolved = matrix.resolve_dest("bigquery", _Dest("merge", "update", key))
    assert resolved.engine == "iceberg"


def test_resolve_dest_ignores_another_destinations_key():
    dest = _Dest("merge", "update", {"snowflake.schema_evolution_engine": "iceberg"})
    with pytest.raises(SqlCommonValidateException) as raised:
        matrix.resolve_dest("bigquery", dest)
    assert "native evolution does not apply to MERGE" in str(raised.value)


@pytest.mark.parametrize("dest_cfg", [None, {}, {"postgres.logging_level": 10}])
def test_resolve_dest_takes_no_engine_where_the_destination_has_no_key(dest_cfg):
    resolved = matrix.resolve_dest("postgres", _Dest("replace", "update", dest_cfg))
    assert (resolved.schema_evolution, resolved.engine) == ("update", "iceberg")


def test_a_refusal_names_what_was_written():
    with pytest.raises(SqlCommonValidateException) as raised:
        matrix.resolve("snowflake", "append", "update", "iceberg")
    message = str(raised.value)
    assert message.startswith("SQL_COMMON_11: Snowflake does not support")
    assert (
        "if_table_exists='append', schema_evolution='update',"
        " snowflake.schema_evolution_engine='iceberg'"
    ) in message


@pytest.mark.parametrize("destination", sorted(matrix.MATRIX))
def test_a_docstring_section_heads_and_indents_the_table(destination):
    section = matrix.docstring_section(destination).splitlines()
    assert section[0] == "Schema evolution:"
    assert "    .. code-block:: text" in section
    table = section[section.index("    .. code-block:: text") + 2 :]
    assert all(line == "" or line.startswith(" " * 8) for line in table)
