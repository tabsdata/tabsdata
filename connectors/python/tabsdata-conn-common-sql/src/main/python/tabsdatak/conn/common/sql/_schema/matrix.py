#
# Copyright 2026 Tabsdata Inc.
#

"""What each destination does with the `schema_evolution` policy and the engine
a user writes, per `if_table_exists`: the one source the destinations validate
against, the plugins resolve from, registration reports, and the docstring
tables and `SCHEMA_EVOLUTION.md` are rendered from."""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from typing import Any, Literal, TypeAlias

from tabsdatak.conn.common.sql.error import SqlCommonErrorCode

Disposition: TypeAlias = Literal["append", "replace", "merge"]
Policy: TypeAlias = Literal["strict", "update"]
Engine: TypeAlias = Literal["native", "iceberg"]
Status: TypeAlias = Literal["offered", "planned", "impossible"]

DISPOSITIONS: tuple[Disposition, ...] = ("append", "replace", "merge")
POLICIES: tuple[Policy, ...] = ("strict", "update")
ENGINES: tuple[Engine, ...] = ("native", "iceberg")

NOTES: dict[int, str] = {
    1: "Native evolution on BigQuery and Snowflake adds columns and makes"
    " columns nullable, but never widens a type.",
    2: "Their native evolution doesn't apply to MERGE. To evolve a merge, write"
    " the engine iceberg explicitly; update alone is rejected with a message"
    " saying so.",
    3: "Strict refuses a wider type unless its values fit, as on every other"
    " destination.",
    4: "Druid can't enforce a schema, so update is its only policy and its default.",
    5: "ClickHouse has no MERGE. There are two workable designs: a"
    " ReplacingMergeTree table keyed on the merge columns, cheap but"
    " deduplicated only eventually, so readers need FINAL; or rebuilding the"
    " touched partitions and swapping them in with REPLACE PARTITION, exact"
    " and atomic, but each touched partition is rewritten. Which one to use is"
    " still to be defined.",
    6: "StarRocks upserts by key when it loads into a Primary Key table, and"
    " deletes through __op. The target must be a Primary Key table whose key"
    " is the merge columns.",
}


@dataclass(frozen=True)
class Row:
    """One destination and disposition."""

    engine: Engine | None
    status: Status = "offered"
    detail: str = ""
    default: Policy = "strict"
    policies: tuple[Policy, ...] = POLICIES
    # Update takes effect only with the engine written explicitly as iceberg.
    explicit: bool = False
    # Footnote per cell: "status", "left_out", "strict", "update", "iceberg".
    marks: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class Destination:
    name: str
    rows: dict[Disposition, Row]
    # The dest_cfg key an engine is written in, where one is accepted at all.
    engine_key: str | None = None


@dataclass(frozen=True)
class Resolution:
    """What a write does: the policy it applies and the engine that applies
    it."""

    if_table_exists: Disposition
    schema_evolution: Policy
    engine: Engine

    def options(self) -> dict[str, str]:
        return {
            "if_table_exists": self.if_table_exists,
            "schema_evolution": self.schema_evolution,
            "schema_evolution_engine": self.engine,
        }


def _offered(engine: Engine, **kwargs) -> Row:
    return Row(engine=engine, **kwargs)


def _planned(detail: str, **kwargs) -> Row:
    return Row(engine=None, status="planned", detail=detail, **kwargs)


_NATIVE_UPDATE = {"update": 1}
_SQL_STRICT = {"left_out": 3, "strict": 3}


def _warehouse(name: str, key: str) -> Destination:
    return Destination(
        name=name,
        engine_key=key,
        rows={
            "append": _offered("native", marks=_NATIVE_UPDATE),
            "replace": _offered("native", marks=_NATIVE_UPDATE),
            "merge": _offered(
                "iceberg", explicit=True, marks={"update": 2, "iceberg": 2}
            ),
        },
    )


def _relational(name: str, merge: str) -> Destination:
    return Destination(
        name=name,
        rows={
            "append": _offered("iceberg", marks=_SQL_STRICT),
            "replace": _offered("iceberg", marks=_SQL_STRICT),
            "merge": _planned(f"backend can ({merge})"),
        },
    )


MATRIX: dict[str, Destination] = {
    "bigquery": _warehouse("BigQuery", "bigquery.schema_evolution_engine"),
    "snowflake": _warehouse("Snowflake", "snowflake.schema_evolution_engine"),
    "databricks": Destination(
        name="Databricks",
        rows={disposition: _offered("native") for disposition in DISPOSITIONS},
    ),
    "awsglue": Destination(
        name="AWS Glue",
        rows={
            "append": _offered("native"),
            "replace": _offered("native"),
        },
    ),
    "clickhouse": Destination(
        name="ClickHouse",
        rows={
            "append": _offered("iceberg"),
            "replace": _offered("iceberg"),
            "merge": _planned("semantics to define", marks={"status": 5}),
        },
    ),
    "starrocks": Destination(
        name="StarRocks",
        rows={
            "append": _offered("iceberg"),
            "replace": _offered("iceberg"),
            "merge": _planned("backend can (Primary Key upsert)", marks={"status": 6}),
        },
    ),
    "postgres": _relational("Postgres", "MERGE 15+, ON CONFLICT"),
    "mysql": _relational("MySQL", "ON DUPLICATE KEY"),
    "mssql": _relational("SQL Server", "MERGE"),
    "oracle": _relational("Oracle", "MERGE"),
    "db2": _relational("Db2", "MERGE"),
    "druid": Destination(
        name="Druid",
        rows={
            "append": _offered(
                "native", default="update", policies=("update",), marks={"left_out": 4}
            ),
            "replace": _offered(
                "native",
                detail="time ranges",
                default="update",
                policies=("update",),
                marks={"left_out": 4},
            ),
            "merge": Row(engine=None, status="impossible", detail="no keyed upsert"),
        },
    ),
}


def _refuse(
    destination: Destination,
    disposition: str,
    policy: str | None,
    engine: str | None,
    reason: str,
) -> Exception:
    written = f"if_table_exists={disposition!r}"
    if policy is not None:
        written += f", schema_evolution={policy!r}"
    if engine is not None:
        written += f", {destination.engine_key or 'engine'}={engine!r}"
    return SqlCommonErrorCode.SQL_COMMON_11.exception(
        destination=destination.name, written=written, reason=reason
    )


def resolve(
    destination: str,
    disposition: str,
    policy: str | None = None,
    engine: str | None = None,
) -> Resolution:
    """The policy and engine a write to ``destination`` applies when the user
    writes ``disposition``, ``policy`` and ``engine`` (``None`` when left out),
    or ``SQL_COMMON_11`` naming why the combination is not supported."""
    entry = MATRIX[destination]
    row = entry.rows.get(disposition)  # type: ignore[arg-type]
    if row is None:
        reason = (
            f"{entry.name} does not {disposition}"
            if disposition in DISPOSITIONS
            else "no such disposition"
        )
        raise _refuse(entry, disposition, policy, engine, reason)
    if row.status == "planned":
        raise _refuse(
            entry,
            disposition,
            policy,
            engine,
            f"{disposition} is planned for {entry.name} but not offered yet",
        )
    if row.status == "impossible" or row.engine is None:
        raise _refuse(
            entry,
            disposition,
            policy,
            engine,
            f"{entry.name} cannot {disposition}: {row.detail}",
        )
    resolved = row.default if policy is None else policy
    if resolved not in row.policies:
        allowed = " or ".join(repr(p) for p in row.policies)
        raise _refuse(
            entry,
            disposition,
            policy,
            engine,
            f"{entry.name} cannot enforce a schema; schema_evolution is {allowed}",
        )
    if engine is not None:
        if row.explicit and resolved == "update" and engine == "iceberg":
            return Resolution(disposition, resolved, "iceberg")  # type: ignore[arg-type]
        if row.explicit:
            reason = (
                "the engine is written only as 'iceberg', with"
                " schema_evolution='update', to evolve a merge"
            )
        else:
            reason = (
                f"{entry.name} {disposition} evolves with its {row.engine}"
                " engine; leave the engine out"
            )
        raise _refuse(entry, disposition, policy, engine, reason)
    if row.explicit and resolved == "update":
        raise _refuse(
            entry,
            disposition,
            policy,
            engine,
            f"{entry.name}'s native evolution does not apply to MERGE; to evolve"
            f" a merge, write dest_cfg={{{entry.engine_key!r}: 'iceberg'}}",
        )
    return Resolution(disposition, resolved, row.engine)  # type: ignore[arg-type]


def resolve_dest(destination: str, dest: Any) -> Resolution:
    """:func:`resolve` for a destination descriptor: its `if_table_exists`, its
    `schema_evolution` and, where the destination takes one, the engine written
    in its `dest_cfg`."""
    key = MATRIX[destination].engine_key
    engine = (getattr(dest, "dest_cfg", None) or {}).get(key) if key else None
    return resolve(destination, dest.if_table_exists, dest.schema_evolution, engine)


# --- Rendering --------------------------------------------------------------

LEFT_OUT, STRICT, UPDATE, ICEBERG, OTHER = (
    "left out",
    "strict",
    "update",
    "update+iceberg",
    "other engine",
)
WRITTEN = (LEFT_OUT, STRICT, UPDATE, ICEBERG, OTHER)


def cell(row: Row, written: str) -> str:
    """What ``written`` resolves to on ``row``, as the tables show it."""
    if row.status == "planned":
        return "PLANNED"
    if row.status == "impossible" or row.engine is None:
        return "INVALID"

    def outcome(policy: Policy, engine: Engine) -> str:
        return "strict" if policy == "strict" else f"update/{engine}"

    if written == LEFT_OUT:
        if row.explicit and row.default == "update":
            return "INVALID"
        return outcome(row.default, row.engine)
    if written == STRICT:
        return "strict" if "strict" in row.policies else "INVALID"
    if written == UPDATE:
        if "update" not in row.policies or row.explicit:
            return "INVALID"
        return outcome("update", row.engine)
    if written == ICEBERG:
        return "update/iceberg" if row.explicit else "INVALID"
    return "INVALID"


def _engine_cell(row: Row) -> str:
    if row.status == "planned":
        return "PLANNED"
    return row.engine or "-"


def _grid(header: list[str], body: list[list[str]]) -> list[str]:
    widths = [max(len(line[i]) for line in [header, *body]) for i in range(len(header))]
    rule = "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    def line(cells: list[str]) -> str:
        return "| " + " | ".join(c.ljust(w) for c, w in zip(cells, widths)) + " |"

    return [rule, line(header), rule, *(line(cells) for cells in body), rule]


def _notes_for(entry: Destination) -> list[int]:
    return sorted({mark for row in entry.rows.values() for mark in row.marks.values()})


def docstring_table(destination: str) -> str:
    """The ASCII table of ``destination``'s rows, one column per
    `if_table_exists`, and the notes that apply to it, for its docstring."""
    entry = MATRIX[destination]
    dispositions = [d for d in DISPOSITIONS if d in entry.rows]
    rows = [entry.rows[d] for d in dispositions]
    body = [["engine", *(_engine_cell(row) for row in rows)]]
    body += [[written, *(cell(row, written) for row in rows)] for written in WRITTEN]
    lines = _grid(["", *dispositions], body)
    for disposition, row in zip(dispositions, rows):
        if row.status != "offered":
            lines.append("")
            lines += textwrap.wrap(
                f"{disposition}: {row.status} -- {row.detail}.", width=68
            )
    if entry.engine_key is not None:
        lines.append("")
        lines += textwrap.wrap(
            f"The engine is written as dest_cfg={{{entry.engine_key!r}: 'iceberg'}}.",
            width=68,
        )
    for number in _notes_for(entry):
        lines.append("")
        lines += textwrap.wrap(
            f"[{number}] {NOTES[number]}", width=68, subsequent_indent="    "
        )
    return "\n".join(lines)


def docstring_section(destination: str) -> str:
    """The `Schema evolution:` section of ``destination``'s docstring, as it
    reads once the docstring is dedented."""
    table = textwrap.indent(docstring_table(destination), " " * 8)
    return "\n".join([
        "Schema evolution:",
        "    What each written `schema_evolution`, and engine, resolves to",
        "    per `if_table_exists`; registration refuses every INVALID and",
        "    PLANNED combination and reports the one it resolves:",
        "",
        "    .. code-block:: text",
        "",
        table,
    ])


_SUPERSCRIPTS = {1: "¹", 2: "²", 3: "³", 4: "⁴", 5: "⁵", 6: "⁶"}


def _marked(text: str, row: Row, key: str) -> str:
    mark = row.marks.get(key)
    return f"{text} {_SUPERSCRIPTS[mark]}" if mark else text


def _status_cell(row: Row) -> str:
    if row.status == "offered":
        text = f"offered ({row.detail})" if row.detail else "offered"
    else:
        text = f"{row.status} · {row.detail}"
    return _marked(text, row, "status")


def _md_cell(row: Row, written: str) -> str:
    text = cell(row, written).replace("/", " · ")
    if text in ("PLANNED",) or (text == "INVALID" and written != UPDATE):
        return text
    key = {LEFT_OUT: "left_out", STRICT: "strict", UPDATE: "update"}.get(
        written, "iceberg"
    )
    return _marked(text, row, key)


def markdown() -> str:
    """The whole matrix, every destination, as ``SCHEMA_EVOLUTION.md``."""
    header = [
        "Destination",
        "Disposition",
        "Engine",
        "Status",
        "left out →",
        "strict",
        "update",
        "update + iceberg",
        "any other engine",
    ]
    lines = [
        "<!-- Copyright 2026 Tabsdata Inc. -->",
        "",
        "# Schema evolution by destination",
        "",
        "Generated from `tabsdatak/conn/common/sql/_schema/matrix.py`, which the",
        "destinations validate against, the plugins resolve from and registration",
        "reports; `test_matrix.py` fails when this file and the module disagree.",
        "Regenerate it with:",
        "",
        "```",
        "python -c 'from tabsdatak.conn.common.sql._schema import matrix;"
        ' print(matrix.markdown(), end="")\' > SCHEMA_EVOLUTION.md',
        "```",
        "",
        "## Rules",
        "",
        "- Writes are data only. A replace empties the table (TRUNCATE; DELETE on",
        "  Db2, which takes TRUNCATE only as the first statement of a unit of",
        "  work) and inserts the batch: under strict it just inserts, under update",
        "  it expands the schema first. A batch the policy refuses leaves the rows",
        "  in place. Schemas only grow.",
        "- One engine per destination and disposition: native where the backend",
        "  supports that operation, otherwise iceberg. The engine is never written,",
        "  except iceberg with update on a BigQuery or Snowflake merge.",
        "- A policy left out resolves to strict, except on Druid, where update is",
        "  the only policy.",
        "- Each destination's `validate()` resolves what the user wrote through",
        "  `matrix.resolve()`, so registration rejects every INVALID and PLANNED",
        "  combination (`SQL_COMMON_11`), and `tdk fn register` reports the",
        "  resolved disposition, policy and engine. The plugins write with the",
        "  same resolution.",
        "- Implementation paths the matrix does not offer (iceberg on a BigQuery or",
        "  Snowflake append, for one) may stay in the code, but registration never",
        "  lets a write reach them.",
        "",
        "## Matrix",
        "",
        "How to read the table:",
        "",
        "- Columns are what the user writes. *any other engine* covers native",
        "  written explicitly, and iceberg written either without update or where",
        "  native already applies.",
        "- *left out →* is what registration applies when the policy is left out.",
        "- PLANNED means not offered yet and pending definition; registration",
        "  rejects it until then.",
        "",
        "| " + " | ".join(header) + " |",
        "|" + "|".join("---" for _ in header) + "|",
    ]
    for entry in MATRIX.values():
        for disposition in DISPOSITIONS:
            row = entry.rows.get(disposition)
            if row is None:
                continue
            engine = _engine_cell(row)
            if row.explicit:
                engine = f"{engine} (explicit)"
            if row.status == "impossible":
                engine = "—"
            cells = [
                entry.name,
                disposition,
                engine,
                _status_cell(row),
                *(_md_cell(row, written) for written in WRITTEN),
            ]
            lines.append("| " + " | ".join(cells) + " |")
    lines += ["", "## Notes", ""]
    for number, note in NOTES.items():
        lines += textwrap.wrap(f"{number}. {note}", width=80, subsequent_indent="   ")
    lines.append("")
    return "\n".join(lines)
