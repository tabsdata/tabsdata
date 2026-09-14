#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake's merge dialect, and the shared driver run against it.

The driver's ordering is covered by the write-path tests in ``test__impl``;
these cover what Snowflake contributes to it -- the statements it emits, how it
folds names, what it will and will not evolve -- plus the shared pieces read
through this dialect, so the wiring between the two is exercised rather than
assumed.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from test_tabsdatak.conn.snowflake.support import (
    _FakeConnection,
    _FakeCtx,
    _INFER_PREFIX,
    assignments as _assignments,
    column_fakes,
    insert_lists as _insert_lists,
    watermark_answers,
)

from tabsdatak.conn.common.sql.cdc.constants import CdcOperation

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._batch import coalesced_batch, incoming_column

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._driver import (
    MergeResult,
    merge_batch,
    write_columns,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._statement import render_merge

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import WatermarkCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import Staged
from tabsdatak.conn.snowflake import MergeCfg

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _iceberg

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake._merge_dialect import SnowflakeMergeDialect
from tabsdatak.conn.snowflake.error import (
    SnowflakeErrorCode,
    SnowflakeRuntimeException,
)

pytestmark = pytest.mark.snowflake

_TARGET = '"DB"."S"."T"'
_SCRATCH = '"DB"."S"."T_ABCD1234"'


def _dialect(fake: _FakeConnection) -> SnowflakeMergeDialect:
    return SnowflakeMergeDialect(
        fake.cursor(),
        "MY_STAGE",
        match_by_column_name="CASE_INSENSITIVE",
        purge="TRUE",
    )


def _staged(columns: list[tuple[str, str]], relation: str = _SCRATCH) -> Staged:
    return Staged(relation=relation, columns=list(columns))


def _cfg(on=("c",), op_column="op", watermark_column=None) -> MergeCfg:
    return MergeCfg(
        on=list(on), op_column=op_column, watermark_column=watermark_column
    )


def _alters(log: list[str]) -> list[str]:
    return [s for s in log if s.startswith("ALTER TABLE")]


# ---------------------------------------------------------------------------
# evolve -- ALTER TABLE for a widened batch
#
# The load matches by name, so a column the batch carries and the table lacks
# has nowhere to land. The table the connector creates is built from the same
# staged relation and so is never short a column; a table that already existed
# can be.
#
# The diff routes through the shared Iceberg delta machinery
# (``compute_delta`` + ``SnowflakeRenderer``), so these also pin what reaches
# the warehouse: an ADD_COLUMN, and nothing else. A column both sides have is
# left as it stands whatever its type has done.
# ---------------------------------------------------------------------------


def _evolve(table_columns, file_columns, target=_TARGET):
    """Evolve ``target`` against a batch, returning ``(fake, changed)``."""
    fake = column_fakes(table_columns, file_columns)
    changed = _dialect(fake).evolve(target, _staged(file_columns), "OP")
    return fake, changed


def test_evolve_adds_only_what_the_table_lacks():
    fake, added = _evolve(
        ["ORDER_ID", "STATUS"],
        [("ORDER_ID", "NUMBER(38,0)"), ("STATUS", "TEXT"), ("AMOUNT", "FLOAT")],
    )
    assert added == ["AMOUNT"]
    assert _alters(fake.log) == [
        'ALTER TABLE "DB"."S"."T" ADD COLUMN IF NOT EXISTS "AMOUNT" FLOAT'
    ]


def test_evolve_carries_the_inferred_type():
    fake, _ = _evolve(
        ["ID"], [("ID", "NUMBER(38,0)"), ("PRICE", "NUMBER(10,2)")], target='"T"'
    )
    assert _alters(fake.log) == [
        'ALTER TABLE "T" ADD COLUMN IF NOT EXISTS "PRICE" NUMBER(10,2)'
    ]


def test_evolve_adds_nothing_when_the_table_is_wider():
    fake, changed = _evolve(["ID", "STATUS", "EXTRA"], [("ID", "NUMBER(38,0)")])
    assert changed == []
    assert _alters(fake.log) == []


def test_evolve_ignores_a_case_difference():
    """A target created with quoted lower-case columns already matches the load
    (MATCH_BY_COLUMN_NAME is CASE_INSENSITIVE), so it must not gain a second
    upper-case column beside each one."""
    fake, changed = _evolve(
        ["order_id", "status"],
        [("ORDER_ID", "NUMBER(38,0)"), ("STATUS", "TEXT")],
    )
    assert changed == []
    assert _alters(fake.log) == []


def test_evolve_quotes_a_special_char_column():
    fake, _ = _evolve(["ID"], [("WEIRD NAME", "TEXT")], target='"T"')
    assert _alters(fake.log) == [
        'ALTER TABLE "T" ADD COLUMN IF NOT EXISTS "WEIRD NAME" TEXT'
    ]


def test_evolve_doubles_an_embedded_quote():
    fake, _ = _evolve(["ID"], [('OD"D', "TEXT")], target='"T"')
    assert _alters(fake.log) == [
        'ALTER TABLE "T" ADD COLUMN IF NOT EXISTS "OD""D" TEXT'
    ]


def test_evolve_keeps_the_batch_column_order():
    _, changed = _evolve(["B"], [("A", "TEXT"), ("B", "TEXT"), ("C", "INT")])
    assert changed == ["A", "C"]


def test_evolve_reads_the_table_before_altering_it():
    fake, _ = _evolve(["ID"], [("ID", "NUMBER(38,0)"), ("NEW_COL", "TEXT")])
    probe = next(i for i, s in enumerate(fake.log) if s.startswith("DESC TABLE"))
    alter = next(i for i, s in enumerate(fake.log) if s.startswith("ALTER TABLE"))
    assert probe < alter


def test_evolve_probes_the_table_with_desc():
    fake, _ = _evolve(["ID"], [("ID", "NUMBER(38,0)")])
    assert 'DESC TABLE "DB"."S"."T"' in fake.log


def test_evolve_widens_a_column_the_batch_has_outgrown():
    fake, changed = _evolve(
        [("NAME", "VARCHAR(10)")], [("NAME", "VARCHAR(20)")], target='"T"'
    )
    assert changed == ["NAME"]
    assert _alters(fake.log) == [
        'ALTER TABLE "T" ALTER COLUMN "NAME" SET DATA TYPE VARCHAR(20)'
    ]


def test_evolve_grows_a_number_precision_at_a_fixed_scale():
    fake, _ = _evolve(
        [("PRICE", "NUMBER(10,2)")], [("PRICE", "NUMBER(20,2)")], target='"T"'
    )
    assert _alters(fake.log) == [
        'ALTER TABLE "T" ALTER COLUMN "PRICE" SET DATA TYPE NUMBER(20,2)'
    ]


def test_evolve_leaves_a_synonym_of_the_same_type_alone():
    # The load-bearing case: DESC TABLE reports the column VARCHAR(16777216) and
    # infer_schema calls the same type TEXT. Aligned onto the target's own
    # spelling, the two are one type and no change is reported -- without which
    # every write on a table with an unparameterized text column would be
    # refused as an unsafe retype.
    fake, changed = _evolve([("NAME", "VARCHAR(16777216)")], [("NAME", "TEXT")])
    assert changed == []
    assert _alters(fake.log) == []


@pytest.mark.parametrize(
    "stored,inferred",
    [
        ("TIMESTAMP_NTZ(9)", "TIMESTAMP_NTZ(6)"),
        ("TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ(9)"),
        ("BINARY(10)", "BINARY(20)"),
    ],
)
def test_evolve_leaves_a_base_it_cannot_alter_alone(stored, inferred):
    # ALTER COLUMN reaches the parameters of NUMBER and VARCHAR alone, so a
    # timestamp whose precision the two sides report differently is a difference
    # with no statement behind it. The load casts into the column as it stands --
    # refusing the write would stop a load that works.
    fake, changed = _evolve([("SEEN_AT", stored)], [("SEEN_AT", inferred)])
    assert changed == []
    assert _alters(fake.log) == []


def test_evolve_still_adds_beside_a_base_it_cannot_alter():
    # And the rest of the batch is not held hostage to it: the new column lands.
    fake, changed = _evolve(
        [("ID", "NUMBER(38,0)"), ("SEEN_AT", "TIMESTAMP_NTZ(9)")],
        [
            ("ID", "NUMBER(38,0)"),
            ("SEEN_AT", "TIMESTAMP_NTZ(6)"),
            ("NOTE", "TEXT"),
        ],
        target='"T"',
    )
    assert changed == ["NOTE"]
    assert _alters(fake.log) == [
        'ALTER TABLE "T" ADD COLUMN IF NOT EXISTS "NOTE" TEXT'
    ]


@pytest.mark.parametrize(
    "stored,inferred",
    [
        ("NUMBER(38,0)", "TEXT"),
        ("NUMBER(38,0)", "FLOAT"),
        ("VARCHAR(20)", "VARCHAR(10)"),
        ("NUMBER(20,2)", "NUMBER(20,4)"),
        ("DATE", "TIMESTAMP_NTZ(9)"),
    ],
)
def test_evolve_refuses_a_change_snowflake_cannot_make(stored, inferred):
    # A narrowing, or a conversion between two bases: the batch's values have
    # nowhere to land, so the write stops here rather than at the MERGE.
    fake = column_fakes([("AMOUNT", stored)], [("AMOUNT", inferred)])
    with pytest.raises(_iceberg.UnsafeSchemaChangeError):
        _dialect(fake).evolve(_TARGET, _staged([("AMOUNT", inferred)]), "OP")
    assert _alters(fake.log) == []


def test_evolve_refuses_before_it_alters_anything():
    # The check covers the whole delta, so a column that could have been added
    # is not added when another column's retype is refused. Half a schema
    # change is worse than none.
    columns = [("AMOUNT", "FLOAT"), ("NOTE", "TEXT")]
    fake = column_fakes([("AMOUNT", "NUMBER(38,0)")], columns)
    with pytest.raises(_iceberg.UnsafeSchemaChangeError):
        _dialect(fake).evolve(_TARGET, _staged(columns), "OP")
    assert _alters(fake.log) == []


def test_evolve_adds_and_widens_in_one_pass():
    # ``compute_delta`` reports every addition before any retype, and the
    # returned names follow the statements that were run.
    fake, changed = _evolve(
        [("NAME", "VARCHAR(10)")],
        [("NAME", "VARCHAR(20)"), ("AMOUNT", "FLOAT")],
        target='"T"',
    )
    assert changed == ["AMOUNT", "NAME"]
    assert _alters(fake.log) == [
        'ALTER TABLE "T" ADD COLUMN IF NOT EXISTS "AMOUNT" FLOAT',
        'ALTER TABLE "T" ALTER COLUMN "NAME" SET DATA TYPE VARCHAR(20)',
    ]



def test_evolve_emits_no_ddl_beyond_the_alters():
    """A column the table has and the batch does not is a DROP_COLUMN change,
    and this connector does not drop -- the action is not one the renderer
    supports, so nothing reaches the warehouse for it."""
    fake, _ = _evolve(["ID", "GONE"], [("ID", "NUMBER(38,0)")])
    assert not any(
        s.startswith(("ALTER TABLE", "CREATE TABLE", "DROP")) for s in fake.log
    )


def test_evolve_keeps_the_marker_out_of_the_target():
    # The marker is excluded from the desired schema rather than filtered out of
    # the rendered statements, so the diff never reports an addition nobody
    # wants.
    fake, changed = _evolve(["ID"], [("ID", "NUMBER(38,0)"), ("OP", "TEXT")])
    assert changed == []
    assert _alters(fake.log) == []


# ---------------------------------------------------------------------------
# target_columns
# ---------------------------------------------------------------------------


def test_target_columns_reports_the_type_of_every_column():
    fake = column_fakes([("ID", "NUMBER(38,0)"), ("NAME", "VARCHAR(16777216)")], [])
    assert _dialect(fake).target_columns('"T"') == [
        ("ID", "NUMBER(38,0)"),
        ("NAME", "VARCHAR(16777216)"),
    ]


def test_target_columns_finds_the_fields_by_name_not_by_position():
    # DESC TABLE reports a dozen columns and Snowflake is free to add another;
    # a reader going by row[0] / row[1] would call the comment a name.
    fake = _FakeConnection({
        "DESC TABLE": (
            [("comment",), ("type",), ("name",)],
            [("seeded by ETL", "NUMBER(38,0)", "ID")],
        )
    })
    assert _dialect(fake).target_columns('"T"') == [("ID", "NUMBER(38,0)")]


# ---------------------------------------------------------------------------
# Write lists
#
# Naming the columns explicitly rather than using a wildcard is what keeps the
# marker out of the target, and it lifts the demand that both sides carry the
# same columns -- a batch wider or narrower than the target is no longer the
# statement's problem.
# ---------------------------------------------------------------------------


def _merge_sql(columns, **cfg_kwargs):
    dialect = _dialect(_FakeConnection())
    return render_merge(
        dialect, _TARGET, _staged([]), _cfg(**cfg_kwargs), list(columns)
    )


def test_merge_statement_takes_the_staging_table_whole():
    # No projection: every column the clauses name is verified to be there, and
    # one they do not name costs nothing.
    assert f'USING {_SCRATCH} AS source' in _merge_sql(["C", "STATUS"])


def test_merge_statement_inserts_every_written_column():
    sql = _merge_sql(["C", "STATUS"])
    assert (
        'THEN INSERT ("C", "STATUS") VALUES (source."C", source."STATUS")' in sql
    )


def test_merge_statement_inserts_columns_and_values_in_the_same_order():
    # Both lists come from one sequence, so a transposition here is impossible
    # -- which is the failure mode explicit lists introduce.
    columns, values = _insert_lists(_merge_sql(["B", "A"]))
    assert columns == ['"B"', '"A"']
    assert values == ['source."B"', 'source."A"']


def test_merge_statement_updates_every_written_column_but_the_keys():
    assignments = _assignments(_merge_sql(["C", "STATUS", "AMOUNT"]))
    assert assignments == (
        'target."STATUS" = source."STATUS", target."AMOUNT" = source."AMOUNT"'
    )
    assert '"C"' not in assignments


def test_merge_statement_omits_the_update_when_only_keys_are_written():
    # ``UPDATE SET`` with no assignments is a syntax error, so the clause has to
    # go rather than be emitted empty.
    sql = _merge_sql(["C"])
    assert "THEN UPDATE" not in sql
    assert "THEN DELETE" in sql
    assert 'THEN INSERT ("C") VALUES (source."C")' in sql


def test_merge_statement_quotes_every_written_column():
    sql = _merge_sql(["C", "weird name"])
    assert '"weird name"' in sql
    assert 'source."weird name"' in sql


def test_merge_statement_carries_no_evolution_modifier():
    """Snowflake evolves nothing from inside a MERGE -- the ALTERs have already
    run by the time the statement does."""
    assert _merge_sql(["C", "STATUS"]).startswith(f"MERGE INTO {_TARGET} AS target")


# ---------------------------------------------------------------------------
# write_columns
# ---------------------------------------------------------------------------


def _written(fake, file_columns, **cfg_kwargs):
    kwargs: dict = dict(on=["a"], op_column="op")
    kwargs.update(cfg_kwargs)
    return write_columns(
        _dialect(fake), '"T"', _staged(file_columns), MergeCfg(**kwargs)
    )


def test_written_columns_keeps_the_batch_order():
    columns = [("A", "TEXT"), ("B", "TEXT"), ("OP", "TEXT")]
    fake = column_fakes(["B", "A", "OP"], columns)
    assert _written(fake, columns) == ["A", "B"]


def test_written_columns_drops_what_the_target_lacks():
    columns = [("A", "TEXT"), ("B", "TEXT"), ("OP", "TEXT")]
    assert _written(column_fakes(["A"], columns), columns) == ["A"]


def test_written_columns_drops_what_the_batch_lacks():
    columns = [("A", "TEXT"), ("OP", "TEXT")]
    assert _written(column_fakes(["A", "B"], columns), columns) == ["A"]


def test_written_columns_drops_the_marker_even_when_the_target_has_it():
    # A target that picked the marker up from an earlier run keeps the column,
    # but the merge stops writing it.
    columns = [("A", "TEXT"), ("OP", "TEXT")]
    assert _written(column_fakes(["A", "OP"], columns), columns) == ["A"]


def test_written_columns_matches_case_insensitively():
    columns = [("ORDER_ID", "TEXT"), ("OP", "TEXT")]
    fake = column_fakes(["order_id", "op"], columns)
    assert _written(fake, columns, on=["order_id"]) == ["ORDER_ID"]


def test_written_columns_is_empty_when_nothing_lines_up():
    columns = [("B", "TEXT"), ("OP", "TEXT")]
    assert _written(column_fakes(["A"], columns), columns) == []


def test_written_columns_refuses_a_marker_the_batch_lacks():
    """The merge reads the marker off the staged rows, so a batch without it
    cannot say what to do with any row."""
    columns = [("A", "TEXT")]
    with pytest.raises(SnowflakeRuntimeException) as ei:
        _written(column_fakes(["A", "OP"], columns), columns)
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_18
    assert "'op'" in str(ei.value)


# ---------------------------------------------------------------------------
# op_column in the MERGE
#
# The marker gates all three clauses. DELETE is emitted first because Snowflake
# takes the first matching WHEN MATCHED, so a row marked 'd' is deleted rather
# than falling through to the update.
# ---------------------------------------------------------------------------


def test_merge_statement_gates_every_clause_on_the_marker():
    sql = _merge_sql(["C", "OP"], op_column="op")
    assert 'WHEN MATCHED AND (LOWER(source."OP") = \'d\') THEN DELETE' in sql
    assert 'WHEN MATCHED AND (LOWER(source."OP") = \'u\') THEN UPDATE SET' in sql
    assert 'WHEN NOT MATCHED AND (LOWER(source."OP") = \'i\') THEN INSERT (' in sql


def test_merge_statement_deletes_before_it_updates():
    sql = _merge_sql(["C", "OP"], op_column="op")
    assert sql.index("THEN DELETE") < sql.index("THEN UPDATE")


def test_merge_statement_compares_against_the_bare_marker():
    """The markers are interpolated into SQL, so they have to be the enum's
    *values*: formatting a str-mixin enum member yields
    ``'CdcOperation.DELETE'``, which is valid SQL that no row can ever match --
    every row would be silently passed over, and only a real Snowflake run would
    show it."""
    sql = _merge_sql(["C", "OP"], op_column="op")
    assert "CdcOperation" not in sql
    for operation in (
        CdcOperation.INSERT,
        CdcOperation.UPDATE,
        CdcOperation.DELETE,
    ):
        assert len(operation.value) == 1
        assert f"= '{operation.value}'" in sql


def test_merge_statement_quotes_a_case_sensitive_op_column():
    sql = _merge_sql(["C", "opCol"], op_column='"opCol"')
    assert 'LOWER(source."opCol") = \'u\'' in sql


@pytest.mark.parametrize("marker", ["i", "I", "u", "U", "d", "D"])
def test_merge_statement_lower_cases_the_marker_before_comparing(marker):
    """The comparison is against lower case, so a feed emitting either case is
    claimed by the same clause."""
    sql = _merge_sql(["C", "OP"], op_column="op")
    assert f"LOWER(source.\"OP\") = '{marker.lower()}'" in sql


def test_merge_statement_never_writes_the_marker():
    # The whole point of the explicit lists: the clauses read the marker off
    # ``source`` and no write list mentions it, so the target never gains it.
    sql = _merge_sql(["C", "STATUS"], op_column="op")
    columns, values = _insert_lists(sql)
    assert '"OP"' not in _assignments(sql)
    assert '"OP"' not in columns
    assert 'source."OP"' not in values


# ---------------------------------------------------------------------------
# ensure_target
#
# A merge's marker column has to be kept out of the target at creation as well
# as at write time: an ALTER can add a column later but nothing takes one back,
# so a first run that created the marker in would leave it there for good.
# ---------------------------------------------------------------------------


def test_ensure_target_excludes_the_marker():
    fake = _FakeConnection()
    columns = [("C", "NUMBER(38,0)"), ("OP", "TEXT")]
    _dialect(fake).ensure_target(_TARGET, _staged(columns), "OP")
    assert fake.log == [
        f'CREATE TABLE IF NOT EXISTS {_TARGET} AS'
        f' SELECT * EXCLUDE ("OP") FROM {_SCRATCH} LIMIT 0'
    ]


def test_ensure_target_folds_the_marker_onto_the_batch_spelling():
    fake = _FakeConnection()
    columns = [("C", "NUMBER(38,0)"), ("op", "TEXT")]
    _dialect(fake).ensure_target(_TARGET, _staged(columns), "OP")
    assert 'EXCLUDE ("op")' in fake.log[0]


def test_ensure_target_projects_everything_when_the_batch_has_no_marker():
    # write_columns is what refuses that batch; a create that has not reached it
    # yet must still be valid SQL rather than an empty EXCLUDE list.
    fake = _FakeConnection()
    _dialect(fake).ensure_target(_TARGET, _staged([("C", "NUMBER(38,0)")]), "OP")
    assert "SELECT * FROM" in fake.log[0]


# ---------------------------------------------------------------------------
# staging
# ---------------------------------------------------------------------------


def _staging_fake() -> _FakeConnection:
    return column_fakes([], [("C", "NUMBER(38,0)"), ("OP", "TEXT")])


def test_staging_puts_templates_and_copies_in_that_order(tmp_path):
    batch = tmp_path / "in.parquet"
    batch.write_bytes(b"")
    fake = _staging_fake()
    with _dialect(fake).staging(_TARGET, batch) as staged:
        assert staged.columns == [("C", "NUMBER(38,0)"), ("OP", "TEXT")]
    log = fake.log
    put = next(i for i, s in enumerate(log) if s.startswith("PUT file://"))
    infer = next(i for i, s in enumerate(log) if s.startswith(_INFER_PREFIX))
    create = next(
        i for i, s in enumerate(log) if s.startswith("CREATE TEMPORARY TABLE")
    )
    copy = next(i for i, s in enumerate(log) if s.startswith("COPY INTO"))
    assert put < infer < create < copy


def test_staging_reads_the_columns_before_the_copy_purges_the_file(tmp_path):
    # PURGE removes the staged file as soon as it has been ingested, so
    # infer_schema has to have run by then or there is nothing left to report.
    batch = tmp_path / "in.parquet"
    batch.write_bytes(b"")
    fake = _staging_fake()
    with _dialect(fake).staging(_TARGET, batch):
        pass
    infer = next(i for i, s in enumerate(fake.log) if s.startswith(_INFER_PREFIX))
    copy = next(i for i, s in enumerate(fake.log) if "PURGE = TRUE" in s)
    assert infer < copy


def test_staging_keeps_the_marker_in_the_scratch_table(tmp_path):
    """The scratch table is templated from the whole batch: the clauses read the
    marker off it."""
    batch = tmp_path / "in.parquet"
    batch.write_bytes(b"")
    fake = _staging_fake()
    with _dialect(fake).staging(_TARGET, batch):
        pass
    create = next(s for s in fake.log if s.startswith("CREATE TEMPORARY TABLE"))
    assert "EXCLUDE" not in create and "WHERE" not in create


def test_staging_names_the_scratch_table_beside_the_target(tmp_path):
    # A temporary table shadows a permanent one of the same name for the rest of
    # the session, so it must never be spelled exactly like the target.
    batch = tmp_path / "in.parquet"
    batch.write_bytes(b"")
    fake = _staging_fake()
    with _dialect(fake).staging(_TARGET, batch) as staged:
        assert staged.relation.startswith('"DB"."S"."T_')
        assert staged.relation != _TARGET


def test_staging_leaves_the_scratch_table_to_the_session(tmp_path):
    # A TEMPORARY table goes with the connection, so nothing drops it here --
    # and its unique suffix means the few a multi-table write leaves behind
    # collide with nothing.
    batch = tmp_path / "in.parquet"
    batch.write_bytes(b"")
    fake = _staging_fake()
    with _dialect(fake).staging(_TARGET, batch):
        pass
    assert not any(s.startswith("DROP") for s in fake.log)


# ---------------------------------------------------------------------------
# The watermark
#
# A merge is not naturally idempotent: rerunning one applies the same batch
# twice. Naming a watermark column makes it so -- every row written carries the
# trx's id, and a rerun that finds the target already stamped with it does
# nothing.
# ---------------------------------------------------------------------------

_TRX = "TRX_1"


def _watermark(column: str = "WM", transaction_id: str = _TRX) -> WatermarkCfg:
    return WatermarkCfg(column=column, transaction_id=transaction_id)


def _check(fake: _FakeConnection, target: str = _TARGET, **kwargs) -> bool:
    return _dialect(fake).check_watermark_column(target, _watermark(**kwargs))


def _selects(log: list[str]) -> list[str]:
    return [s for s in log if s.startswith("SELECT COUNT(")]


def test_watermark_reads_unstamped_when_the_target_has_no_such_column():
    """Which is also the answer for a target that does not exist yet: a table
    that is not there contributes no rows to INFORMATION_SCHEMA.COLUMNS, so one
    probe covers both."""
    fake = column_fakes([], [], watermark_answers(has_column=False))
    assert _check(fake) is False


def test_watermark_does_not_probe_a_target_that_lacks_the_column():
    """Selecting a column the table has not got is an error, not an answer, so
    the second probe has to be gated on the first."""
    fake = column_fakes([], [], watermark_answers(has_column=False))
    _check(fake)
    assert len(_selects(fake.log)) == 1
    assert not any(_TARGET in s for s in _selects(fake.log))


def test_watermark_reads_stamped_when_the_target_carries_the_id():
    fake = column_fakes([], [], watermark_answers(has_column=True, stamped=True))
    assert _check(fake) is True


def test_watermark_reads_unstamped_when_the_target_carries_another_id():
    fake = column_fakes([], [], watermark_answers(has_column=True, stamped=False))
    assert _check(fake) is False


def test_watermark_probe_is_qualified_with_the_targets_own_database():
    # INFORMATION_SCHEMA is per-database, so the session's would answer about
    # the wrong one for a target that names its own.
    fake = column_fakes([], [], watermark_answers(has_column=False))
    _check(fake)
    assert '"DB".INFORMATION_SCHEMA.COLUMNS' in _selects(fake.log)[0]


def test_watermark_probe_falls_back_to_the_session_for_a_bare_target():
    # An unqualified target resolves in the session's own database and schema,
    # which is where the probe has to look for it too.
    fake = column_fakes([], [], watermark_answers(has_column=False))
    _check(fake, target='"T"')
    probe = _selects(fake.log)[0]
    assert "INFORMATION_SCHEMA.COLUMNS" in probe and '"DB".' not in probe
    assert "COALESCE(%s, CURRENT_SCHEMA())" in probe


def test_watermark_probe_binds_the_names_it_matches_on():
    # The parts go in as values rather than as SQL, and unquoted: the connector
    # creates its targets per-part quoted, so what unfqn gives back is already
    # the spelling Snowflake stored.
    fake = column_fakes([], [], watermark_answers(has_column=False))
    _check(fake)
    _, params = fake.binds[0]
    assert params == ("S", "T", "WM")


def test_watermark_probe_binds_the_transaction_id():
    fake = column_fakes([], [], watermark_answers(has_column=True, stamped=True))
    _check(fake)
    statement, params = fake.binds[1]
    assert f'FROM {_TARGET} WHERE "WM" = %s' in statement
    assert params == (_TRX,)


def test_watermark_probe_quotes_a_special_char_column():
    fake = column_fakes([], [], watermark_answers(has_column=True, stamped=True))
    _check(fake, column="weird name")
    assert '"weird name" = %s' in fake.binds[1][0]


# --- Stamping the staged rows -----------------------------------------


def _stage_with(fake, tmp_path, watermark_cfg):
    batch = tmp_path / "in.parquet"
    batch.write_bytes(b"")
    with _dialect(fake).staging(_TARGET, batch, watermark_cfg) as staged:
        return staged


def test_staging_stamps_the_scratch_table_with_the_transaction_id(tmp_path):
    fake = _staging_fake()
    staged = _stage_with(fake, tmp_path, _watermark())
    (alter,) = _alters(fake.log)
    assert alter == (
        f"ALTER TABLE {staged.relation} ADD COLUMN IF NOT EXISTS"
        f" \"WM\" STRING DEFAULT '{_TRX}'"
    )


def test_staging_stamps_after_the_rows_are_in(tmp_path):
    # The batch does not carry the column, so a scratch table templated with it
    # would leave the COPY nothing to match it against. Adding it afterwards
    # with a constant default backfills the rows that were just loaded.
    fake = _staging_fake()
    _stage_with(fake, tmp_path, _watermark())
    copy = next(i for i, s in enumerate(fake.log) if s.startswith("COPY INTO"))
    alter = next(i for i, s in enumerate(fake.log) if s.startswith("ALTER TABLE"))
    assert copy < alter


def test_staging_reports_the_watermark_among_the_staged_columns(tmp_path):
    # Which is what carries it onward: ensure_target, evolve and the write list
    # all read the target's shape off the staged columns.
    staged = _stage_with(_staging_fake(), tmp_path, _watermark())
    assert staged.columns == [
        ("C", "NUMBER(38,0)"),
        ("OP", "TEXT"),
        ("WM", "STRING"),
    ]


def test_staging_stamps_nothing_without_a_watermark(tmp_path):
    fake = _staging_fake()
    staged = _stage_with(fake, tmp_path, None)
    assert _alters(fake.log) == []
    assert staged.columns == [("C", "NUMBER(38,0)"), ("OP", "TEXT")]


# --- Getting the column onto the target under strict ------------------


def _ensure(fake, watermark_cfg, target=_TARGET):
    return _dialect(fake).ensure_watermark_column(target, watermark_cfg)


def test_ensure_watermark_adds_the_column_a_strict_target_lacks():
    # Strict holds the schema still, and this is the one column it cannot hold
    # back: nothing else adds it, so the guard would read every run as the
    # first one.
    fake = column_fakes(["C"], [])
    assert _ensure(fake, _watermark()) == ["WM"]
    assert _alters(fake.log) == [
        f'ALTER TABLE {_TARGET} ADD COLUMN IF NOT EXISTS "WM" STRING'
    ]


def test_ensure_watermark_adds_it_without_a_default():
    """The rows already in the target belong to earlier transactions;
    backfilling this one's id would claim them for it and make the next rerun
    of it skip a batch it never applied."""
    fake = column_fakes(["C"], [])
    _ensure(fake, _watermark())
    assert "DEFAULT" not in _alters(fake.log)[0]


def test_ensure_watermark_leaves_a_target_that_already_has_it_alone():
    fake = column_fakes(["C", "WM"], [])
    assert _ensure(fake, _watermark()) == []
    assert _alters(fake.log) == []


def test_ensure_watermark_matches_the_existing_column_case_insensitively():
    fake = column_fakes(["C", "wm"], [])
    assert _ensure(fake, _watermark()) == []
    assert _alters(fake.log) == []


def test_ensure_watermark_does_nothing_without_a_watermark():
    fake = column_fakes(["C"], [])
    assert _ensure(fake, None) == []
    assert fake.log == []


def test_evolve_leaves_a_watermark_the_target_already_has_alone(tmp_path):
    """STRING and the VARCHAR(16777216) that DESC TABLE reports for it are one
    type, so a second run does not read the column as a retype of itself."""
    fake, changed = _evolve(
        [("C", "NUMBER(38,0)"), ("WM", "VARCHAR(16777216)")],
        [("C", "NUMBER(38,0)"), ("WM", "STRING")],
    )
    assert changed == []
    assert _alters(fake.log) == []


# ---------------------------------------------------------------------------
# Coalescing the batch before it is staged
#
# MERGE matches each incoming row against the target at most once, so a batch
# carrying several changes for one key has to be reduced first. That happens on
# the file, before the PUT.
# ---------------------------------------------------------------------------


def _batch(path: Path, **columns) -> Path:
    """A parquet holding whatever columns the test names."""
    import polars as pl

    pl.DataFrame(columns).write_parquet(path)
    return path


def _coalesce(batch: Path, cfg: MergeCfg):
    return coalesced_batch(_dialect(_FakeConnection()), "DB.S.T", batch, cfg)


def test_coalesce_normalizes_the_marker_case(tmp_path):
    import polars as pl

    batch = _batch(tmp_path / "in.parquet", c=[1, 1], op=["I", "U"], v=[10, 20])
    with _coalesce(batch, _cfg()) as out:
        assert pl.read_parquet(out).rows() == [(1, "i", 20)]


def test_coalesce_removes_its_temp_directory(tmp_path):
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"])
    with _coalesce(batch, _cfg()) as out:
        assert out.exists()
    assert not out.exists()
    assert not out.parent.exists()


def test_coalesce_leaves_the_original_alone(tmp_path):
    import polars as pl

    batch = _batch(tmp_path / "in.parquet", c=[1, 1], op=["i", "u"], v=[10, 20])
    with _coalesce(batch, _cfg()):
        pass
    assert pl.read_parquet(batch).height == 2


def test_coalesce_keeps_the_original_basename(tmp_path):
    """Everything downstream keys off the staged basename, so the reduced file
    has to carry the original's."""
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"])
    with _coalesce(batch, _cfg()) as out:
        assert out.name == "in.parquet"
        assert out != batch


def test_coalesce_drops_an_ephemeral_key(tmp_path):
    """A key inserted and deleted inside one batch never reached the target, so
    it reaches neither the staged file nor the merge."""
    import polars as pl

    batch = _batch(tmp_path / "in.parquet", c=[1, 1], op=["i", "d"], v=[10, 20])
    with _coalesce(batch, _cfg()) as out:
        reduced = pl.read_parquet(out)
    assert reduced.is_empty()
    assert reduced.columns == ["c", "op", "v"]


def test_coalesce_folds_a_lower_case_batch(tmp_path):
    """End to end through the resolution: an upper-folded cfg against a
    lower-case frame."""
    import polars as pl

    batch = _batch(
        tmp_path / "in.parquet", order_id=[7, 7], op=["i", "u"], amount=[1, 2]
    )
    with _coalesce(batch, _cfg(on=["order_id"])) as out:
        assert pl.read_parquet(out).rows() == [(7, "i", 2)]


def test_coalesce_rejects_a_missing_marker(tmp_path):
    batch = _batch(tmp_path / "in.parquet", c=[1], v=[10])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        with _coalesce(batch, _cfg()):
            pass
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_18
    assert "'op'" in str(ei.value)


def test_coalesce_rejects_a_missing_key(tmp_path):
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        with _coalesce(batch, _cfg(on=["nope"])):
            pass
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_19
    assert "'nope'" in str(ei.value)


def test_coalesce_names_every_missing_key(tmp_path):
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        with _coalesce(batch, _cfg(on=["c", "gone", "also"])):
            pass
    message = str(ei.value)
    assert "'gone'" in message and "'also'" in message and "'c'" not in message


# --- Resolving the merge columns onto the incoming ones ---------------


def test_incoming_column_matches_a_lower_case_column_by_an_upper_folded_key():
    """``on=["order_id"]`` folds to ``ORDER_ID`` for Snowflake, but the frame
    spells it lower case -- and polars is case-sensitive."""
    dialect = _dialect(_FakeConnection())
    assert (
        incoming_column(dialect, ["order_id", "op"], "ORDER_ID", "DB.S.T")
        == "order_id"
    )


def test_incoming_column_matches_a_quoted_mixed_case_column():
    dialect = _dialect(_FakeConnection())
    assert incoming_column(dialect, ["myCol", "op"], "myCol", "DB.S.T") == "myCol"


def test_incoming_column_is_none_when_absent():
    dialect = _dialect(_FakeConnection())
    assert incoming_column(dialect, ["a", "b"], "C", "DB.S.T") is None


def test_incoming_column_rejects_an_ambiguous_match():
    """Snowflake loads columns case-insensitively, so two columns differing only
    in case cannot be told apart -- picking one would merge on the wrong data."""
    dialect = _dialect(_FakeConnection())
    with pytest.raises(SnowflakeRuntimeException) as ei:
        incoming_column(dialect, ["op", "OP"], "OP", "DB.S.T")
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_20
    assert "'op'" in str(ei.value) and "'OP'" in str(ei.value)


# ---------------------------------------------------------------------------
# The driver, end to end over this dialect
# ---------------------------------------------------------------------------


def test_merge_batch_refuses_a_target_sharing_no_column(tmp_path):
    """Nothing to write means the merge would be a no-op, and the likely cause
    -- a frame aimed at the wrong table -- deserves saying."""
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"])
    fake = column_fakes(["OTHER"], [("C", "NUMBER(38,0)"), ("OP", "TEXT")])
    with pytest.raises(SnowflakeRuntimeException) as ei:
        merge_batch(
            _dialect(fake),
            target=_TARGET,
            batch=batch,
            cfg=_cfg(),
            schema_evolution="strict",
            ctx=_FakeCtx(),
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_16
    assert _TARGET in str(ei.value)
    assert not any(s.startswith("MERGE") for s in fake.log)


def test_merge_batch_evolves_only_on_update(tmp_path):
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"], v=[10])
    columns = [("C", "NUMBER(38,0)"), ("OP", "TEXT"), ("V", "NUMBER(38,0)")]
    for evolution, expected in (("strict", []), ("update", ["V"])):
        fake = column_fakes(["C"], columns)
        result = merge_batch(
            _dialect(fake),
            target=_TARGET,
            batch=batch,
            cfg=_cfg(),
            schema_evolution=evolution,
            ctx=_FakeCtx(),
        )
        assert result.evolved == expected


def _merge(fake, batch, schema_evolution="update", watermark_column=None, **cfg_kwargs):
    """Drive the shared driver over ``fake``.

    ``watermark_column`` arrives at the driver already folded -- the destination
    that names it does the folding -- so it is spelled here the way the target
    stores it.
    """
    return merge_batch(
        _dialect(fake),
        target=_TARGET,
        batch=batch,
        cfg=_cfg(**cfg_kwargs),
        schema_evolution=schema_evolution,
        ctx=_FakeCtx(trx_id=_TRX),
        watermark_column=watermark_column,
    )


def _watermark_fake(answers, table_columns=("C",)):
    columns = [("C", "NUMBER(38,0)"), ("OP", "TEXT")]
    return column_fakes(list(table_columns), columns, answers)


def test_merge_batch_leaves_a_target_already_stamped_alone(tmp_path):
    """The whole point of the watermark: a trx that already wrote these rows is
    not to write them again."""
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"])
    fake = _watermark_fake(watermark_answers(has_column=True, stamped=True))
    result = _merge(fake, batch, watermark_column="WM")
    assert result == MergeResult(evolved=[], written=[])
    assert not any(s.startswith(("PUT", "MERGE", "CREATE")) for s in fake.log)


def test_merge_batch_checks_the_watermark_before_it_stages_anything(tmp_path):
    # Staging is the expensive half, so the answer has to come first -- and the
    # scratch table it would leave behind is one nobody asked for.
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"])
    fake = _watermark_fake(watermark_answers(has_column=False))
    _merge(fake, batch, watermark_column="WM")
    probe = next(i for i, s in enumerate(fake.log) if s.startswith("SELECT COUNT("))
    put = next(i for i, s in enumerate(fake.log) if s.startswith("PUT file://"))
    assert probe < put


def test_merge_batch_writes_the_watermark_onto_the_target(tmp_path):
    # The stamp only makes the next run idempotent if it reaches the target, so
    # the column is evolved onto it and named in the write list like any other.
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"])
    fake = _watermark_fake(watermark_answers(has_column=False))
    result = _merge(fake, batch, watermark_column="WM")
    assert result.evolved == ["WM"]
    assert result.written == ["C", "WM"]
    assert 'ALTER TABLE "DB"."S"."T" ADD COLUMN IF NOT EXISTS "WM" STRING' in fake.log
    assert 'source."WM"' in next(s for s in fake.log if s.startswith("MERGE"))


def test_merge_batch_probes_no_watermark_when_the_cfg_names_none(tmp_path):
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"])
    fake = _watermark_fake(watermark_answers(has_column=True, stamped=True))
    result = _merge(fake, batch)
    assert not any(s.startswith("SELECT COUNT(") for s in fake.log)
    assert result.written == ["C"]


def test_merge_batch_writes_the_watermark_on_strict_too(tmp_path):
    # Strict adds nothing else, but a watermark the target cannot hold is no
    # watermark: the column is put there so the next run of this trx has a stamp
    # to find.
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"])
    fake = _watermark_fake(watermark_answers(has_column=False))
    result = _merge(fake, batch, schema_evolution="strict", watermark_column="WM")
    assert result.evolved == ["WM"]
    assert result.written == ["C", "WM"]
    assert 'ALTER TABLE "DB"."S"."T" ADD COLUMN IF NOT EXISTS "WM" STRING' in fake.log


def test_merge_batch_adds_nothing_else_on_strict(tmp_path):
    # The rest of the widened batch is still held back -- the exception is the
    # watermark alone, not evolution creeping into strict.
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"], v=[10])
    columns = [("C", "NUMBER(38,0)"), ("OP", "TEXT"), ("V", "NUMBER(38,0)")]
    fake = column_fakes(["C"], columns, watermark_answers(has_column=False))
    result = _merge(fake, batch, schema_evolution="strict", watermark_column="WM")
    assert result.evolved == ["WM"]
    assert "V" not in result.written


def test_merge_batch_adds_no_watermark_on_strict_without_one(tmp_path):
    batch = _batch(tmp_path / "in.parquet", c=[1], op=["i"])
    fake = _watermark_fake(watermark_answers(has_column=False))
    result = _merge(fake, batch, schema_evolution="strict")
    assert result.evolved == []
    assert _alters(fake.log) == []
