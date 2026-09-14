#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery's merge dialect, and the shared driver run against it.

What BigQuery contributes to the merge: the statement it renders, how it stages a
batch through GCS and a load job, and how it brings a target into being and
widens it. The shared pieces are exercised through this dialect rather than a
stub, so the wiring between the two is tested and not assumed.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import MergeCfg, _iceberg, _impl as impl

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery._merge_dialect import BigQueryMergeDialect
from tabsdatak.conn.bigquery.error import BigQueryErrorCode, BigQueryRuntimeException

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._batch import coalesced_batch

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._driver import write_columns

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._statement import render_merge

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge.dialect import Staged

pytestmark = pytest.mark.bigquery


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    work = tmp_path / "work"
    work.mkdir()
    return work

_TARGET = "my-project.analytics.orders"
_STAGING = "my-project.analytics.orders_a1b2c3d4"
_QUOTED_TARGET = "`my-project.analytics.orders`"
_QUOTED_STAGING = "`my-project.analytics.orders_a1b2c3d4`"


def _merge_cfg(**overrides) -> MergeCfg:
    kwargs: dict = dict(on=["order_id"], op_column="op")
    kwargs.update(overrides)
    return MergeCfg(**kwargs)


def _field(name, type_, mode="NULLABLE", **kwargs):
    from google.cloud import bigquery

    return bigquery.SchemaField(name, type_, mode=mode, **kwargs)


def _field_client(schemas: dict[str, list]) -> MagicMock:
    """A ``bq_client`` stub whose ``get_table`` answers with ``SchemaField``s.

    A table the caller did not describe does not exist, and says so the way the
    SDK does.
    """
    from google.cloud.exceptions import NotFound

    def get_table(table_fqn: str):
        if table_fqn not in schemas:
            raise NotFound(f"{table_fqn} not found")
        return SimpleNamespace(schema=list(schemas[table_fqn]))

    return MagicMock(get_table=MagicMock(side_effect=get_table))


def _named_client(schemas: dict[str, list[str]]) -> MagicMock:
    """A ``bq_client`` stub described by column names alone, for what only reads
    them."""
    return _field_client({
        table: [_field(name, "STRING") for name in names]
        for table, names in schemas.items()
    })


def _dialect(client, *, uri: str = "gs://bucket/in.parquet") -> BigQueryMergeDialect:
    return BigQueryMergeDialect(
        client, MagicMock(), uri=uri, target_fqn=_TARGET
    )


def _staged(fields: list, relation: str = _QUOTED_STAGING) -> Staged:
    """The ``Staged`` the driver would hand the dialect for these staged
    fields."""
    return Staged(
        relation=relation,
        columns=[(field.name, impl.column_type(field)) for field in fields],
    )


def _staged_names(names: list[str]) -> Staged:
    return _staged([_field(name, "STRING") for name in names])


def _statements(client) -> list[str]:
    return [call.args[0] for call in client.query_and_wait.call_args_list]


# ---------------------------------------------------------------------------
# The MERGE statement
# ---------------------------------------------------------------------------


def _merge_sql(columns: list[str], **cfg) -> str:
    return render_merge(
        _dialect(MagicMock()),
        _QUOTED_TARGET,
        _staged([]),
        _merge_cfg(**cfg),
        columns,
    )


def test_merge_statement_emits_the_whole_merge():
    assert _merge_sql(["order_id", "total"]) == (
        "MERGE INTO `my-project.analytics.orders` AS target"
        " USING `my-project.analytics.orders_a1b2c3d4` AS source"
        " ON target.`order_id` = source.`order_id`"
        " WHEN MATCHED AND (LOWER(source.`op`) = 'd') THEN DELETE"
        " WHEN MATCHED AND (LOWER(source.`op`) = 'u') THEN"
        " UPDATE SET `total` = source.`total`"
        " WHEN NOT MATCHED BY TARGET AND (LOWER(source.`op`) = 'i') THEN"
        " INSERT (`order_id`, `total`) VALUES (source.`order_id`, source.`total`)"
    )


def test_merge_statement_deletes_before_it_updates():
    """BigQuery takes the first matching WHEN MATCHED, so a row marked for
    deletion must not fall through to the update."""
    stmt = _merge_sql(["order_id", "total"])
    assert stmt.index("THEN DELETE") < stmt.index("THEN UPDATE SET")


def test_merge_statement_never_writes_the_marker_column():
    """The clauses read the marker off ``source``; no write list names it."""
    stmt = _merge_sql(["order_id", "total"])
    assignments = stmt[stmt.index("UPDATE SET") : stmt.index("WHEN NOT MATCHED")]
    inserted = stmt[stmt.index("INSERT (") :]
    assert "`op`" not in assignments
    assert "`op`" not in inserted


def test_merge_statement_assigns_a_bare_column_name():
    """An update item can only resolve against the target, and BigQuery reads a
    qualified name there as an error rather than as noise."""
    stmt = _merge_sql(["order_id", "total"])
    assignments = stmt[stmt.index("UPDATE SET") : stmt.index("WHEN NOT MATCHED")]
    assert "`total` = source.`total`" in assignments
    assert "target.`total` =" not in assignments


def test_merge_statement_does_not_reassign_the_keys():
    """The keys are what the row matched on, so an update leaves them alone."""
    stmt = _merge_sql(["order_id", "line", "q"], on=["order_id", "line"])
    assignments = stmt[stmt.index("UPDATE SET") : stmt.index("WHEN NOT MATCHED")]
    assert assignments.strip() == "UPDATE SET `q` = source.`q`"


def test_merge_statement_does_not_reassign_a_key_the_batch_spells_differently():
    """The write list carries the staging table's spelling and the keys their
    folded one, so the two are compared folded or the key is reassigned."""
    stmt = _merge_sql(["Order_Id", "Total"])
    assignments = stmt[stmt.index("UPDATE SET") : stmt.index("WHEN NOT MATCHED")]
    assert assignments.strip() == "UPDATE SET `Total` = source.`Total`"


def test_merge_statement_omits_the_update_clause_when_only_keys_are_shared():
    """Nothing to assign, and ``UPDATE SET`` with an empty list is a syntax
    error -- so a 'u' row simply matches no clause."""
    stmt = _merge_sql(["order_id"])
    assert "THEN UPDATE SET" not in stmt
    assert "THEN DELETE" in stmt
    assert "THEN INSERT" in stmt


def test_merge_statement_folds_key_and_marker_names():
    """BigQuery resolves a column name case-insensitively, so the folded
    spelling reaches the column whatever case the config used."""
    stmt = _merge_sql(["orderid", "t"], on=["`OrderId`"], op_column="OP")
    assert "target.`orderid` = source.`orderid`" in stmt
    assert "LOWER(source.`op`)" in stmt


def test_merge_statement_inserts_and_values_stay_in_step():
    stmt = _merge_sql(["order_id", "total", "note"])
    assert (
        "INSERT (`order_id`, `total`, `note`)"
        " VALUES (source.`order_id`, source.`total`, source.`note`)" in stmt
    )


def test_merge_statement_quotes_through_the_iceberg_dialect():
    """One quoter for every identifier the connector emits."""
    assert _dialect(MagicMock()).quote_column("weird col") == _iceberg.quote_column(
        "weird col"
    )


def test_execute_runs_the_statement_through_query_and_wait():
    client = MagicMock()
    _dialect(client).execute("MERGE INTO x")
    client.query_and_wait.assert_called_once_with("MERGE INTO x")


# ---------------------------------------------------------------------------
# The write list
# ---------------------------------------------------------------------------


def _written(staged: list[str], target: list[str], **cfg) -> list[str]:
    client = _named_client({_TARGET: target})
    return write_columns(
        _dialect(client), _QUOTED_TARGET, _staged_names(staged), _merge_cfg(**cfg)
    )


def test_written_columns_is_the_shared_columns_without_the_marker():
    # Staging order, target membership, marker out, source-only column out.
    assert _written(
        ["order_id", "total", "op", "extra"],
        ["order_id", "total", "untouched"],
    ) == ["order_id", "total"]


def test_written_columns_matches_names_case_insensitively():
    # The staging table's spelling is what the statement uses.
    assert _written(["Order_Id", "Total", "OP"], ["order_id", "total"]) == [
        "Order_Id",
        "Total",
    ]


def test_written_columns_rejects_a_batch_without_the_marker():
    with pytest.raises(BigQueryRuntimeException) as ei:
        _written(["order_id", "total"], ["order_id", "total"])
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_15


def test_written_columns_rejects_a_key_the_target_does_not_have():
    with pytest.raises(BigQueryRuntimeException) as ei:
        _written(["order_id", "total", "op"], ["total"])
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_17
    assert "order_id" in str(ei.value)


# ---------------------------------------------------------------------------
# Creating the target
# ---------------------------------------------------------------------------


def _ensure_target(staging: list, target: list | None = None, marker: str = "op"):
    schemas = {} if target is None else {_TARGET: target}
    client = _field_client(schemas)
    _dialect(client).ensure_target(_QUOTED_TARGET, _staged(staging), marker)
    return client


def test_ensure_target_builds_the_target_from_the_staging_table():
    client = _ensure_target([
        _field("order_id", "INTEGER"),
        _field("status", "STRING"),
        _field("tags", "STRING", mode="REPEATED"),
    ])
    (statement,) = _statements(client)
    assert statement == (
        "CREATE TABLE IF NOT EXISTS `my-project.analytics.orders` (\n"
        "    `order_id` INT64,\n"
        "    `status` STRING,\n"
        "    `tags` ARRAY<STRING>\n"
        ")"
    )


def test_ensure_target_leaves_the_marker_out_of_the_target():
    client = _ensure_target(
        [_field("order_id", "INTEGER"), _field("op", "STRING")]
    )
    (statement,) = _statements(client)
    assert "`op`" not in statement
    assert "`order_id` INT64" in statement


def test_ensure_target_leaves_an_existing_target_alone():
    """Widening an existing target is ``evolve``'s job; this one only bootstraps
    a missing one."""
    client = _ensure_target(
        [_field("order_id", "INTEGER"), _field("note", "STRING")],
        target=[_field("order_id", "INTEGER")],
    )
    client.query_and_wait.assert_not_called()


def test_ensure_target_renders_an_idempotent_create():
    """A second run that read the same missing target creates nothing rather
    than failing."""
    client = _ensure_target([_field("order_id", "INTEGER")])
    assert _statements(client)[0].startswith("CREATE TABLE IF NOT EXISTS")


def test_ensure_target_does_nothing_when_only_the_marker_was_staged():
    """A CREATE with no columns is not a statement; the merge is what reports
    a batch that carries nothing to write."""
    client = _ensure_target([_field("op", "STRING")])
    client.query_and_wait.assert_not_called()


# ---------------------------------------------------------------------------
# Evolving the target
# ---------------------------------------------------------------------------


def _evolve(target: list, staging: list, marker: str = "op"):
    client = _field_client({_TARGET: target})
    evolved = _dialect(client).evolve(_QUOTED_TARGET, _staged(staging), marker)
    return client, evolved


def test_evolve_alters_the_target_for_each_new_column():
    client, evolved = _evolve(
        [_field("order_id", "INTEGER")],
        [
            _field("order_id", "INTEGER"),
            _field("note", "STRING"),
            _field("tags", "STRING", mode="REPEATED"),
        ],
    )
    assert evolved == ["note", "tags"]
    assert _statements(client) == [
        "ALTER TABLE `my-project.analytics.orders`"
        " ADD COLUMN IF NOT EXISTS `note` STRING",
        "ALTER TABLE `my-project.analytics.orders`"
        " ADD COLUMN IF NOT EXISTS `tags` ARRAY<STRING>",
    ]


def test_evolve_excludes_the_marker_column():
    """The marker says what to do with a row; the target must never gain it."""
    client, evolved = _evolve(
        [_field("order_id", "INTEGER")],
        [_field("order_id", "INTEGER"), _field("op", "STRING")],
    )
    assert evolved == []
    assert _statements(client) == []


def test_evolve_excludes_the_marker_case_insensitively():
    _, evolved = _evolve(
        [_field("order_id", "INTEGER")],
        [_field("order_id", "INTEGER"), _field("OP", "STRING")],
    )
    assert evolved == []


def test_evolve_treats_a_case_difference_as_the_same_column():
    """BigQuery refuses a table two columns differing only in case, so a staged
    ``Order_Id`` must not be added beside the target's ``order_id``."""
    client, evolved = _evolve(
        [_field("order_id", "INTEGER")], [_field("Order_Id", "INTEGER")]
    )
    assert evolved == []
    assert _statements(client) == []


def test_evolve_keeps_the_staging_spelling_of_a_new_column():
    client, evolved = _evolve(
        [_field("order_id", "INTEGER")], [_field("Note", "STRING")]
    )
    assert evolved == ["Note"]
    assert "`Note` STRING" in _statements(client)[0]


def test_evolve_never_drops_or_retypes():
    """Schema drift stays as harmless as it has always been here: a column only
    the target has, or one whose type changed, is reported and ignored."""
    client, evolved = _evolve(
        [_field("order_id", "INTEGER"), _field("legacy", "STRING")],
        [_field("order_id", "STRING")],
    )
    assert evolved == []
    assert _statements(client) == []


def test_evolve_alters_rather_than_creating_an_empty_target():
    """``is_create`` is pinned to False, so a target reporting no columns still
    takes the ALTER path instead of forking the load job's create."""
    client, evolved = _evolve([], [_field("note", "STRING")])
    assert evolved == ["note"]
    assert "ADD COLUMN IF NOT EXISTS `note` STRING" in _statements(client)[0]


# ---------------------------------------------------------------------------
# Staging through GCS and a load job
# ---------------------------------------------------------------------------


def test_staging_loads_the_batch_into_a_table_of_its_own(work_dir, monkeypatch):
    """A MERGE cannot read a parquet object, so the batch lands in a staging
    table and the statement takes that as its source."""
    loaded: dict = {}

    def fake_load(bq_client, *, uri, table, job_config):
        loaded.update(uri=uri, table=table, job_config=job_config)

    # noinspection PyProtectedMember
    from tabsdatak.conn.bigquery import _plugin

    monkeypatch.setattr(_plugin, "_load_table_from_uri", fake_load)
    monkeypatch.setattr(_plugin, "_upload_to_gcs", lambda *a: None)

    batch = work_dir / "in.parquet"
    pl.DataFrame({"order_id": [1], "op": ["i"]}).write_parquet(batch)
    client = _field_client({
        _STAGING: [_field("order_id", "INTEGER"), _field("op", "STRING")]
    })
    monkeypatch.setattr(impl, "temp_table_fqn", lambda table: _STAGING)

    with _dialect(client).staging(_QUOTED_TARGET, batch) as staged:
        assert staged.relation == _QUOTED_STAGING
        assert staged.columns == [("order_id", "INT64"), ("op", "STRING")]
    assert loaded["uri"] == "gs://bucket/in.parquet"
    assert loaded["table"] == _STAGING
    client.delete_table.assert_called_once_with(_STAGING, not_found_ok=True)


def test_staging_drops_the_table_when_the_merge_fails(work_dir, monkeypatch):
    # noinspection PyProtectedMember
    from tabsdatak.conn.bigquery import _plugin

    monkeypatch.setattr(_plugin, "_load_table_from_uri", lambda *a, **k: None)
    monkeypatch.setattr(_plugin, "_upload_to_gcs", lambda *a: None)
    monkeypatch.setattr(impl, "temp_table_fqn", lambda table: _STAGING)

    batch = work_dir / "in.parquet"
    pl.DataFrame({"order_id": [1], "op": ["i"]}).write_parquet(batch)
    client = _field_client({_STAGING: [_field("order_id", "INTEGER")]})
    with pytest.raises(RuntimeError):
        with _dialect(client).staging(_QUOTED_TARGET, batch):
            raise RuntimeError("merge blew up")
    client.delete_table.assert_called_once_with(_STAGING, not_found_ok=True)


# ---------------------------------------------------------------------------
# Coalescing the batch
# ---------------------------------------------------------------------------


def _coalesced_rows(work_dir: Path, rows: list[dict], **cfg) -> list[dict]:
    src = work_dir / "cdc.parquet"
    pl.DataFrame(rows).write_parquet(src)
    with coalesced_batch(
        _dialect(MagicMock()), _TARGET, src, _merge_cfg(**cfg)
    ) as reduced:
        return pl.read_parquet(reduced).to_dicts()


def test_coalesce_reduces_a_key_to_its_net_state(work_dir):
    """MERGE matches each incoming row at most once, so two changes for one key
    have to become one."""
    rows = _coalesced_rows(
        work_dir,
        [
            {"order_id": 1, "total": 10, "op": "i"},
            {"order_id": 1, "total": 20, "op": "u"},
        ],
    )
    assert rows == [{"order_id": 1, "total": 20, "op": "i"}]


def test_coalesce_drops_an_ephemeral_key(work_dir):
    rows = _coalesced_rows(
        work_dir,
        [
            {"order_id": 1, "total": 10, "op": "i"},
            {"order_id": 1, "total": 10, "op": "d"},
        ],
    )
    assert rows == []


def test_coalesce_keeps_the_original_basename(work_dir):
    src = work_dir / "cdc.parquet"
    pl.DataFrame([{"order_id": 1, "op": "i"}]).write_parquet(src)
    with coalesced_batch(
        _dialect(MagicMock()), _TARGET, src, _merge_cfg()
    ) as reduced:
        assert reduced.name == "cdc.parquet"
        assert reduced != src


def test_coalesce_matches_the_marker_case_insensitively(work_dir):
    rows = _coalesced_rows(
        work_dir,
        [
            {"order_id": 1, "total": 10, "OP": "i"},
            {"order_id": 1, "total": 20, "OP": "u"},
        ],
    )
    assert rows == [{"order_id": 1, "total": 20, "OP": "i"}]


def test_coalesce_rejects_a_batch_without_the_marker(work_dir):
    with pytest.raises(BigQueryRuntimeException) as ei:
        _coalesced_rows(work_dir, [{"order_id": 1}])
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_15


def test_coalesce_rejects_a_batch_missing_a_key(work_dir):
    with pytest.raises(BigQueryRuntimeException) as ei:
        _coalesced_rows(work_dir, [{"total": 1, "op": "i"}])
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_16


def test_coalesce_rejects_columns_that_differ_only_in_case(work_dir):
    """A parquet can carry ``op`` and ``OP``; BigQuery would resolve them to one
    column, so which one the merge reads is not a decision to make silently."""
    with pytest.raises(BigQueryRuntimeException) as ei:
        _coalesced_rows(work_dir, [{"order_id": 1, "op": "i", "OP": "u"}])
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_19
