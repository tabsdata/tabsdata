#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.bigquery import BigQueryDestConn, BigQueryDest, MergeCfg

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import _iceberg as iceberg

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import _impl as impl

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import _plugin as plugin
from tabsdatak.conn.bigquery.error import (
    BigQueryRuntimeException,
    BigQueryErrorCode,
    BigQueryValidateException,
)
from tabsdatak.conn.bigquery._explorer import BigQueryConnectionExplorer
from tabsdatak.conn.bigquery import GCSLocation
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import GCPJson

pytestmark = pytest.mark.bigquery


# ---------------------------------------------------------------------------
# Test context shim for DestContext
# ---------------------------------------------------------------------------


@dataclass
class _Ctx:
    _work: Path
    trx_id: str = "TRX_1"

    def work_dir(self) -> Path:
        return self._work


def _dest_ctx(work: Path, trx_id: str = "TRX_1") -> _Ctx:
    return _Ctx(work, trx_id)


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


# ---------------------------------------------------------------------------
# Minimal connection / dest builders. Credentials are literal secrets so
# resolution doesn't need a backing secret store; the underlying GCP / BQ
# clients are always mocked in these tests.
# ---------------------------------------------------------------------------


def _gcp_key() -> str:
    # A minimal JSON shape that the SDK won't see -- we mock the
    # ``Client.from_service_account_info`` call -- but the legacy
    # impl still calls ``json.loads`` on the secret payload, so it
    # must be valid JSON.
    return '{"type": "service_account", "project_id": "p"}'


def _staging() -> GCSLocation:
    return GCSLocation(
        bucket="staging-bucket",
        credentials=GCPJson(json=Secret(_gcp_key())),
        base_path="/staging",
    )


# ---------------------------------------------------------------------------
# schema_evolution_engine
#
# BigQuery's own evolution only ever adds a column or relaxes one to nullable,
# so a bulk load cannot widen one however the batch has grown. Diffing the
# target against the batch reaches what the load-job options cannot -- and
# refuses what BigQuery could not do at all.
# ---------------------------------------------------------------------------


def _schema_field(name, type_, mode="NULLABLE"):
    from google.cloud import bigquery

    return bigquery.SchemaField(name, type_, mode=mode)


def _evolving_clients(
    monkeypatch, target_columns, batch_columns, target_exists=True
):
    """A bq client whose target has ``target_columns`` and whose staged batch
    loads to a table with ``batch_columns``, each a ``(name, legacy_type)``."""
    from google.cloud.exceptions import NotFound

    target = "my-project.analytics.users"

    def get_table(fqn):
        if fqn.startswith(f"{target}_"):  # the staging table the batch loads to
            return SimpleNamespace(
                schema=[_schema_field(n, t) for n, t in batch_columns]
            )
        if fqn == target and target_exists:
            return SimpleNamespace(
                schema=[_schema_field(n, t) for n, t in target_columns]
            )
        raise NotFound(f"{fqn} not found")

    bq_client = MagicMock(name="bq", get_table=MagicMock(side_effect=get_table))
    bq_client.load_table_from_uri.return_value = MagicMock(
        result=MagicMock(return_value=None)
    )
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)

    gcs_client = MagicMock(name="gcs")
    gcs_client.bucket.return_value.blob.return_value = MagicMock()
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)
    return bq_client


def _write(work_dir, dest_cfg=None, schema_evolution="update"):
    dest = BigQueryDest(
        tables=["`my-project.analytics.users`"],
        if_table_exists="append",
        schema_evolution=schema_evolution,
        dest_cfg=dest_cfg,
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    plugin._BigQueryDestPlugin().write_out(_dest_ctx(work_dir), _bare_conn(), dest, [src])


def _alters(bq_client):
    return [c.args[0] for c in bq_client.query_and_wait.call_args_list]


_ICEBERG_ENGINE = {"bigquery.schema_evolution_engine": "iceberg"}


def test_dest_plugin_iceberg_engine_widens_a_column(monkeypatch, work_dir):
    bq_client = _evolving_clients(
        monkeypatch, [("id", "INTEGER")], [("id", "NUMERIC")]
    )
    _write(work_dir, _ICEBERG_ENGINE)
    assert _alters(bq_client) == [
        "ALTER TABLE `my-project.analytics.users`"
        " ALTER COLUMN IF EXISTS `id` SET DATA TYPE NUMERIC"
    ]


def test_dest_plugin_iceberg_engine_adds_a_column_the_target_lacks(
    monkeypatch, work_dir
):
    bq_client = _evolving_clients(
        monkeypatch, [("id", "INTEGER")], [("id", "INTEGER"), ("note", "STRING")]
    )
    _write(work_dir, _ICEBERG_ENGINE)
    assert _alters(bq_client) == [
        "ALTER TABLE `my-project.analytics.users`"
        " ADD COLUMN IF NOT EXISTS `note` STRING"
    ]


def test_dest_plugin_iceberg_engine_refuses_a_change_bigquery_cannot_make(
    monkeypatch, work_dir
):
    bq_client = _evolving_clients(
        monkeypatch, [("id", "NUMERIC")], [("id", "STRING")]
    )
    with pytest.raises(BigQueryRuntimeException):
        _write(work_dir, _ICEBERG_ENGINE)
    assert _alters(bq_client) == []


def test_dest_plugin_iceberg_engine_evolves_before_it_loads(monkeypatch, work_dir):
    # The rows have to land in a target already shaped to hold them.
    bq_client = _evolving_clients(
        monkeypatch, [("id", "INTEGER")], [("id", "INTEGER"), ("note", "STRING")]
    )
    _write(work_dir, _ICEBERG_ENGINE)
    # The final load is the last one; the batch-schema read comes before it.
    loaded = [c.args[1] for c in bq_client.load_table_from_uri.call_args_list]
    assert loaded[-1] == "my-project.analytics.users"
    assert loaded[0].startswith("my-project.analytics.users_")
    assert bq_client.query_and_wait.called


def test_dest_plugin_iceberg_engine_drops_the_table_it_read_the_batch_from(
    monkeypatch, work_dir
):
    bq_client = _evolving_clients(
        monkeypatch, [("id", "INTEGER")], [("id", "INTEGER")]
    )
    _write(work_dir, _ICEBERG_ENGINE)
    dropped = [c.args[0] for c in bq_client.delete_table.call_args_list]
    assert len(dropped) == 1 and dropped[0].startswith("my-project.analytics.users_")


def test_dest_plugin_iceberg_engine_does_nothing_under_strict(monkeypatch, work_dir):
    """Strict pins the target's schema; the engine only says *how* a schema
    that is allowed to move is moved."""
    bq_client = _evolving_clients(
        monkeypatch, [("id", "INTEGER")], [("id", "INTEGER"), ("note", "STRING")]
    )
    _write(work_dir, _ICEBERG_ENGINE, schema_evolution="strict")
    assert _alters(bq_client) == []
    assert bq_client.delete_table.call_count == 0


def test_dest_plugin_native_engine_emits_no_alters_of_its_own(monkeypatch, work_dir):
    """The counterpart: native evolution is the load-job options and nothing
    else, so a target short a column is left to the load job."""
    bq_client = _evolving_clients(
        monkeypatch, [("id", "INTEGER")], [("id", "INTEGER"), ("note", "STRING")]
    )
    _write(work_dir)
    assert _alters(bq_client) == []
    assert bq_client.load_table_from_uri.call_count == 1


def test_dest_plugin_iceberg_engine_skips_a_target_that_does_not_exist(
    monkeypatch, work_dir
):
    """The load job creates it from the batch, so there is nothing to
    reconcile -- and nothing to read it off."""
    bq_client = _evolving_clients(
        monkeypatch, [], [("id", "INTEGER")], target_exists=False
    )
    _write(work_dir, _ICEBERG_ENGINE)
    assert _alters(bq_client) == []


def test_dest_plugin_defaults_to_the_native_engine():
    assert plugin._resolve_dest_cfg(None).schema_evolution_engine == "native"


@pytest.mark.parametrize("engine", ["native", "iceberg"])
def test_dest_plugin_reads_the_engine_off_dest_cfg(engine):
    cfg = plugin._resolve_dest_cfg({"bigquery.schema_evolution_engine": engine})
    assert cfg.schema_evolution_engine == engine


def test_dest_plugin_rejects_an_unknown_engine():
    with pytest.raises(BigQueryValidateException) as ei:
        plugin._resolve_dest_cfg({"bigquery.schema_evolution_engine": "icebrg"})
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_21
    assert "'icebrg'" in str(ei.value)


# A MERGE takes no load-job options, so the connector evolves its target with
# ALTER statements or not at all -- and the write has to say so.
_ICEBERG = {"bigquery.schema_evolution_engine": "iceberg"}


def _bare_conn() -> BigQueryDestConn:
    return BigQueryDestConn(
        staging=_staging(),
        credentials=GCPJson(json=Secret(_gcp_key())),
        project="my-project",
        dataset="analytics",
    )


# ---------------------------------------------------------------------------
# _impl.unquote_table_fqn
# ---------------------------------------------------------------------------


def test_unquote_table_fqn_strips_outer_backticks():
    assert (
        impl.unquote_table_fqn("`my-project.analytics.users`")
        == "my-project.analytics.users"
    )


def test_unquote_table_fqn_handles_dataset_table():
    assert impl.unquote_table_fqn("`analytics.users`") == "analytics.users"


def test_unquote_table_fqn_handles_unqualified_table():
    assert impl.unquote_table_fqn("`users`") == "users"


def test_unquote_table_fqn_unescapes_inner_backtick():
    # backslash + backtick is the BigQuery escape for a literal backtick
    # inside the FQN.
    assert impl.unquote_table_fqn(r"`my-project.a\`b.c`") == "my-project.a`b.c"


# ---------------------------------------------------------------------------
# _impl.build_load_job_config
# ---------------------------------------------------------------------------


def test_load_job_config_replace_uses_write_truncate():
    from google.cloud import bigquery

    cfg = impl.build_load_job_config("replace", "update")
    assert cfg.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    # replace wipes the schema, so no update options carry forward.
    assert list(cfg.schema_update_options or []) == []


def test_load_job_config_append_strict_no_schema_update():
    from google.cloud import bigquery

    cfg = impl.build_load_job_config("append", "strict")
    assert cfg.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert list(cfg.schema_update_options or []) == []


def test_load_job_config_append_update_emits_field_options():
    from google.cloud import bigquery

    cfg = impl.build_load_job_config("append", "update")
    assert cfg.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert set(cfg.schema_update_options or []) == {
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
    }


def test_load_job_config_iceberg_engine_takes_the_field_options_off():
    """The connector has already ALTERed the target into shape; leaving
    BigQuery's own evolution on would be a second engine on one write, and one
    that would quietly re-add a column the Iceberg pass had refused."""
    cfg = impl.build_load_job_config("append", "update", "iceberg")
    assert list(cfg.schema_update_options or []) == []


def test_load_job_config_defaults_to_the_native_engine():
    from google.cloud import bigquery

    cfg = impl.build_load_job_config("append", "update")
    assert set(cfg.schema_update_options or []) == {
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
    }


def test_load_job_config_parquet_source_and_create_if_needed():
    from google.cloud import bigquery

    cfg = impl.build_load_job_config("append", "update")
    assert cfg.source_format == bigquery.SourceFormat.PARQUET
    assert cfg.create_disposition == bigquery.CreateDisposition.CREATE_IF_NEEDED


def test_merge_staging_load_job_config_truncates_and_creates():
    from google.cloud import bigquery

    cfg = impl.build_merge_staging_load_job_config()
    assert cfg.source_format == bigquery.SourceFormat.PARQUET
    assert cfg.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert cfg.create_disposition == bigquery.CreateDisposition.CREATE_IF_NEEDED
    # The staging table takes the parquet's schema outright; evolving the
    # target is the MERGE's job.
    assert list(cfg.schema_update_options or []) == []


def test_merge_staging_load_job_config_widens_decimals_like_a_direct_load():
    staging = impl.build_merge_staging_load_job_config()
    direct = impl.build_load_job_config("append", "update")
    assert staging.decimal_target_types == direct.decimal_target_types


# ---------------------------------------------------------------------------
# _impl.temp_table_fqn
# ---------------------------------------------------------------------------


def test_temp_table_fqn_suffixes_the_table_part():
    name = impl.temp_table_fqn("my-project.analytics.users")
    project, dataset, table = name.split(".")
    assert (project, dataset) == ("my-project", "analytics")
    assert table.startswith("users_")
    suffix = table.removeprefix("users_")
    assert len(suffix) == 8
    int(suffix, 16)


def test_temp_table_fqn_differs_per_call():
    """Two runs against one target must not share a staging table."""
    first = impl.temp_table_fqn("p.ds.t")
    second = impl.temp_table_fqn("p.ds.t")
    assert first != second


# ---------------------------------------------------------------------------
# _impl -- merge SQL
# ---------------------------------------------------------------------------

_TARGET = "my-project.analytics.orders"
_STAGING = "my-project.analytics.orders_a1b2c3d4"


def _merge_cfg(**overrides) -> MergeCfg:
    kwargs: dict = dict(on=["order_id"], op_column="op")
    kwargs.update(overrides)
    return MergeCfg(**kwargs)


def _schema_client(schemas: dict[str, list[str]]) -> MagicMock:
    """A ``bq_client`` stub whose ``get_table`` answers with the given columns."""

    def get_table(table_fqn: str):
        return SimpleNamespace(
            schema=[SimpleNamespace(name=name) for name in schemas[table_fqn]]
        )

    return MagicMock(get_table=MagicMock(side_effect=get_table))


def test_quote_table_wraps_the_whole_path_in_one_backtick_pair():
    assert impl.quote_table(_TARGET) == "`my-project.analytics.orders`"


def test_quote_column_backticks_a_single_name():
    assert impl.quote_column("order_id") == "`order_id`"
    assert impl.quote_column("weird col") == "`weird col`"


# ---------------------------------------------------------------------------
# _impl -- schema evolution (Iceberg dialect)
# ---------------------------------------------------------------------------


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
            raise NotFound(f"Not found: Table {table_fqn}")
        return SimpleNamespace(schema=schemas[table_fqn])

    return MagicMock(get_table=MagicMock(side_effect=get_table))


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        (_field("a", "INTEGER"), "INT64"),
        (_field("a", "FLOAT"), "FLOAT64"),
        (_field("a", "BOOLEAN"), "BOOL"),
        (_field("a", "STRING"), "STRING"),
        (_field("a", "TIMESTAMP"), "TIMESTAMP"),
        (_field("a", "NUMERIC"), "NUMERIC"),
        (_field("a", "STRING", mode="REPEATED"), "ARRAY<STRING>"),
    ],
)
def test_column_type_reads_the_googlesql_name(field, expected):
    """``field_type`` reports the legacy name (INTEGER, BOOLEAN, RECORD), which
    is not what DDL should carry."""
    assert impl.column_type(field) == expected


def test_column_type_renders_a_struct():
    field = _field(
        "addr",
        "RECORD",
        fields=[_field("city", "STRING"), _field("zip", "INTEGER")],
    )
    assert impl.column_type(field) == "STRUCT<`city` STRING, `zip` INT64>"


def test_column_type_renders_a_repeated_struct():
    field = _field("lines", "RECORD", mode="REPEATED", fields=[_field("q", "INTEGER")])
    assert impl.column_type(field) == "ARRAY<STRUCT<`q` INT64>>"


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        (_field("a", "NUMERIC", precision=20, scale=4), "NUMERIC(20, 4)"),
        (_field("a", "BIGNUMERIC", precision=40), "BIGNUMERIC(40)"),
        (_field("a", "STRING", max_length=50), "STRING(50)"),
        (_field("a", "BYTES", max_length=16), "BYTES(16)"),
    ],
)
def test_column_type_keeps_type_parameters(field, expected):
    """``to_standard_sql()`` drops them, so a target built from it would take
    the type's whole range instead of the staging table's."""
    assert impl.column_type(field) == expected


def test_column_type_omits_parameters_a_type_does_not_carry():
    assert impl.column_type(_field("a", "NUMERIC")) == "NUMERIC"
    assert impl.column_type(_field("a", "STRING")) == "STRING"


def test_column_type_keeps_parameters_on_a_repeated_element():
    field = _field("a", "NUMERIC", mode="REPEATED", precision=10, scale=2)
    assert impl.column_type(field) == "ARRAY<NUMERIC(10, 2)>"


def test_column_type_keeps_parameters_on_a_struct_field():
    field = _field(
        "money",
        "RECORD",
        fields=[
            _field("amount", "NUMERIC", precision=9, scale=2),
            _field("currency", "STRING", max_length=3),
        ],
    )
    assert (
        impl.column_type(field)
        == "STRUCT<`amount` NUMERIC(9, 2), `currency` STRING(3)>"
    )


def test_table_column_types_pairs_names_with_googlesql_types():
    client = _field_client({_TARGET: [_field("id", "INTEGER"), _field("s", "STRING")]})
    assert impl.table_column_types(client, _TARGET) == [("id", "INT64"), ("s", "STRING")]


# ---------------------------------------------------------------------------
# _impl.BigQueryWriteCfg.from_dest
# ---------------------------------------------------------------------------


def test_write_cfg_unwraps_backticked_tables():
    dest = BigQueryDest(
        tables=["`my-project.analytics.users`", "`my-project.analytics.orders`"],
    )
    cfg = impl.BigQueryWriteCfg.from_dest(_bare_conn(), dest)
    assert cfg.target_tables == [
        "my-project.analytics.users",
        "my-project.analytics.orders",
    ]
    assert cfg.if_table_exists == "append"
    assert cfg.schema_evolution == "strict"


def test_write_cfg_handles_empty_tables():
    dest = BigQueryDest(tables=[])
    cfg = impl.BigQueryWriteCfg.from_dest(_bare_conn(), dest)
    assert cfg.target_tables == []


def test_write_cfg_carries_no_merge_cfgs_outside_merge_mode():
    dest = BigQueryDest(tables=["`p.ds.users`"])
    cfg = impl.BigQueryWriteCfg.from_dest(_bare_conn(), dest)
    assert cfg.merge_cfgs is None


def test_write_cfg_broadcasts_one_merge_cfg_across_every_table():
    merge_cfg = MergeCfg(on=["user_id"], op_column="op")
    dest = BigQueryDest(
        tables=["`p.ds.users`", "`p.ds.orders`"],
        if_table_exists="merge",
        merge_cfg=merge_cfg, dest_cfg=_ICEBERG,
    )
    cfg = impl.BigQueryWriteCfg.from_dest(_bare_conn(), dest)
    assert cfg.merge_cfgs == [merge_cfg, merge_cfg]


def test_write_cfg_keeps_a_merge_cfg_per_table_aligned_with_target_tables():
    first = MergeCfg(on=["user_id"], op_column="op")
    second = MergeCfg(on=["order_id"], op_column="op")
    dest = BigQueryDest(
        tables=["`users`", "`orders`"],
        if_table_exists="merge",
        merge_cfg=[first, second], dest_cfg=_ICEBERG,
    )
    cfg = impl.BigQueryWriteCfg.from_dest(_bare_conn(), dest)
    assert cfg.target_tables == [
        "my-project.analytics.users",
        "my-project.analytics.orders",
    ]
    assert cfg.merge_cfgs == [first, second]


# ---------------------------------------------------------------------------
# _impl.qualify_table_fqn
# ---------------------------------------------------------------------------


def test_qualify_table_fqn_prefixes_dataset_and_project():
    assert (
        impl.qualify_table_fqn("wld_country", "my-project", "analytics")
        == "my-project.analytics.wld_country"
    )


def test_qualify_table_fqn_prefixes_project_for_dataset_table():
    assert (
        impl.qualify_table_fqn("analytics.users", "my-project", "analytics")
        == "my-project.analytics.users"
    )


def test_qualify_table_fqn_leaves_full_name_unchanged():
    assert (
        impl.qualify_table_fqn("other-project.ds.t", "my-project", "analytics")
        == "other-project.ds.t"
    )


def test_qualify_table_fqn_bare_without_dataset_unchanged():
    # No dataset default -> nothing to complete; the bare name is left
    # for the SDK to reject (rather than shifting it into a wrong slot).
    assert impl.qualify_table_fqn("wld_country", "my-project", None) == "wld_country"


def test_qualify_table_fqn_dataset_table_without_project_unchanged():
    assert impl.qualify_table_fqn("analytics.users", None, None) == "analytics.users"


# ---------------------------------------------------------------------------
# _impl.BigQueryWriteCfg.from_dest -- FQN completion from the connection
# ---------------------------------------------------------------------------


def test_write_cfg_qualifies_bare_table_from_conn():
    dest = BigQueryDest(tables=["`wld_country`"])
    cfg = impl.BigQueryWriteCfg.from_dest(_bare_conn(), dest)
    assert cfg.target_tables == ["my-project.analytics.wld_country"]


def test_write_cfg_qualifies_dataset_table_with_conn_project():
    dest = BigQueryDest(tables=["`analytics.orders`"])
    cfg = impl.BigQueryWriteCfg.from_dest(_bare_conn(), dest)
    assert cfg.target_tables == ["my-project.analytics.orders"]


def test_write_cfg_bare_table_unqualified_without_conn_dataset():
    conn = BigQueryDestConn(
        staging=_staging(),
        credentials=GCPJson(json=Secret(_gcp_key())),
        project="my-project",
    )
    dest = BigQueryDest(tables=["`wld_country`"])
    cfg = impl.BigQueryWriteCfg.from_dest(conn, dest)
    assert cfg.target_tables == ["wld_country"]


# ---------------------------------------------------------------------------
# _BigQueryDestPlugin.write_out -- defensive / error wrapping
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_bigquery_4_on_tables_slots_mismatch(work_dir):
    dest = BigQueryDest(
        tables=["`my-project.analytics.users`", "`my-project.analytics.orders`"],
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(BigQueryRuntimeException) as ei:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_4


def test_dest_plugin_raises_bigquery_4_when_empty_tables(work_dir):
    dest = BigQueryDest(tables=[])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(BigQueryRuntimeException) as ei:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_4


def test_dest_plugin_wraps_client_init_failure_as_bigquery_5(
    monkeypatch,
    work_dir,
):
    def _boom(*args, **kwargs):
        raise RuntimeError("client boom")

    monkeypatch.setattr(impl, "build_bigquery_client", _boom)
    # GCS client construction shouldn't be reached, but a defensive
    # stub keeps the test from accidentally creating a real one if the
    # short-circuit regresses.
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: MagicMock())

    dest = BigQueryDest(
        tables=["`my-project.analytics.users`"],
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(BigQueryRuntimeException) as ei:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_5


def test_dest_plugin_wraps_gcs_upload_failure_as_bigquery_6(
    monkeypatch,
    work_dir,
):
    bq_client = MagicMock(name="bq")
    monkeypatch.setattr(
        impl,
        "build_bigquery_client",
        lambda *a, **kw: bq_client,
    )

    gcs_client = MagicMock(name="gcs")
    bucket = MagicMock(name="bucket")
    blob = MagicMock(name="blob")
    gcs_client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    blob.upload_from_filename.side_effect = RuntimeError("gcs upload boom")
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    dest = BigQueryDest(
        tables=["`my-project.analytics.users`"],
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(BigQueryRuntimeException) as ei:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_6
    # Best-effort cleanup still attempts to delete the staging URI even
    # when the upload (which created the partial / nothing) failed.
    blob.delete.assert_called()


def test_dest_plugin_wraps_load_job_failure_as_bigquery_7(
    monkeypatch,
    work_dir,
):
    load_job = MagicMock(name="load_job")
    load_job.result.side_effect = RuntimeError("load_job boom")

    bq_client = MagicMock(name="bq")
    bq_client.load_table_from_uri.return_value = load_job
    monkeypatch.setattr(
        impl,
        "build_bigquery_client",
        lambda *a, **kw: bq_client,
    )

    gcs_client = MagicMock(name="gcs")
    bucket = MagicMock(name="bucket")
    blob = MagicMock(name="blob")
    gcs_client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    dest = BigQueryDest(
        tables=["`my-project.analytics.users`"],
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(BigQueryRuntimeException) as ei:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_7
    # Upload succeeded, so cleanup runs unconditionally.
    blob.delete.assert_called()


# ---------------------------------------------------------------------------
# _BigQueryDestPlugin.write_out -- happy path against mocked clients
# ---------------------------------------------------------------------------


def test_dest_plugin_happy_path_calls_load_table_from_uri_per_table(
    monkeypatch,
    work_dir,
):
    load_job = MagicMock(name="load_job")
    load_job.result.return_value = None
    load_job.output_rows = 1

    bq_client = MagicMock(name="bq")
    bq_client.load_table_from_uri.return_value = load_job
    monkeypatch.setattr(
        impl,
        "build_bigquery_client",
        lambda *a, **kw: bq_client,
    )

    gcs_client = MagicMock(name="gcs")
    bucket = MagicMock(name="bucket")
    blob = MagicMock(name="blob")
    gcs_client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    dest = BigQueryDest(
        tables=[
            "`my-project.analytics.users`",
            "`my-project.analytics.orders`",
        ],
    )
    src1 = _make_parquet(work_dir / "in1.parquet", [{"id": 1}])
    src2 = _make_parquet(work_dir / "in2.parquet", [{"id": 2}])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir),
        _bare_conn(),
        dest,
        [src1, src2],
    )

    # Two parquet -> GCS uploads.
    assert blob.upload_from_filename.call_count == 2
    # Two BQ load jobs, one per table.
    assert bq_client.load_table_from_uri.call_count == 2
    # Load job called with bare (unquoted) table names.
    called_tables = [
        call.args[1] for call in bq_client.load_table_from_uri.call_args_list
    ]
    assert called_tables == [
        "my-project.analytics.users",
        "my-project.analytics.orders",
    ]
    # Cleanup attempted for both staging URIs.
    assert blob.delete.call_count == 2


def _merge_ready_client() -> MagicMock:
    """A ``bq_client`` stub the whole merge path can run against.

    Every table answers with the same ``id, other_id, op`` schema, so the marker
    and either key are where ``merge_into`` looks for them, and the schema
    evolution pass finds nothing to add. Real ``SchemaField``s, since evolution
    reads each column's type off ``to_standard_sql()``.
    """
    load_job = MagicMock(name="load_job")
    load_job.result.return_value = None

    bq_client = MagicMock(name="bq")
    bq_client.load_table_from_uri.return_value = load_job
    bq_client.get_table.side_effect = lambda table_fqn: SimpleNamespace(
        schema=[
            _field("id", "INTEGER"),
            _field("other_id", "INTEGER"),
            _field("op", "STRING"),
        ]
    )
    return bq_client


def test_dest_plugin_loads_a_merge_batch_into_a_staging_table(monkeypatch, work_dir):
    from google.cloud import bigquery

    bq_client = _merge_ready_client()
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)

    gcs_client = MagicMock(name="gcs")
    bucket = MagicMock(name="bucket")
    blob = MagicMock(name="blob")
    gcs_client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    dest = BigQueryDest(
        tables=["`my-project.analytics.users`"],
        if_table_exists="merge",
        merge_cfg=MergeCfg(on=["id"], op_column="op"), dest_cfg=_ICEBERG,
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1, "op": "i"}])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [src]
    )

    (call,) = bq_client.load_table_from_uri.call_args_list
    loaded_table = call.args[1]
    # The batch lands beside the target, never in it.
    assert loaded_table != "my-project.analytics.users"
    assert loaded_table.startswith("my-project.analytics.users_")
    suffix = loaded_table.rsplit("_", 1)[1]
    assert len(suffix) == 8
    int(suffix, 16)

    job_config = call.kwargs["job_config"]
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert (
        job_config.create_disposition == bigquery.CreateDisposition.CREATE_IF_NEEDED
    )
    assert job_config.source_format == bigquery.SourceFormat.PARQUET
    # Evolving the target is the MERGE's job, not this load's.
    assert not job_config.schema_update_options


def test_dest_plugin_gives_each_merge_table_its_own_staging_table(
    monkeypatch, work_dir
):
    bq_client = _merge_ready_client()
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)

    gcs_client = MagicMock(name="gcs")
    bucket = MagicMock(name="bucket")
    blob = MagicMock(name="blob")
    gcs_client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    dest = BigQueryDest(
        tables=["`p.ds.users`", "`p.ds.users`"],
        if_table_exists="merge",
        merge_cfg=MergeCfg(on=["id"], op_column="op"), dest_cfg=_ICEBERG,
    )
    src1 = _make_parquet(work_dir / "in1.parquet", [{"id": 1, "op": "i"}])
    src2 = _make_parquet(work_dir / "in2.parquet", [{"id": 2, "op": "i"}])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [src1, src2]
    )

    loaded = [c.args[1] for c in bq_client.load_table_from_uri.call_args_list]
    assert len(set(loaded)) == 2


def test_dest_plugin_merges_the_staging_table_then_drops_it(monkeypatch, work_dir):
    bq_client = _merge_ready_client()
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)

    gcs_client = MagicMock(name="gcs")
    bucket = MagicMock(name="bucket")
    blob = MagicMock(name="blob")
    gcs_client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    dest = BigQueryDest(
        tables=["`my-project.analytics.users`"],
        if_table_exists="merge",
        merge_cfg=MergeCfg(on=["id"], op_column="op"), dest_cfg=_ICEBERG,
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1, "op": "i"}])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [src]
    )

    staged = bq_client.load_table_from_uri.call_args_list[0].args[1]
    (merge,) = _statements(bq_client, "MERGE INTO")
    assert merge.startswith("MERGE INTO `my-project.analytics.users` AS target")
    assert f"USING `{staged}` AS source" in merge
    bq_client.delete_table.assert_called_once_with(staged, not_found_ok=True)


def _widening_client() -> MagicMock:
    """A ``bq_client`` stub whose staging table is one column wider than the
    target -- and carries the marker the target must not gain.

    A completed ``ALTER`` shows up in the next ``get_table``, the way BigQuery
    reports a table once the DDL job has finished. Without that the stub could
    not tell a merge that writes the added column from one that ignores it.
    """
    load_job = MagicMock(name="load_job")
    load_job.result.return_value = None

    target = "my-project.analytics.users"
    schemas = {target: [_field("id", "INTEGER")]}
    staged = [_field("id", "INTEGER"), _field("note", "STRING"), _field("op", "STRING")]

    def get_table(table_fqn: str):
        return SimpleNamespace(schema=list(schemas.get(table_fqn, staged)))

    def query_and_wait(statement: str):
        if statement.startswith("ALTER TABLE") and "`note`" in statement:
            schemas[target] = [*schemas[target], _field("note", "STRING")]
        return MagicMock(name="rows")

    bq_client = MagicMock(name="bq")
    bq_client.load_table_from_uri.return_value = load_job
    bq_client.get_table.side_effect = get_table
    bq_client.query_and_wait.side_effect = query_and_wait
    return bq_client


def _statements(bq_client: MagicMock, prefix: str = "") -> list[str]:
    """The statements sent to ``query_and_wait``, in order.

    Selected by kind rather than by position: every merge write now issues a
    CREATE ahead of the rest, and a run may or may not issue an ALTER.
    """
    return [
        call.args[0]
        for call in bq_client.query_and_wait.call_args_list
        if call.args[0].startswith(prefix)
    ]


def _merge_dest(schema_evolution):
    return BigQueryDest(
        tables=["`my-project.analytics.users`"],
        if_table_exists="merge",
        schema_evolution=schema_evolution,
        merge_cfg=MergeCfg(on=["id"], op_column="op"), dest_cfg=_ICEBERG,
    )


def test_dest_plugin_evolves_the_target_before_merging(monkeypatch, work_dir):
    bq_client = _widening_client()
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: MagicMock())

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1, "note": "n", "op": "i"}])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), _merge_dest("update"), [src]
    )

    statements = _statements(bq_client)
    (alter,) = _statements(bq_client, "ALTER TABLE")
    (merge,) = _statements(bq_client, "MERGE INTO")
    # The column is added first, so the MERGE can write it.
    assert statements.index(alter) < statements.index(merge)
    assert alter == (
        "ALTER TABLE `my-project.analytics.users`"
        " ADD COLUMN IF NOT EXISTS `note` STRING"
    )
    assert "`note` = source.`note`" in merge
    # The marker is not a column the target gains.
    assert "`op`" not in alter


def test_dest_plugin_does_not_evolve_the_target_in_strict_mode(monkeypatch, work_dir):
    bq_client = _widening_client()
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: MagicMock())

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1, "note": "n", "op": "i"}])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), _merge_dest("strict"), [src]
    )

    assert _statements(bq_client, "ALTER TABLE") == []
    (merge,) = _statements(bq_client, "MERGE INTO")
    # The staged column the target does not have is simply not written.
    assert "`note`" not in merge


def _uploaded_rows(blob: MagicMock) -> list[dict]:
    """The rows of the parquet that reached GCS.

    Read inside the upload call, since the coalesced file lives in a temp
    directory that is gone by the time ``write_out`` returns.
    """
    return blob.captured


def _capturing_gcs() -> tuple[MagicMock, MagicMock]:
    gcs_client = MagicMock(name="gcs")
    bucket = MagicMock(name="bucket")
    blob = MagicMock(name="blob")
    gcs_client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    blob.captured = []

    def upload_from_filename(path: str) -> None:
        blob.captured.extend(pl.read_parquet(path).to_dicts())

    blob.upload_from_filename.side_effect = upload_from_filename
    return gcs_client, blob


def test_dest_plugin_coalesces_the_batch_before_uploading_it(monkeypatch, work_dir):
    bq_client = _merge_ready_client()
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)
    gcs_client, blob = _capturing_gcs()
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    src = _make_parquet(
        work_dir / "in.parquet",
        [
            {"id": 1, "other_id": 10, "op": "i"},
            {"id": 1, "other_id": 11, "op": "u"},
            {"id": 2, "other_id": 20, "op": "i"},
            {"id": 2, "other_id": 20, "op": "d"},
        ],
    )
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), _merge_dest("strict"), [src]
    )

    # Key 1 reduced to its net state; key 2 never existed.
    assert _uploaded_rows(blob) == [{"id": 1, "other_id": 11, "op": "i"}]


def test_dest_plugin_uploads_the_batch_untouched_outside_merge_mode(
    monkeypatch, work_dir
):
    load_job = MagicMock(name="load_job")
    load_job.result.return_value = None
    bq_client = MagicMock(name="bq")
    bq_client.load_table_from_uri.return_value = load_job
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)
    gcs_client, blob = _capturing_gcs()
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    rows = [{"id": 1, "op": "i"}, {"id": 1, "op": "u"}]
    src = _make_parquet(work_dir / "in.parquet", rows)
    dest = BigQueryDest(tables=["`my-project.analytics.users`"])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [src]
    )

    assert _uploaded_rows(blob) == rows


def test_dest_plugin_refuses_a_batch_without_the_marker_before_uploading(
    monkeypatch, work_dir
):
    bq_client = _merge_ready_client()
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)
    gcs_client, blob = _capturing_gcs()
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(BigQueryRuntimeException) as ei:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), _merge_dest("strict"), [src]
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_15
    # Nothing was staged, so nothing was loaded either.
    blob.upload_from_filename.assert_not_called()
    bq_client.load_table_from_uri.assert_not_called()


def _creating_client() -> MagicMock:
    """A stub whose target does not exist until the CREATE has run.

    ``get_table`` reports the target absent, then reports it once the DDL has
    been through -- so a test can tell a create that happened first from one
    that happened at all.
    """
    from google.cloud.exceptions import NotFound

    load_job = MagicMock(name="load_job")
    load_job.result.return_value = None

    target = "my-project.analytics.users"
    staged = [_field("id", "INTEGER"), _field("note", "STRING"), _field("op", "STRING")]
    created: dict[str, list] = {}

    def get_table(table_fqn: str):
        if table_fqn != target:
            return SimpleNamespace(schema=staged)
        if target not in created:
            raise NotFound(f"Not found: Table {target}")
        return SimpleNamespace(schema=created[target])

    def query_and_wait(statement: str):
        if statement.startswith("CREATE TABLE"):
            created[target] = [f for f in staged if f.name != "op"]
        return MagicMock(name="rows")

    bq_client = MagicMock(name="bq")
    bq_client.load_table_from_uri.return_value = load_job
    bq_client.get_table.side_effect = get_table
    bq_client.query_and_wait.side_effect = query_and_wait
    return bq_client


@pytest.mark.parametrize("schema_evolution", ["update", "strict"])
def test_dest_plugin_creates_a_missing_target_before_merging(
    monkeypatch, work_dir, schema_evolution
):
    """A merge reads the target to decide what it can write, so a target that is
    not there is built first -- in either evolution mode, since strict has to be
    able to bootstrap a target too."""
    bq_client = _creating_client()
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: MagicMock())

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1, "note": "n", "op": "i"}])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), _merge_dest(schema_evolution), [src]
    )

    statements = _statements(bq_client)
    (create,) = _statements(bq_client, "CREATE TABLE")
    (merge,) = _statements(bq_client, "MERGE INTO")
    assert statements.index(create) < statements.index(merge)
    assert create.startswith("CREATE TABLE IF NOT EXISTS `my-project.analytics.users`")
    # Built from the batch's own columns, without the marker.
    assert "`id` INT64" in create
    assert "`note` STRING" in create
    assert "`op`" not in create


def test_dest_plugin_does_not_create_a_target_that_already_exists(
    monkeypatch, work_dir
):
    bq_client = _merge_ready_client()
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: MagicMock())

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1, "op": "i"}])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), _merge_dest("update"), [src]
    )

    assert _statements(bq_client, "CREATE TABLE") == []
    assert len(_statements(bq_client, "MERGE INTO")) == 1


def test_dest_plugin_wraps_a_failed_schema_evolution_as_bigquery_18(
    monkeypatch, work_dir
):
    bq_client = _widening_client()
    bq_client.query_and_wait.side_effect = RuntimeError("alter boom")
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: MagicMock())

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1, "note": "n", "op": "i"}])
    with pytest.raises(BigQueryRuntimeException) as ei:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), _merge_dest("update"), [src]
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_18
    assert "my-project.analytics.users" in str(ei.value)
    # The staging table is still dropped.
    bq_client.delete_table.assert_called_once()


def test_dest_plugin_wraps_a_failed_merge_as_bigquery_18(monkeypatch, work_dir):
    bq_client = _merge_ready_client()
    bq_client.query_and_wait.side_effect = RuntimeError("merge boom")
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: MagicMock())

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1, "op": "i"}])
    with pytest.raises(BigQueryRuntimeException) as ei:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), _merge_dest("strict"), [src]
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_18
    bq_client.delete_table.assert_called_once()


def test_dest_plugin_keeps_a_classified_merge_error_unwrapped(monkeypatch, work_dir):
    """A staging table missing the marker has a precise code of its own;
    BIGQUERY_18 would bury which column the two sides disagree on."""
    load_job = MagicMock(name="load_job")
    load_job.result.return_value = None
    bq_client = MagicMock(name="bq")
    bq_client.load_table_from_uri.return_value = load_job
    # The batch carries the marker, so coalescing passes; the staging table
    # does not, so the merge is what objects.
    bq_client.get_table.side_effect = lambda table_fqn: SimpleNamespace(
        schema=[_field("id", "INTEGER")]
    )
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: MagicMock())

    src = _make_parquet(work_dir / "in.parquet", [{"id": 1, "op": "i"}])
    with pytest.raises(BigQueryRuntimeException) as ei:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), _merge_dest("strict"), [src]
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_15
    bq_client.delete_table.assert_called_once()


def test_dest_plugin_pairs_each_table_with_its_own_merge_cfg(monkeypatch, work_dir):
    """The cfg travels with its table: a list of configs lines up one-to-one
    with ``tables``, first with first."""
    bq_client = _merge_ready_client()
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)

    gcs_client = MagicMock(name="gcs")
    bucket = MagicMock(name="bucket")
    blob = MagicMock(name="blob")
    gcs_client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    dest = BigQueryDest(
        tables=["`p.ds.users`", "`p.ds.orders`"],
        if_table_exists="merge",
        merge_cfg=[
            MergeCfg(on=["id"], op_column="op"),
            MergeCfg(on=["other_id"], op_column="op"),
        ], dest_cfg=_ICEBERG,
    )
    rows = [{"id": 1, "other_id": 10, "op": "i"}]
    src1 = _make_parquet(work_dir / "in1.parquet", rows)
    src2 = _make_parquet(work_dir / "in2.parquet", rows)
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [src1, src2]
    )

    first, second = _statements(bq_client, "MERGE INTO")
    assert "ON target.`id` = source.`id`" in first
    assert "ON target.`other_id` = source.`other_id`" in second


def test_dest_plugin_names_the_staging_table_in_a_failed_merge_load(
    monkeypatch, work_dir
):
    load_job = MagicMock(name="load_job")
    load_job.result.side_effect = RuntimeError("load_job boom")

    bq_client = MagicMock(name="bq")
    bq_client.load_table_from_uri.return_value = load_job
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)

    gcs_client = MagicMock(name="gcs")
    bucket = MagicMock(name="bucket")
    blob = MagicMock(name="blob")
    gcs_client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    dest = BigQueryDest(
        tables=["`my-project.analytics.users`"],
        if_table_exists="merge",
        merge_cfg=MergeCfg(on=["id"], op_column="op"), dest_cfg=_ICEBERG,
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1, "op": "i"}])
    with pytest.raises(BigQueryRuntimeException) as ei:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_7
    assert "my-project.analytics.users_" in str(ei.value)
    blob.delete.assert_called()


def test_dest_plugin_loads_straight_into_the_table_outside_merge_mode(
    monkeypatch, work_dir
):
    load_job = MagicMock(name="load_job")
    load_job.result.return_value = None

    bq_client = MagicMock(name="bq")
    bq_client.load_table_from_uri.return_value = load_job
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)

    gcs_client = MagicMock(name="gcs")
    bucket = MagicMock(name="bucket")
    blob = MagicMock(name="blob")
    gcs_client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)

    dest = BigQueryDest(tables=["`my-project.analytics.users`"])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [src]
    )

    (call,) = bq_client.load_table_from_uri.call_args_list
    assert call.args[1] == "my-project.analytics.users"


def test_dest_plugin_forwards_project_to_bigquery_client(monkeypatch, work_dir):
    captured: dict = {}

    def _build_bq(credentials, project, connect_args):
        captured["project"] = project
        captured["connect_args"] = connect_args
        return MagicMock(
            load_table_from_uri=MagicMock(
                return_value=MagicMock(
                    result=MagicMock(return_value=None),
                )
            )
        )

    monkeypatch.setattr(impl, "build_bigquery_client", _build_bq)
    monkeypatch.setattr(
        impl,
        "build_gcs_client",
        lambda *a, **kw: MagicMock(
            bucket=MagicMock(
                return_value=MagicMock(
                    blob=MagicMock(return_value=MagicMock()),
                )
            ),
        ),
    )

    dest = BigQueryDest(
        tables=["`my-project.analytics.users`"],
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [src]
    )

    assert captured["project"] == "my-project"


# ---------------------------------------------------------------------------
# DestDef wiring
# ---------------------------------------------------------------------------


def test_bigquery_dest_def_wires_plugin_and_explorer():
    assert plugin.BIGQUERY_DEST.conn is BigQueryDestConn
    assert plugin.BIGQUERY_DEST.dest is BigQueryDest
    assert plugin.BIGQUERY_DEST.plugin is plugin._BigQueryDestPlugin
    assert plugin.BIGQUERY_DEST.explorer is BigQueryConnectionExplorer
    assert plugin.BIGQUERY_DEST.type_ == "bigquery-bulk-out"
    assert plugin.BIGQUERY_DEST.dest_version == "v1"


def test_bigquery_dest_cardinality_uses_tables_length():
    dest = BigQueryDest(
        tables=[
            "`my-project.analytics.users`",
            "`my-project.analytics.orders`",
        ],
    )
    assert plugin.BIGQUERY_DEST.cardinality(dest) == 2


def test_bigquery_dest_cardinality_zero_for_empty_tables():
    dest = BigQueryDest(tables=[])
    assert plugin.BIGQUERY_DEST.cardinality(dest) == 0


def test_bigquery_dest_requires_tables():
    with pytest.raises(ConnInitException) as ei:
        BigQueryDest()
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_3


# ---------------------------------------------------------------------------
# _resolve_dest_cfg
# ---------------------------------------------------------------------------


def test_resolve_dest_cfg_returns_defaults_when_empty():
    import logging as _logging

    cfg = plugin._resolve_dest_cfg(None)
    assert cfg.logging_level == _logging.ERROR


def test_resolve_dest_cfg_applies_overrides():
    cfg = plugin._resolve_dest_cfg({"bigquery.logging_level": 20})
    assert cfg.logging_level == 20


def test_resolve_dest_cfg_warns_on_unknown_key_via_raw_dict(caplog):
    with caplog.at_level("WARNING", logger=plugin.logger.name):
        plugin._resolve_dest_cfg({"bogus.key": "value"})
    assert any("Unknown dest_cfg key" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# watermark_column on an append
#
# An append is not idempotent on its own: a rerun of the same transaction
# uploads the batch again and the load job appends it a second time. Naming a
# watermark closes it -- the column is written into the parquet before it goes
# to GCS so the load stamps the rows it brings in, and a rerun that finds the
# target already carrying this trx's id does nothing at all.
# ---------------------------------------------------------------------------

_WATERMARK = "wm"
_TARGET_FQN = "my-project.analytics.users"


def _staging_gcs() -> tuple[MagicMock, list[tuple[Path, list[dict]]]]:
    """A GCS client recording the path and rows of every upload.

    Read inside the upload call: the rewritten batch lives in a temp directory
    removed as soon as it has been staged, so a test that looks afterwards
    finds nothing. The path is kept so a test can tell the copy from the
    original and check it was cleaned up.
    """
    gcs_client = MagicMock(name="gcs")
    blob = MagicMock(name="blob")
    gcs_client.bucket.return_value.blob.return_value = blob
    staged: list[tuple[Path, list[dict]]] = []

    def upload_from_filename(path: str) -> None:
        staged.append((Path(path), pl.read_parquet(path).to_dicts()))

    blob.upload_from_filename.side_effect = upload_from_filename
    return gcs_client, staged


def _watermark_clients(
    monkeypatch,
    target_columns=(("id", "INTEGER"), (_WATERMARK, "STRING")),
    batch_columns=(("id", "INTEGER"),),
    *,
    stamped=False,
    target_exists=True,
):
    """A bq client whose target really gains the columns its own DDL adds, plus
    a GCS client that records what it was given.

    The target's schema has to move: two passes read it -- the watermark's and
    the Iceberg diff's -- and a fake that answered the first from a frozen
    schema would report a table its own ALTER had already changed, so the
    second would add the column a second time.

    ``stamped`` answers the guard's probe. Every statement returns the one
    boolean, which the ALTERs and the load ignore.
    """
    from google.cloud.exceptions import NotFound

    target = list(target_columns)
    batch = list(batch_columns)

    def get_table(fqn):
        if fqn.startswith(f"{_TARGET_FQN}_"):  # the table the batch loads to
            return SimpleNamespace(schema=[_schema_field(n, t) for n, t in batch])
        if fqn == _TARGET_FQN and target_exists:
            return SimpleNamespace(schema=[_schema_field(n, t) for n, t in target])
        raise NotFound(f"{fqn} not found")

    added = re.compile(r"ADD COLUMN IF NOT EXISTS `([^`]+)` (\w+)")

    def query_and_wait(statement):
        match = added.search(statement)
        if match is not None and statement.startswith(f"ALTER TABLE `{_TARGET_FQN}`"):
            name, type_ = match.group(1), match.group(2)
            if all(existing != name for existing, _ in target):
                target.append((name, type_))
        return [(stamped,)]

    bq_client = MagicMock(
        name="bq",
        get_table=MagicMock(side_effect=get_table),
        query_and_wait=MagicMock(side_effect=query_and_wait),
    )
    bq_client.load_table_from_uri.return_value = MagicMock(
        result=MagicMock(return_value=None)
    )
    monkeypatch.setattr(impl, "build_bigquery_client", lambda *a, **kw: bq_client)

    gcs_client, staged = _staging_gcs()
    monkeypatch.setattr(impl, "build_gcs_client", lambda *a, **kw: gcs_client)
    return bq_client, staged


def _watermark_write(
    work_dir,
    *,
    watermark_column=_WATERMARK,
    if_table_exists="append",
    schema_evolution="strict",
    dest_cfg=None,
    trx_id="TRX_1",
):
    dest = BigQueryDest(
        tables=[f"`{_TARGET_FQN}`"],
        if_table_exists=if_table_exists,
        schema_evolution=schema_evolution,
        watermark_column=watermark_column,
        dest_cfg=dest_cfg,
    )
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}, {"id": 2}])
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir, trx_id), _bare_conn(), dest, [src]
    )
    return src


def _watermark_alters(bq_client):
    # The guard's probe names the column too, so the prefix is what tells an
    # ALTER apart from a read of it.
    return [
        s
        for s in _alters(bq_client)
        if s.startswith("ALTER TABLE") and f"`{_WATERMARK}`" in s
    ]


def test_dest_plugin_append_watermark_stamps_every_uploaded_row(
    monkeypatch, work_dir
):
    _, staged = _watermark_clients(monkeypatch)
    _watermark_write(work_dir)
    _, rows = staged[0]
    assert [row[_WATERMARK] for row in rows] == ["TRX_1", "TRX_1"]


def test_dest_plugin_append_watermark_uploads_a_copy_not_the_original(
    monkeypatch, work_dir
):
    _, staged = _watermark_clients(monkeypatch)
    src = _watermark_write(work_dir)
    path, _ = staged[0]
    assert path != src


def test_dest_plugin_append_without_a_watermark_uploads_what_it_was_handed(
    monkeypatch, work_dir
):
    _, staged = _watermark_clients(monkeypatch)
    src = _watermark_write(work_dir, watermark_column=None)
    path, rows = staged[0]
    assert path == src
    assert rows == [{"id": 1}, {"id": 2}]


def test_dest_plugin_append_watermark_removes_the_copy_once_it_is_uploaded(
    monkeypatch, work_dir
):
    _, staged = _watermark_clients(monkeypatch)
    _watermark_write(work_dir)
    path, _ = staged[0]
    assert not path.exists()


def test_dest_plugin_append_watermark_adds_the_column_to_the_target(
    monkeypatch, work_dir
):
    bq_client, _ = _watermark_clients(
        monkeypatch, target_columns=(("id", "INTEGER"),)
    )
    _watermark_write(work_dir)
    assert _watermark_alters(bq_client) == [
        f"ALTER TABLE `{_TARGET_FQN}` ADD COLUMN IF NOT EXISTS `{_WATERMARK}` STRING"
    ]


def test_dest_plugin_append_watermark_leaves_an_existing_column_alone(
    monkeypatch, work_dir
):
    bq_client, _ = _watermark_clients(monkeypatch)
    _watermark_write(work_dir)
    assert _watermark_alters(bq_client) == []


def test_dest_plugin_append_watermark_adds_the_column_before_the_load(
    monkeypatch, work_dir
):
    # A load job handed a batch carrying a column the target has not got fails
    # outright rather than dropping it, so the ALTER cannot come afterwards.
    bq_client, _ = _watermark_clients(
        monkeypatch, target_columns=(("id", "INTEGER"),)
    )
    _watermark_write(work_dir)
    assert _watermark_alters(bq_client)
    bq_client.load_table_from_uri.assert_called_once()


def test_dest_plugin_append_watermark_leaves_a_missing_target_to_the_load(
    monkeypatch, work_dir
):
    """The load job creates the target from the batch, which already carries
    the column, so there is nothing to ALTER onto a table that is not there."""
    bq_client, staged = _watermark_clients(
        monkeypatch, target_columns=(), target_exists=False
    )
    _watermark_write(work_dir)
    assert _watermark_alters(bq_client) == []
    assert [row[_WATERMARK] for row in staged[0][1]] == ["TRX_1", "TRX_1"]


def test_dest_plugin_append_watermark_leaves_the_iceberg_pass_nothing_to_add(
    monkeypatch, work_dir
):
    """Added ahead of the diff, so the engine that may also add columns finds
    it already there -- one statement, not two claims on the same column."""
    bq_client, _ = _watermark_clients(
        monkeypatch,
        target_columns=(("id", "INTEGER"),),
        batch_columns=(("id", "INTEGER"), (_WATERMARK, "STRING")),
    )
    _watermark_write(
        work_dir, schema_evolution="update", dest_cfg=_ICEBERG_ENGINE
    )
    assert len(_watermark_alters(bq_client)) == 1


def test_dest_plugin_append_skips_entirely_when_the_trx_is_already_stamped(
    monkeypatch, work_dir
):
    bq_client, staged = _watermark_clients(monkeypatch, stamped=True)
    _watermark_write(work_dir)
    assert staged == []
    bq_client.load_table_from_uri.assert_not_called()


def test_dest_plugin_append_proceeds_when_the_target_carries_another_trx(
    monkeypatch, work_dir
):
    bq_client, staged = _watermark_clients(monkeypatch, stamped=False)
    _watermark_write(work_dir)
    assert len(staged) == 1
    bq_client.load_table_from_uri.assert_called_once()


def test_dest_plugin_append_checks_the_watermark_before_it_uploads(
    monkeypatch, work_dir
):
    # A rerun with nothing to do should not pay to rewrite and ship a file it is
    # about to decline to load.
    bq_client, _ = _watermark_clients(monkeypatch)
    _watermark_write(work_dir)
    probe = f"SELECT COUNT(*) > 0 FROM `{_TARGET_FQN}` WHERE `{_WATERMARK}` = 'TRX_1'"
    assert _alters(bq_client)[0] == probe


@pytest.mark.parametrize(
    "trx_id,literal",
    [
        ("plain", "'plain'"),
        ("it's", "'it\\'s'"),
        ("back\\slash", "'back\\\\slash'"),
    ],
)
def test_dest_plugin_append_escapes_the_trx_id_in_the_probe(
    monkeypatch, work_dir, trx_id, literal
):
    # There is nowhere to bind a value in a statement built by hand.
    bq_client, _ = _watermark_clients(monkeypatch)
    _watermark_write(work_dir, trx_id=trx_id)
    assert _alters(bq_client)[0].endswith(f"= {literal}")


def test_dest_plugin_replace_never_stamps_even_when_handed_a_watermark(
    monkeypatch, work_dir
):
    """BigQueryDest refuses the pairing, and the write path declines it again
    rather than resting on a validation two layers away."""
    bq_client, staged = _watermark_clients(monkeypatch)
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    write_cfg = impl.BigQueryWriteCfg(
        conn=_bare_conn(),
        target_tables=[_TARGET_FQN],
        if_table_exists="replace",
        schema_evolution="strict",
        watermark_column=_WATERMARK,
    )
    monkeypatch.setattr(
        impl.BigQueryWriteCfg, "from_dest", classmethod(lambda cls, c, d: write_cfg)
    )
    dest = BigQueryDest(
        tables=[f"`{_TARGET_FQN}`"], if_table_exists="replace", schema_evolution="strict"
    )
    plugin._BigQueryDestPlugin().write_out(
        _dest_ctx(work_dir), _bare_conn(), dest, [src]
    )
    assert staged[0][0] == src
    assert _watermark_alters(bq_client) == []


def test_dest_plugin_append_does_not_clean_up_a_file_it_never_uploaded(
    monkeypatch, work_dir
):
    # Deleting a URI the guard declined to stage would warn about a file that is
    # absent by design.
    _, staged = _watermark_clients(monkeypatch, stamped=True)
    _watermark_write(work_dir)
    assert staged == []
