#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.bigquery import BigQueryDestConn, BigQueryDest

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import _impl as impl

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import _plugin as plugin
from tabsdatak.conn.bigquery.error import (
    BigQueryRuntimeException,
    BigqueryErrorCode,
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

    def work_dir(self) -> Path:
        return self._work


def _dest_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


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


def test_load_job_config_parquet_source_and_create_if_needed():
    from google.cloud import bigquery

    cfg = impl.build_load_job_config("append", "update")
    assert cfg.source_format == bigquery.SourceFormat.PARQUET
    assert cfg.create_disposition == bigquery.CreateDisposition.CREATE_IF_NEEDED


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
    assert cfg.schema_evolution == "update"


def test_write_cfg_handles_empty_tables():
    dest = BigQueryDest(tables=[])
    cfg = impl.BigQueryWriteCfg.from_dest(_bare_conn(), dest)
    assert cfg.target_tables == []


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
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_4


def test_dest_plugin_raises_bigquery_4_when_empty_tables(work_dir):
    dest = BigQueryDest(tables=[])
    src = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(BigQueryRuntimeException) as ei:
        plugin._BigQueryDestPlugin().write_out(
            _dest_ctx(work_dir), _bare_conn(), dest, [src]
        )
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_4


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
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_5


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
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_6
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
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_7
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
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_3


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
