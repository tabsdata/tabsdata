
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.file import _transporter
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AzureAccountKey,
    GCPJson,
    UserPassword,
)
from tabsdatak.conn.druid import (
    AzBlobLocation,
    DruidConn,
    DruidDest,
    GCSLocation,
    S3Location,
)

# noinspection PyProtectedMember
from tabsdatak.conn.druid import _impl as impl

# noinspection PyProtectedMember
from tabsdatak.conn.druid import _plugin as plugin
from tabsdatak.conn.druid.error import (
    DruidErrorCode,
    DruidRuntimeException,
)


pytestmark = pytest.mark.druid


# ---------------------------------------------------------------------------
# Test context shims for DestContext
# ---------------------------------------------------------------------------


@dataclass
class _Ctx:
    _work: Path
    _trx: str = "trx-unit"

    def work_dir(self) -> Path:
        return self._work

    @property
    def exec_plan_id(self) -> str:
        return "plan-unit"

    @property
    def trx_id(self) -> str:
        return self._trx

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"


def _dest_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


# ---------------------------------------------------------------------------
# Builders for the test connection / destination
# ---------------------------------------------------------------------------


def _s3_staging() -> S3Location:
    return S3Location(
        bucket="staging-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret.lit("AKIA..."),
            secret_access_key=Secret.lit("secret"),
        ),
        base_path="staging/",
    )


def _gcs_staging() -> GCSLocation:
    return GCSLocation(
        bucket="staging-bucket",
        credentials=GCPJson(json=Secret.lit('{"type": "service_account"}')),
        base_path="staging/",
    )


def _az_staging() -> AzBlobLocation:
    return AzBlobLocation(
        container="td-stage",
        credentials=AzureAccountKey(
            account_name=Secret.lit("acct"),
            account_key=Secret.lit("key"),
        ),
        base_path="staging/",
    )


def _conn(staging=None) -> DruidConn:
    return DruidConn(
        overlord_url="http://druid.example.com:8081",
        staging=staging or _s3_staging(),
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
    )


# ---------------------------------------------------------------------------
# DestPlugin.write_out -- defensive recheck (DRUID_6)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_druid_6_on_tables_slots_mismatch(work_dir):
    dest = DruidDest(conn=_conn(), tables=["events", "metrics"])
    parquet = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    with pytest.raises(DruidRuntimeException) as ei:
        plugin.DruidDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [parquet],
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_6


# ---------------------------------------------------------------------------
# DestPlugin.write_out -- staging upload failure (DRUID_7)
# ---------------------------------------------------------------------------


def test_dest_plugin_wraps_transporter_run_error_as_druid_7(
    monkeypatch, work_dir,
):
    def boom(*args, **kwargs):
        raise _transporter.TransporterRunError(1, "", "upload failed")

    monkeypatch.setattr(impl, "upload_staged_files", boom)
    parquet = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = DruidDest(conn=_conn(), tables=["events"])
    with pytest.raises(DruidRuntimeException) as ei:
        plugin.DruidDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [parquet],
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_7


def test_dest_plugin_wraps_transporter_binary_not_found_as_druid_7(
    monkeypatch, work_dir,
):
    def boom(*args, **kwargs):
        raise _transporter.TransporterBinaryNotFoundError("missing binary")

    monkeypatch.setattr(impl, "upload_staged_files", boom)
    parquet = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = DruidDest(conn=_conn(), tables=["events"])
    with pytest.raises(DruidRuntimeException) as ei:
        plugin.DruidDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [parquet],
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_7


# ---------------------------------------------------------------------------
# DestPlugin.write_out -- DRUID_8 (submit raises non-ApiException)
# ---------------------------------------------------------------------------


def test_dest_plugin_wraps_submit_failure_as_druid_8(
    monkeypatch, work_dir,
):
    monkeypatch.setattr(
        impl,
        "upload_staged_files",
        lambda **kwargs: ["s3://staging-bucket/staging/dummy"],
    )

    fake_client = MagicMock()
    fake_client.submit_task.side_effect = RuntimeError("network down")
    monkeypatch.setattr(
        impl,
        "make_overlord_client_for_dest",
        lambda conn, dest: fake_client,
    )
    parquet = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = DruidDest(conn=_conn(), tables=["events"])
    with pytest.raises(DruidRuntimeException) as ei:
        plugin.DruidDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [parquet],
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_8
    # The URL embedded in the error is the safe-sanitized form.
    assert "druid.example.com" in ei.value.extra["url"]


# ---------------------------------------------------------------------------
# DestPlugin.write_out -- DRUID_9 (task FAILED)
# ---------------------------------------------------------------------------


def test_dest_plugin_wraps_task_failure_as_druid_9(monkeypatch, work_dir):
    monkeypatch.setattr(
        impl,
        "upload_staged_files",
        lambda **kwargs: ["s3://staging-bucket/staging/dummy"],
    )

    fake_client = MagicMock()
    fake_client.submit_task.return_value = "task-123"

    def _wait(_task_id):
        # noinspection PyProtectedMember
        raise DruidErrorCode.DRUID_9.exception(
            datasource="?",
            task_id=_task_id,
            reason="bad parquet",
        )

    fake_client.wait_for_completion.side_effect = _wait
    monkeypatch.setattr(
        impl,
        "make_overlord_client_for_dest",
        lambda conn, dest: fake_client,
    )
    parquet = _make_parquet(work_dir / "in.parquet", [{"id": 1}])
    dest = DruidDest(conn=_conn(), tables=["events"])
    with pytest.raises(DruidRuntimeException) as ei:
        plugin.DruidDestPlugin().write_out(
            _dest_ctx(work_dir), dest, [parquet],
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_9
    # The datasource is hydrated from the dest's tables -- the
    # original placeholder ("?") must be replaced.
    assert ei.value.extra["datasource"] == "events"
    assert ei.value.extra["task_id"] == "task-123"
    assert "bad parquet" in ei.value.extra["reason"]


# ---------------------------------------------------------------------------
# DestPlugin.write_out -- happy path with a mocked client
# ---------------------------------------------------------------------------


def test_dest_plugin_submits_one_task_per_table(monkeypatch, work_dir):
    captured: dict[str, Any] = {"specs": [], "uploads": []}

    def _fake_upload(**kwargs):
        captured["uploads"].append(kwargs)
        return ["s3://staging-bucket/staging/dummy"]

    monkeypatch.setattr(impl, "upload_staged_files", _fake_upload)

    fake_client = MagicMock()

    def _fake_submit(spec: dict) -> str:
        captured["specs"].append(spec)
        return f"task-{len(captured['specs'])}"

    fake_client.submit_task.side_effect = _fake_submit
    fake_client.wait_for_completion.return_value = None
    monkeypatch.setattr(
        impl,
        "make_overlord_client_for_dest",
        lambda conn, dest: fake_client,
    )

    parquet_a = _make_parquet(work_dir / "a.parquet", [{"id": 1}])
    parquet_b = _make_parquet(work_dir / "b.parquet", [{"id": 2}])
    dest = DruidDest(
        conn=_conn(),
        tables=["events", "metrics"],
        if_table_exists="replace",
        time_column="ts",
        segment_granularity="HOUR",
        max_concurrent_subtasks=4,
    )
    plugin.DruidDestPlugin().write_out(
        _dest_ctx(work_dir), dest, [parquet_a, parquet_b],
    )

    # One ingestion task per target datasource.
    assert [s["spec"]["dataSchema"]["dataSource"] for s in captured["specs"]] == [
        "events", "metrics",
    ]
    # All specs honour replace semantics + tuning knobs.
    for s in captured["specs"]:
        assert s["spec"]["ioConfig"]["dropExisting"] is True
        assert s["spec"]["ioConfig"]["appendToExisting"] is False
        assert s["spec"]["dataSchema"]["timestampSpec"]["column"] == "ts"
        assert s["spec"]["dataSchema"]["granularitySpec"][
            "segmentGranularity"
        ] == "HOUR"
        assert s["spec"]["tuningConfig"]["maxNumConcurrentSubTasks"] == 4
    # Both upload calls used the same staging subdir root (per
    # transaction).
    subdirs = {kw["subdir"] for kw in captured["uploads"]}
    assert len(subdirs) == 2  # different slot suffix per datasource
    assert all(s.startswith("td-druid/trx-unit/") for s in subdirs)


# ---------------------------------------------------------------------------
# Adapter helpers -- ingestion spec / input source
# ---------------------------------------------------------------------------


def test_build_ingestion_spec_append_default():
    spec = impl.build_ingestion_spec(
        datasource="events",
        input_source={"type": "s3", "uris": ["s3://x/y"]},
        is_replace=False,
        time_column=None,
        segment_granularity="ALL",
        max_concurrent_subtasks=1,
        max_rows_per_segment=5_000_000,
        max_rows_in_memory=1_000_000,
    )
    assert spec["type"] == "index_parallel"
    assert spec["spec"]["dataSchema"]["dataSource"] == "events"
    assert spec["spec"]["ioConfig"]["appendToExisting"] is True
    assert spec["spec"]["ioConfig"]["dropExisting"] is False
    # No time column -> dummy timestamp column injected.
    ts_spec = spec["spec"]["dataSchema"]["timestampSpec"]
    assert ts_spec["column"] == impl.DUMMY_TIME_COLUMN
    assert "missingValue" in ts_spec
    # The replace-mode interval guard is absent on append.
    assert "intervals" not in spec["spec"]["dataSchema"]["granularitySpec"]


def test_build_ingestion_spec_replace_adds_intervals():
    spec = impl.build_ingestion_spec(
        datasource="events",
        input_source={"type": "s3", "uris": ["s3://x/y"]},
        is_replace=True,
        time_column="ts",
        segment_granularity="DAY",
        max_concurrent_subtasks=2,
        max_rows_per_segment=1_000_000,
        max_rows_in_memory=500_000,
    )
    g = spec["spec"]["dataSchema"]["granularitySpec"]
    assert g["intervals"] == ["1970-01-01/2100-01-01"]
    assert spec["spec"]["ioConfig"]["dropExisting"] is True


def test_build_input_source_s3():
    src = impl.build_input_source(
        _s3_staging(),
        ["s3://staging-bucket/staging/part-0.parquet"],
    )
    assert src["type"] == "s3"
    assert src["uris"] == ["s3://staging-bucket/staging/part-0.parquet"]
    assert src["endpointConfig"]["signingRegion"] == "us-east-1"
    assert src["properties"]["accessKeyId"] == "AKIA..."
    assert src["properties"]["secretAccessKey"] == "secret"


def test_build_input_source_gcs_native_no_credentials():
    src = impl.build_input_source(
        _gcs_staging(),
        ["gs://staging-bucket/staging/part-0.parquet"],
    )
    assert src["type"] == "google"
    assert src["uris"] == ["gs://staging-bucket/staging/part-0.parquet"]
    assert "properties" not in src  # native google input source


def test_build_input_source_azure():
    src = impl.build_input_source(
        _az_staging(),
        ["azureStorage://acct/td-stage/staging/part-0.parquet"],
    )
    assert src["type"] == "azureStorage"
    assert src["uris"] == [
        "azureStorage://acct/td-stage/staging/part-0.parquet"
    ]
    assert src["properties"]["key"] == "key"


# ---------------------------------------------------------------------------
# Adapter helpers -- staged_uri / transporter target / env
# ---------------------------------------------------------------------------


def test_staged_uri_for_each_flavor():
    assert impl.staged_uri(_s3_staging(), "staging/x.parquet") == (
        "s3://staging-bucket/staging/x.parquet"
    )
    assert impl.staged_uri(_gcs_staging(), "staging/x.parquet") == (
        "gs://staging-bucket/staging/x.parquet"
    )
    assert impl.staged_uri(_az_staging(), "staging/x.parquet") == (
        "azureStorage://acct/td-stage/staging/x.parquet"
    )


def test_transporter_env_s3():
    # noinspection PyProtectedMember
    env = impl._transporter_env(_s3_staging())
    assert env[_transporter.SERVER_SIDE_AWS_ACCESS_KEY_ID] == "AKIA..."
    assert env[_transporter.SERVER_SIDE_AWS_SECRET_ACCESS_KEY] == "secret"
    assert env[_transporter.SERVER_SIDE_AWS_REGION] == "us-east-1"


def test_transporter_env_gcs():
    # noinspection PyProtectedMember
    env = impl._transporter_env(_gcs_staging())
    assert _transporter.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON in env


def test_transporter_env_azure():
    # noinspection PyProtectedMember
    env = impl._transporter_env(_az_staging())
    assert env[_transporter.SERVER_SIDE_AZURE_ACCOUNT_NAME] == "acct"
    assert env[_transporter.SERVER_SIDE_AZURE_ACCOUNT_KEY] == "key"


# ---------------------------------------------------------------------------
# Adapter helpers -- URL sanitization
# ---------------------------------------------------------------------------


def test_sanitize_url_strips_credentials():
    # noinspection PyProtectedMember
    assert impl._sanitize_url("http://u:p@host:8081/path") == (
        "http://***:***@host:8081/path"
    )
    # noinspection PyProtectedMember
    assert impl._sanitize_url("http://host:8081/path") == (
        "http://host:8081/path"
    )


# ---------------------------------------------------------------------------
# OverlordClient -- submit / status / wait via mocked requests
# ---------------------------------------------------------------------------


def _client_with_mocked_session(*, post=None, get=None) -> impl.OverlordClient:
    client = impl.OverlordClient(
        base_url="http://druid.example.com:8081",
        auth=("u", "p"),
        timeout_sec=5,
        poll_interval_s=0.0,
        retry_attempts=1,
        retry_backoff=0.0,
    )
    sess = MagicMock()
    if post is not None:
        sess.post.return_value = post
    if get is not None:
        sess.get.return_value = get
    # noinspection PyProtectedMember
    client._session = sess
    return client


def _resp(json_payload=None, status=200, raise_for_status_exc=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_payload if json_payload is not None else {}
    r.text = str(json_payload) if json_payload is not None else ""
    if raise_for_status_exc is not None:
        r.raise_for_status.side_effect = raise_for_status_exc
    else:
        r.raise_for_status.return_value = None
    return r


def test_overlord_submit_task_returns_task_id():
    client = _client_with_mocked_session(
        post=_resp({"task": "task-abc"}),
    )
    assert client.submit_task({"type": "index_parallel"}) == "task-abc"


def test_overlord_submit_task_missing_field_raises_druid_10():
    client = _client_with_mocked_session(post=_resp({"not_task": "?"}))
    with pytest.raises(DruidRuntimeException) as ei:
        client.submit_task({"type": "index_parallel"})
    assert ei.value.api_error is DruidErrorCode.DRUID_10
    assert "task" in ei.value.extra["reason"]


def test_overlord_submit_task_non_json_raises_druid_10():
    bad = _resp()
    bad.json.side_effect = ValueError("not json")
    client = _client_with_mocked_session(post=bad)
    with pytest.raises(DruidRuntimeException) as ei:
        client.submit_task({"type": "index_parallel"})
    assert ei.value.api_error is DruidErrorCode.DRUID_10


def test_overlord_task_status_returns_status_code():
    client = _client_with_mocked_session(
        get=_resp({"status": {"statusCode": "SUCCESS"}}),
    )
    status, _payload = client.task_status("task-1")
    assert status == "SUCCESS"


def test_overlord_wait_for_completion_success_returns(monkeypatch):
    client = _client_with_mocked_session(
        get=_resp({"status": {"statusCode": "SUCCESS"}}),
    )
    # Should return silently.
    client.wait_for_completion("task-1")


def test_overlord_wait_for_completion_failed_raises_druid_9(monkeypatch):
    # First call returns FAILED on /status; the second call (reports)
    # returns an empty body to exercise the best-effort error path.
    sess = MagicMock()
    sess.get.side_effect = [
        _resp({"status": {"statusCode": "FAILED"}}),
        _resp({}),
    ]
    client = impl.OverlordClient(
        base_url="http://druid.example.com:8081",
        auth=("u", "p"),
        poll_interval_s=0.0,
        retry_attempts=1,
        retry_backoff=0.0,
    )
    # noinspection PyProtectedMember
    client._session = sess
    with pytest.raises(DruidRuntimeException) as ei:
        client.wait_for_completion("task-1")
    assert ei.value.api_error is DruidErrorCode.DRUID_9


# ---------------------------------------------------------------------------
# Explorer stubs + DestDef wiring
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    exp = plugin.DruidExplorer()
    assert exp.metadata(_conn()) is None
    assert exp.data(_conn(), "SELECT 1") is None


def test_druid_dest_def_wires_plugin_and_explorer():
    assert plugin.DRUID_DEST.conn is DruidConn
    assert plugin.DRUID_DEST.dest is DruidDest
    assert plugin.DRUID_DEST.plugin is plugin.DruidDestPlugin
    assert plugin.DRUID_DEST.explorer is plugin.DruidExplorer
    assert plugin.DRUID_DEST.dest_type == "druid"


def test_druid_dest_cardinality_uses_tables_length():
    dest = DruidDest(conn=_conn(), tables=["events", "metrics", "logs"])
    assert plugin.DRUID_DEST.cardinality(dest) == 3


# ---------------------------------------------------------------------------
# _resolve_dest_cfg
# ---------------------------------------------------------------------------


def test_resolve_dest_cfg_returns_defaults_when_empty():
    import logging as _logging
    cfg = plugin._resolve_dest_cfg(None)
    assert cfg.logging_level == _logging.ERROR
    assert cfg.poll_interval_seconds is None
    assert cfg.retry_attempts is None
    assert cfg.retry_backoff_seconds is None


def test_resolve_dest_cfg_applies_overrides():
    cfg = plugin._resolve_dest_cfg(
        {
            "druid.logging_level": 20,
            "druid.poll_interval_seconds": 5.0,
            "druid.retry_attempts": 4,
            "druid.retry_backoff_seconds": 1.5,
        }
    )
    assert cfg.logging_level == 20
    assert cfg.poll_interval_seconds == 5.0
    assert cfg.retry_attempts == 4
    assert cfg.retry_backoff_seconds == 1.5


def test_resolve_dest_cfg_warns_on_unknown_key_via_raw_dict(caplog):
    with caplog.at_level("WARNING", logger=plugin.logger.name):
        plugin._resolve_dest_cfg({"bogus.key": "value"})
    assert any("Unknown dest_cfg key" in r.message for r in caplog.records)
