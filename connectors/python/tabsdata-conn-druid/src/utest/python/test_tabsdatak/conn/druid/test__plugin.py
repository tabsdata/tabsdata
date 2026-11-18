#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the Druid plugin -- fault-injection via mocks.

The real-Druid happy paths live in ``test__plugin_integration.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AzureAccountKey,
    UserPassword,
)

# noinspection PyProtectedMember
from tabsdatak.conn.druid import (
    AzBlobLocation,
    DruidDest,
    DruidDestConn,
    S3Location,
    _impl as impl,
    _plugin as plugin,
)
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

    @property
    def work_dir(self) -> Path:
        return self._work

    @property
    def execution_id(self) -> str:
        return "plan-unit"

    @property
    def execution(self) -> str | None:
        return None

    @property
    def transaction_id(self) -> str:
        return self._trx

    @property
    def logical_function_run_id(self) -> str:
        return "test-logical-function-run-id"

    @property
    def physical_function_run_id(self) -> str:
        return "test-physical-function-run-id"

    @property
    def triggered_on(self) -> datetime:
        return datetime.fromisoformat("2026-01-01T00:00:00.000000+00:00")


def _dest_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


# ---------------------------------------------------------------------------
# Builders for the test connection / destination
# ---------------------------------------------------------------------------


def _s3_staging() -> S3Location:
    return S3Location(
        bucket="staging-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret("AKIA..."),
            secret_access_key=Secret("secret"),
        ),
        base_path="/staging",
    )


def _az_staging() -> AzBlobLocation:
    return AzBlobLocation(
        container="td-stage",
        credentials=AzureAccountKey(
            account_name=Secret("acct"),
            account_key=Secret("key"),
        ),
        base_path="/staging",
    )


def _conn(staging=None) -> DruidDestConn:
    return DruidDestConn(
        overlord_url="http://druid.example.com:8081",
        staging=staging or _s3_staging(),
        credentials=UserPassword(
            user=Secret("etl"),
            password=Secret("hunter2"),
        ),
    )


# ---------------------------------------------------------------------------
# DestPlugin.write_out -- defensive recheck (DRUID_6)
# ---------------------------------------------------------------------------


def test_dest_plugin_raises_druid_6_on_tables_slots_mismatch(work_dir):
    dest = DruidDest(tables=["events", "metrics"])
    with pytest.raises(DruidRuntimeException) as ei:
        plugin.DruidDestPlugin().write_out(
            _dest_ctx(work_dir),
            _conn(),
            dest,
            [ROWS],
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_6


ROWS = pl.LazyFrame({"id": [1]})


@dataclass
class Staging:
    staged: list[tuple[str, str]]
    discarded: list[str]


def _staging(monkeypatch, fail: Exception | None = None) -> Staging:
    record = Staging([], [])

    def stage(staging, store, frame, subdir, name):
        if fail is not None:
            raise fail
        record.staged.append((subdir, name))
        key = f"staging/{subdir}/{name}"
        return key, f"s3://staging-bucket/{key}"

    monkeypatch.setattr(impl, "stage", stage)
    monkeypatch.setattr(
        impl, "discard", lambda store, keys: record.discarded.extend(keys)
    )
    return record


def _client(monkeypatch, *, submit=None, wait=None) -> MagicMock:
    fake_client = MagicMock()
    fake_client.submit_task.side_effect = submit or (lambda spec: "task-1")
    fake_client.wait_for_completion.side_effect = wait
    monkeypatch.setattr(
        impl,
        "make_overlord_client_for_dest",
        lambda conn, dest: fake_client,
    )
    return fake_client


# ---------------------------------------------------------------------------
# DestPlugin.write_out -- staging failure (DRUID_7)
# ---------------------------------------------------------------------------


def test_dest_plugin_wraps_a_staging_failure_as_druid_7(monkeypatch, work_dir):
    record = _staging(monkeypatch, fail=OSError("bucket unreachable"))
    _client(monkeypatch)
    with pytest.raises(DruidRuntimeException) as ei:
        plugin.DruidDestPlugin().write_out(
            _dest_ctx(work_dir), _conn(), DruidDest(tables=["events"]), [ROWS]
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_7
    assert record.discarded == []


# ---------------------------------------------------------------------------
# DestPlugin.write_out -- DRUID_8 (submit raises non-ApiException)
# ---------------------------------------------------------------------------


def test_dest_plugin_wraps_submit_failure_as_druid_8_keeping_the_file(
    monkeypatch,
    work_dir,
):
    record = _staging(monkeypatch)

    def submit(spec):
        raise RuntimeError("network down")

    _client(monkeypatch, submit=submit)
    with pytest.raises(DruidRuntimeException) as ei:
        plugin.DruidDestPlugin().write_out(
            _dest_ctx(work_dir), _conn(), DruidDest(tables=["events"]), [ROWS]
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_8
    # The URL embedded in the error is the safe-sanitized form.
    assert "druid.example.com" in ei.value.extra["url"]
    assert record.discarded == []


# ---------------------------------------------------------------------------
# DestPlugin.write_out -- DRUID_9 (task FAILED)
# ---------------------------------------------------------------------------


def test_dest_plugin_wraps_task_failure_as_druid_9_and_discards(monkeypatch, work_dir):
    record = _staging(monkeypatch)

    def _wait(_task_id):
        # noinspection PyProtectedMember
        raise DruidErrorCode.DRUID_9.exception(
            datasource="?",
            task_id=_task_id,
            reason="bad parquet",
        )

    _client(monkeypatch, wait=_wait)
    with pytest.raises(DruidRuntimeException) as ei:
        plugin.DruidDestPlugin().write_out(
            _dest_ctx(work_dir), _conn(), DruidDest(tables=["events"]), [ROWS]
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_9
    # The datasource is hydrated from the dest's tables -- the
    # original placeholder ("?") must be replaced.
    assert ei.value.extra["datasource"] == "events"
    assert ei.value.extra["task_id"] == "task-1"
    assert "bad parquet" in ei.value.extra["reason"]
    assert record.discarded == [f"staging/{record.staged[0][0]}/{record.staged[0][1]}"]


def test_dest_plugin_keeps_the_file_when_the_task_state_is_unknown(
    monkeypatch, work_dir
):
    record = _staging(monkeypatch)

    def _wait(_task_id):
        # noinspection PyProtectedMember
        raise DruidErrorCode.DRUID_10.exception(
            url="http://druid.example.com:8081", reason="status unreachable"
        )

    _client(monkeypatch, wait=_wait)
    with pytest.raises(DruidRuntimeException) as ei:
        plugin.DruidDestPlugin().write_out(
            _dest_ctx(work_dir), _conn(), DruidDest(tables=["events"]), [ROWS]
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_10
    assert record.discarded == []


# ---------------------------------------------------------------------------
# DestPlugin.write_out -- happy path with a mocked client
# ---------------------------------------------------------------------------


def test_dest_plugin_submits_one_task_per_table(monkeypatch, work_dir):
    record = _staging(monkeypatch)
    specs: list[dict] = []

    def _fake_submit(spec: dict) -> str:
        specs.append(spec)
        return f"task-{len(specs)}"

    _client(monkeypatch, submit=_fake_submit)
    dest = DruidDest(
        tables=["events", "metrics"],
        if_table_exists="replace",
        time_column="ts",
        segment_granularity="HOUR",
        max_concurrent_subtasks=4,
    )
    plugin.DruidDestPlugin().write_out(
        _dest_ctx(work_dir),
        _conn(),
        dest,
        [ROWS, pl.LazyFrame({"id": [2]})],
    )

    # One ingestion task per target datasource.
    assert [s["spec"]["dataSchema"]["dataSource"] for s in specs] == [
        "events",
        "metrics",
    ]
    # All specs honour replace semantics + tuning knobs.
    for s in specs:
        assert s["spec"]["ioConfig"]["dropExisting"] is True
        assert s["spec"]["ioConfig"]["appendToExisting"] is False
        assert s["spec"]["dataSchema"]["timestampSpec"]["column"] == "ts"
        assert (
            s["spec"]["dataSchema"]["granularitySpec"]["segmentGranularity"] == "HOUR"
        )
        assert s["spec"]["tuningConfig"]["maxNumConcurrentSubTasks"] == 4
    # Both stagings share the transaction's subdir root, one slot each.
    subdirs = {subdir for subdir, _ in record.staged}
    assert len(subdirs) == 2
    assert all(s.startswith("td-druid/trx-unit/") for s in subdirs)
    assert [name for _, name in record.staged] == [
        "part-00000-0.parquet",
        "part-00000-1.parquet",
    ]
    assert len(record.discarded) == 2


def test_dest_plugin_leaves_a_none_datasource_untouched(monkeypatch, work_dir):
    record = _staging(monkeypatch)
    specs: list[dict] = []

    def _fake_submit(spec: dict) -> str:
        specs.append(spec)
        return f"task-{len(specs)}"

    _client(monkeypatch, submit=_fake_submit)
    dest = DruidDest(tables=["events", "metrics"], if_table_exists="replace")
    plugin.DruidDestPlugin().write_out(_dest_ctx(work_dir), _conn(), dest, [None, ROWS])

    assert [s["spec"]["dataSchema"]["dataSource"] for s in specs] == ["metrics"]
    assert len(record.staged) == 1
    assert len(record.discarded) == 1


def test_dest_plugin_writes_nothing_when_every_table_is_none(monkeypatch, work_dir):
    record = _staging(monkeypatch)
    make_client = MagicMock()
    monkeypatch.setattr(impl, "make_overlord_client_for_dest", make_client)

    plugin.DruidDestPlugin().write_out(
        _dest_ctx(work_dir),
        _conn(),
        DruidDest(tables=["events", "metrics"]),
        [None, None],
    )

    make_client.assert_not_called()
    assert record.staged == []


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


def test_build_input_source_azure():
    src = impl.build_input_source(
        _az_staging(),
        ["azureStorage://acct/td-stage/staging/part-0.parquet"],
    )
    assert src["type"] == "azureStorage"
    assert src["uris"] == ["azureStorage://acct/td-stage/staging/part-0.parquet"]
    assert src["properties"]["key"] == "key"


# ---------------------------------------------------------------------------
# Adapter helpers -- staged_uri / staging store
# ---------------------------------------------------------------------------


def test_staged_uri_for_each_flavor():
    assert impl.staged_uri(_s3_staging(), "staging/x.parquet") == (
        "s3://staging-bucket/staging/x.parquet"
    )
    assert impl.staged_uri(_az_staging(), "staging/x.parquet") == (
        "azureStorage://acct/td-stage/staging/x.parquet"
    )


def test_staging_store_of_s3_is_its_bucket_with_credentials():
    staging = S3Location(
        bucket="staging-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret("AKIA..."),
            secret_access_key=Secret("secret"),
        ),
        cfg={"obstore.endpoint": "http://127.0.0.1:9000"},
    )
    store = impl.staging_store(staging)
    assert store.root == "s3://staging-bucket"
    assert store.options == {
        "aws_access_key_id": "AKIA...",
        "aws_secret_access_key": "secret",
        "aws_region": "us-east-1",
        "aws_endpoint": "http://127.0.0.1:9000",
    }


def test_staging_store_of_azure_is_its_container_with_credentials():
    store = impl.staging_store(_az_staging())
    assert store.root == "az://td-stage"
    assert store.options == {
        "azure_storage_account_name": "acct",
        "azure_storage_account_key": "key",
    }


def test_stage_writes_the_frame_under_the_base_path(tmp_path):
    store = FileStore(tmp_path.as_uri())
    key, uri = impl.stage(_s3_staging(), store, ROWS, "td-druid/trx/x", "p.parquet")
    assert key == "staging/td-druid/trx/x/p.parquet"
    assert uri == "s3://staging-bucket/staging/td-druid/trx/x/p.parquet"
    assert pl.read_parquet(tmp_path / key).to_dicts() == [{"id": 1}]


def test_discard_never_fails(tmp_path, caplog):
    class Broken:
        def delete(self, keys):
            raise OSError("denied")

    impl.discard(Broken(), ["a"])
    assert "staged files remain" in caplog.text


# ---------------------------------------------------------------------------
# Adapter helpers -- URL sanitization
# ---------------------------------------------------------------------------


def test_sanitize_url_strips_credentials():
    # noinspection PyProtectedMember
    assert impl._sanitize_url("http://u:p@host:8081/path") == (
        "http://***:***@host:8081/path"
    )
    # noinspection PyProtectedMember
    assert impl._sanitize_url("http://host:8081/path") == ("http://host:8081/path")


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
# DestDef wiring
# ---------------------------------------------------------------------------


def test_druid_dest_def_wires_plugin_and_explorer():
    assert plugin.DRUID_DEST.conn is DruidDestConn
    assert plugin.DRUID_DEST.dest is DruidDest
    assert plugin.DRUID_DEST.plugin is plugin.DruidDestPlugin
    assert plugin.DRUID_DEST.explorer is plugin.DruidConnectionExplorer
    assert plugin.DRUID_DEST.type_ == "druid-bulk-out"


def test_druid_dest_cardinality_uses_tables_length():
    dest = DruidDest(tables=["events", "metrics", "logs"])
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
    cfg = plugin._resolve_dest_cfg({
        "druid.logging_level": 20,
        "druid.poll_interval_seconds": 5.0,
        "druid.retry_attempts": 4,
        "druid.retry_backoff_seconds": 1.5,
    })
    assert cfg.logging_level == 20
    assert cfg.poll_interval_seconds == 5.0
    assert cfg.retry_attempts == 4
    assert cfg.retry_backoff_seconds == 1.5


def test_resolve_dest_cfg_warns_on_unknown_key_via_raw_dict(caplog):
    plugin._resolve_dest_cfg({"bogus.key": "value"})
    assert any("Unknown dest_cfg key" in r.getMessage() for r in caplog.records)
