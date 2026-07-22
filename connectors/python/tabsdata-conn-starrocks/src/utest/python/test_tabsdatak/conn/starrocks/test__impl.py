#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AzureAccountKey,
    GCPHmac,
    GCPJson,
    UserPassword,
)
from tabsdatak.conn.starrocks import (
    AzBlobLocation,
    GCSLocation,
    S3Location,
)

# noinspection PyProtectedMember
from tabsdatak.common._transporter.handler.handler1 import TransporterHandler1
from tabsdatak.conn.starrocks import _impl as impl
from tabsdatak.conn.starrocks.error import (
    StarRocksErrorCode,
    StarRocksRuntimeException,
)


pytestmark = pytest.mark.starrocks

HANDLER = TransporterHandler1()


# ---------------------------------------------------------------------------
# build_starrocks_engine
# ---------------------------------------------------------------------------


def _credentials() -> UserPassword:
    return UserPassword(
        user=Secret("etl"),
        password=Secret("hunter2"),
    )


def test_build_starrocks_engine_uses_mysqlconnector_driver(monkeypatch):
    box: dict = {}

    def fake_create_engine(url, *, connect_args):
        box["url"] = url
        box["connect_args"] = connect_args
        return MagicMock(name="fake-engine")

    import sqlalchemy
    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine)

    impl.build_starrocks_engine(
        host="sr.example.com",
        port=9030,
        database="analytics",
        credentials=_credentials(),
    )
    assert box["url"].startswith(
        "mysql+mysqlconnector://etl:hunter2@sr.example.com:9030/analytics",
    )
    assert box["connect_args"] == {}


def test_build_starrocks_engine_forwards_connect_args(monkeypatch):
    box: dict = {}

    def fake_create_engine(url, *, connect_args):
        box["connect_args"] = connect_args
        return MagicMock()

    import sqlalchemy
    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine)

    impl.build_starrocks_engine(
        host="h", port=9030, database="d",
        credentials=_credentials(),
        connect_args={"connection_timeout": 5},
    )
    assert box["connect_args"] == {"connection_timeout": 5}


# ---------------------------------------------------------------------------
# build_files_function -- S3
# ---------------------------------------------------------------------------


def _s3_staging() -> S3Location:
    return S3Location(
        bucket="stg",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret("AKIA..."),
            secret_access_key=Secret("secret"),
        ),
        base_path="/prefix",
    )


def test_build_files_function_s3_single_uri_passes_through():
    sql = impl.build_files_function(
        _s3_staging(),
        ["s3://stg/prefix/up1/t/a.parquet"],
    )
    assert sql.startswith("FILES(")
    assert '"path" = "s3://stg/prefix/up1/t/a.parquet"' in sql
    assert '"format" = "parquet"' in sql
    assert '"aws.s3.access_key" = "AKIA..."' in sql
    assert '"aws.s3.secret_key" = "secret"' in sql
    assert '"aws.s3.region" = "us-east-1"' in sql


def test_build_files_function_s3_multiple_uris_collapse_to_wildcard():
    sql = impl.build_files_function(
        _s3_staging(),
        [
            "s3://stg/prefix/up1/t/a.parquet",
            "s3://stg/prefix/up1/t/b.parquet",
        ],
    )
    assert '"path" = "s3://stg/prefix/up1/t/*"' in sql


# ---------------------------------------------------------------------------
# build_files_function -- Azure Blob
# ---------------------------------------------------------------------------


def _az_staging() -> AzBlobLocation:
    return AzBlobLocation(
        container="ctr",
        credentials=AzureAccountKey(
            account_name=Secret("acct"),
            account_key=Secret("azkey"),
        ),
        base_path="/prefix",
    )


def test_build_files_function_azure():
    sql = impl.build_files_function(
        _az_staging(),
        ["wasbs://ctr@acct.blob.core.windows.net/prefix/up1/t/a.parquet"],
    )
    assert '"format" = "parquet"' in sql
    assert '"azure.blob.shared_key" = "azkey"' in sql
    assert "wasbs://ctr@acct.blob.core.windows.net/prefix/up1/t/a.parquet" in sql


# ---------------------------------------------------------------------------
# build_files_function -- GCS (S3-compatible endpoint via HMAC)
# ---------------------------------------------------------------------------


def _gcs_staging() -> GCSLocation:
    return GCSLocation(
        bucket="gbk",
        credentials=GCPJson(json=Secret("{}")),
        hmac_credentials=GCPHmac(
            access_key=Secret("GOOG1..."),
            secret_key=Secret("goog-secret"),
        ),
        base_path="/prefix",
    )


def test_build_files_function_gcs_uses_hmac_and_endpoint():
    sql = impl.build_files_function(
        _gcs_staging(),
        ["s3a://gbk/prefix/up1/t/a.parquet"],
    )
    assert sql.startswith("FILES(")
    assert '"path" = "s3a://gbk/prefix/up1/t/a.parquet"' in sql
    assert '"format" = "parquet"' in sql
    assert '"aws.s3.access_key" = "GOOG1..."' in sql
    assert '"aws.s3.secret_key" = "goog-secret"' in sql
    assert '"aws.s3.endpoint" = "https://storage.googleapis.com"' in sql


# ---------------------------------------------------------------------------
# build_files_function -- unknown staging type
# ---------------------------------------------------------------------------


def test_build_files_function_unknown_staging_type_raises_starrocks_7():
    class _BogusLoc:
        __kind__ = "bogus"

    with pytest.raises(StarRocksRuntimeException) as ei:
        impl.build_files_function(_BogusLoc(), ["x"])
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_7


# ---------------------------------------------------------------------------
# staged_uri -- URI shaping per cloud backend
# ---------------------------------------------------------------------------


def test_staged_uri_s3():
    assert (
        impl.staged_uri(_s3_staging(), "up1/t/a.parquet")
        == "s3://stg/prefix/up1/t/a.parquet"
    )


def test_staged_uri_azure():
    assert impl.staged_uri(
        _az_staging(), "up1/t/a.parquet",
    ) == "wasbs://ctr@acct.blob.core.windows.net/prefix/up1/t/a.parquet"


def test_staged_uri_gcs_uses_s3a_scheme():
    # GCS staging URIs use ``s3a://`` so StarRocks' ``FILES()`` can read
    # via the S3-compatible HMAC layer.
    assert impl.staged_uri(
        _gcs_staging(), "up1/t/a.parquet",
    ) == "s3a://gbk/prefix/up1/t/a.parquet"


# ---------------------------------------------------------------------------
# _qualify -- table-name handling
# ---------------------------------------------------------------------------


def test_qualify_bare_name_uses_default_database():
    assert impl._qualify("analytics", "users") == "`analytics`.`users`"


def test_qualify_dotted_name_overrides_default():
    assert impl._qualify("ignored", "warehouse.users") == "`warehouse`.`users`"


def test_qualify_preserves_existing_backtick_quotes():
    assert impl._qualify("d", "`a`.`b`") == "`a`.`b`"


# _resolve -- removed in favour of _convert; covered by
# test_types.test_convert_* and the per-staging tests below.


# ---------------------------------------------------------------------------
# transporter_env -- credential resolution per staging type
# ---------------------------------------------------------------------------


def test_transporter_env_s3():
    env = impl.transporter_env(_s3_staging(), HANDLER)
    assert env["LOCATION_AWS_ACCESS_KEY_ID"] == "AKIA..."
    assert env["LOCATION_AWS_SECRET_ACCESS_KEY"] == "secret"
    assert env["LOCATION_AWS_REGION"] == "us-east-1"


def test_transporter_env_azure():
    env = impl.transporter_env(_az_staging(), HANDLER)
    assert env["LOCATION_ACCOUNT_NAME"] == "acct"
    assert env["LOCATION_ACCOUNT_KEY"] == "azkey"


def test_transporter_env_gcs_uses_json_credentials():
    env = impl.transporter_env(_gcs_staging(), HANDLER)
    assert env["LOCATION_GCS_SERVICE_ACCOUNT_JSON"] == "{}"


# ---------------------------------------------------------------------------
# StarRocksWriteCfg.execute -- orchestration smoke test
# ---------------------------------------------------------------------------


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


def _make_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


def test_execute_uploads_then_inserts(monkeypatch, work_dir):
    monkeypatch.setattr(
        impl,
        "upload_parquet",
        lambda *a, **kw: "s3://stg/prefix/up1/t/x.parquet",
    )

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt):
            executed.append(str(stmt))

    class _BeginCtx:
        def __enter__(self):
            return _Conn()

        def __exit__(self, *a):
            return False

    engine = MagicMock()
    engine.begin.side_effect = lambda: _BeginCtx()
    monkeypatch.setattr(impl, "build_starrocks_engine", lambda **kw: engine)

    cfg = impl.StarRocksWriteCfg(
        host="h", port=9030, database="db",
        credentials=_credentials(),
        staging=_s3_staging(),
        target_tables=["users", "orders"],
    )
    parquet1 = _make_parquet(work_dir / "u.parquet", [{"id": 1}])
    parquet2 = _make_parquet(work_dir / "o.parquet", [{"id": 1}])
    impl.execute(cfg, [parquet1, parquet2], work_dir, upload_id="up1")

    assert len(executed) == 2
    assert executed[0].startswith("INSERT INTO `db`.`users`")
    assert executed[1].startswith("INSERT INTO `db`.`orders`")
    engine.dispose.assert_called()


def test_execute_rejects_unknown_if_table_exists(work_dir):
    cfg = impl.StarRocksWriteCfg(
        host="h", port=9030, database="db",
        credentials=_credentials(),
        staging=_s3_staging(),
        target_tables=["t"],
        if_table_exists="invalid",
    )
    parquet = _make_parquet(work_dir / "x.parquet", [{"id": 1}])
    with pytest.raises(ValueError):
        impl.execute(cfg, [parquet], work_dir, upload_id="up1")


# ---------------------------------------------------------------------------
# Retry policy -- transient errors only
# ---------------------------------------------------------------------------


def test_transient_predicate_matches_backends_unavailable():
    exc = Exception(
        "(mysql.connector.errors.DatabaseError) 1064 (HY000): "
        "Current available backends: []"
    )
    assert impl._is_transient_starrocks_error(exc) is True


def test_transient_predicate_rejects_table_not_found():
    exc = Exception(
        "1064 (HY000): Getting analyzing error. "
        "Detail message: Table wld_country_s3 is not found."
    )
    assert impl._is_transient_starrocks_error(exc) is False


def test_execute_does_not_retry_on_non_transient_error(monkeypatch, work_dir):
    # A deterministic failure (e.g. a missing target table) must surface on the
    # first attempt rather than being retried for the full timeout.
    monkeypatch.setattr(
        impl,
        "upload_parquet",
        lambda *a, **kw: "s3://stg/prefix/up1/t/x.parquet",
    )
    attempts = {"n": 0}

    class _Conn:
        def execute(self, stmt):
            attempts["n"] += 1
            raise RuntimeError("Table `t` is not found")

    class _BeginCtx:
        def __enter__(self):
            return _Conn()

        def __exit__(self, *a):
            return False

    engine = MagicMock()
    engine.begin.side_effect = lambda: _BeginCtx()
    monkeypatch.setattr(impl, "build_starrocks_engine", lambda **kw: engine)

    cfg = impl.StarRocksWriteCfg(
        host="h", port=9030, database="db",
        credentials=_credentials(),
        staging=_s3_staging(),
        target_tables=["t"],
        if_table_exists="replace",
    )
    parquet = _make_parquet(work_dir / "x.parquet", [{"id": 1}])
    with pytest.raises(RuntimeError, match="is not found"):
        impl.execute(cfg, [parquet], work_dir, upload_id="up1")
    assert attempts["n"] == 1
