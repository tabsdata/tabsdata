#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.file._store import FileStore
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
from tabsdatak.conn.common.sql._engine import sqlalchemy_utils

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import _impl as impl
from tabsdatak.conn.starrocks.error import (
    StarRocksErrorCode,
    StarRocksRuntimeException,
)


pytestmark = pytest.mark.starrocks


@pytest.fixture(autouse=True)
def _evolutions(monkeypatch):
    seen: list[tuple[str, str]] = []

    class _Evolver:
        def __init__(self, conn, database, table):
            pass

        def evolve(self, target, files_fn, evolution, footer=None):
            seen.append((target, evolution))
            return []

    monkeypatch.setattr(impl, "StarRocksSchemaEvolver", _Evolver)
    return seen



# ---------------------------------------------------------------------------
# build_starrocks_engine
# ---------------------------------------------------------------------------


def _credentials() -> UserPassword:
    return UserPassword(
        user=Secret("etl"),
        password=Secret("hunter2"),
    )


def test_build_starrocks_engine_uses_pymysql_driver(monkeypatch):
    box: dict = {}

    def fake_create_engine(url, *, connect_args):
        box["url"] = url
        box["connect_args"] = connect_args
        return MagicMock(name="fake-engine")

    monkeypatch.setattr(sqlalchemy_utils, "create_engine", fake_create_engine)

    impl.build_starrocks_engine(
        host="sr.example.com",
        port=9030,
        database="analytics",
        credentials=_credentials(),
    )
    assert box["url"].startswith(
        "mysql+pymysql://etl:hunter2@sr.example.com:9030/analytics",
    )
    assert box["connect_args"] == {}


def test_build_starrocks_engine_forwards_connect_args(monkeypatch):
    box: dict = {}

    def fake_create_engine(url, *, connect_args):
        box["connect_args"] = connect_args
        return MagicMock()

    monkeypatch.setattr(sqlalchemy_utils, "create_engine", fake_create_engine)

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
# staging_store / stage / discard
# ---------------------------------------------------------------------------


def test_staging_store_of_s3_is_its_bucket_with_credentials():
    store = impl.staging_store(_s3_staging())
    assert store.root.startswith("s3://")
    assert store.options["aws_access_key_id"] == "AKIA..."
    assert store.options["aws_secret_access_key"] == "secret"
    assert store.options["aws_region"] == "us-east-1"


def test_staging_store_of_azure_is_its_container_with_credentials():
    store = impl.staging_store(_az_staging())
    assert store.root.startswith("az://")
    assert store.options == {
        "azure_storage_account_name": "acct",
        "azure_storage_account_key": "azkey",
    }


def test_staging_store_of_gcs_uploads_with_the_json_key():
    store = impl.staging_store(_gcs_staging())
    assert store.root.startswith("gs://")
    assert store.options == {"google_service_account_key": "{}"}


def test_stage_writes_the_frame_under_the_base_path(tmp_path):
    store = FileStore(tmp_path.as_uri())
    staging = _s3_staging()
    key, uri = impl.stage(staging, store, pl.LazyFrame({"id": [1]}), "up/t/0.parquet")
    assert uri == impl.staged_uri(staging, "up/t/0.parquet")
    assert key.endswith("up/t/0.parquet")
    assert pl.read_parquet(tmp_path / key).to_dicts() == [{"id": 1}]


def test_discard_never_fails(caplog):
    class Broken:
        def delete(self, keys):
            raise OSError("denied")

    impl.discard(Broken(), ["a"])
    assert "staged files remain" in caplog.text


# ---------------------------------------------------------------------------
# StarRocksWriteCfg.execute -- orchestration smoke test
# ---------------------------------------------------------------------------


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


ROWS = pl.LazyFrame({"id": [1]})


def _staging(monkeypatch, fail: Exception | None = None) -> list[str]:
    discarded: list[str] = []

    def stage(staging, store, frame, rel):
        if fail is not None:
            raise fail
        return f"prefix/{rel}", f"s3://stg/prefix/{rel}"

    monkeypatch.setattr(impl, "stage", stage)
    monkeypatch.setattr(impl, "discard", lambda store, keys: discarded.extend(keys))
    # Nothing is staged, so there is no footer to read.
    monkeypatch.setattr(impl, "_footer", lambda store, key: None)
    return discarded


def test_execute_uploads_then_inserts(monkeypatch, work_dir):
    discarded = _staging(monkeypatch)

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
    impl.execute(cfg, [ROWS, ROWS], upload_id="up1")

    assert len(executed) == 2
    assert executed[0].startswith("INSERT INTO `db`.`users` (`id`)")
    assert executed[1].startswith("INSERT INTO `db`.`orders` (`id`)")
    assert discarded == ["prefix/up1/users/0.parquet", "prefix/up1/orders/1.parquet"]
    engine.dispose.assert_called()


def test_execute_rejects_unknown_if_table_exists(work_dir):
    cfg = impl.StarRocksWriteCfg(
        host="h", port=9030, database="db",
        credentials=_credentials(),
        staging=_s3_staging(),
        target_tables=["t"],
        if_table_exists="invalid",
    )
    with pytest.raises(ValueError):
        impl.execute(cfg, [ROWS], upload_id="up1")


# ---------------------------------------------------------------------------
# Retry policy -- transient errors only
# ---------------------------------------------------------------------------


def test_transient_predicate_matches_backends_unavailable():
    exc = Exception(
        "(pymysql.err.ProgrammingError) (1064, "
        "'Current available backends: []')"
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
    discarded = _staging(monkeypatch)
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
    with pytest.raises(RuntimeError, match="is not found"):
        impl.execute(cfg, [ROWS], upload_id="up1")
    assert attempts["n"] == 1
    assert discarded == ["prefix/up1/t/0.parquet"]


def test_a_staging_failure_is_a_write_failure_of_its_table(monkeypatch):
    _staging(monkeypatch, fail=OSError("bucket unreachable"))
    monkeypatch.setattr(impl, "build_starrocks_engine", lambda **kw: MagicMock())
    cfg = impl.StarRocksWriteCfg(
        host="h", port=9030, database="db",
        credentials=_credentials(),
        staging=_s3_staging(),
        target_tables=["t"],
    )
    with pytest.raises(StarRocksRuntimeException) as raised:
        impl.execute(cfg, [ROWS], upload_id="up1")
    assert raised.value.api_error is StarRocksErrorCode.STARROCKS_5
    assert raised.value.extra["table"] == "t"
