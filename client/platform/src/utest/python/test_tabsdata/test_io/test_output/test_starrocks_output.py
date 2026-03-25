#
# Copyright 2026 Tabs Data Inc.
#

import polars as pl
import pytest

from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    GCPServiceAccountKeyCredentials,
    HMACCredentials,
    S3AccessKeyCredentials,
    UserPasswordCredentials,
)
from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)
from tabsdata._io.connections.starrocks_connection import StarRocksConn
from tabsdata._io.outputs.sql.starrocks_output import StarRocksDest
from tabsdata._io.plugin import DestinationPlugin
from tabsdata.exceptions import (
    DestinationConfigurationError,
    ErrorCode,
    ParameterValidationError,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def s3_conn():
    staging = S3Config(
        bucket="my-bucket",
        region="us-east-1",
        base_path="staging/starrocks",
        credentials=S3AccessKeyCredentials("AKIAIOSFODNN7EXAMPLE", "secret"),
    )
    return StarRocksConn(host="sr.example.com", staging=staging, database="analytics")


@pytest.fixture
def azure_conn():
    staging = AzureConfig(
        container="my-container",
        base_path="staging/starrocks",
        credentials=AzureAccountKeyCredentials("myaccount", "base64key=="),
    )
    return StarRocksConn(host="sr.example.com", staging=staging, database="analytics")


@pytest.fixture
def gcs_conn():
    staging = GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging/starrocks",
        hmac_credentials=HMACCredentials("hmac-key", "hmac-secret"),
    )
    return StarRocksConn(host="sr.example.com", staging=staging, database="analytics")


# ── Construction: defaults ────────────────────────────────────────────────────────────


def test_str_table_defaults(s3_conn):
    dest = StarRocksDest(s3_conn, "events")
    assert dest.conn == s3_conn
    assert dest.destination_tables == ["events"]
    assert dest.if_table_exists == "append"
    assert dest.schema_evolution == "none"
    assert isinstance(dest, DestinationPlugin)


def test_list_tables_defaults(s3_conn):
    dest = StarRocksDest(s3_conn, ["events", "users"])
    assert dest.destination_tables == ["events", "users"]
    assert dest.if_table_exists == "append"


def test_all_params_explicit(s3_conn):
    dest = StarRocksDest(
        conn=s3_conn,
        destination_tables=["events", "users"],
        if_table_exists="replace",
        schema_evolution="iceberg",
    )
    assert dest.destination_tables == ["events", "users"]
    assert dest.if_table_exists == "replace"
    assert dest.schema_evolution == "iceberg"


# ── if_table_exists ──────────────────────────────────────────────────────────────────


def test_if_table_exists_append(s3_conn):
    dest = StarRocksDest(s3_conn, "events", if_table_exists="append")
    assert dest.if_table_exists == "append"


def test_if_table_exists_replace(s3_conn):
    dest = StarRocksDest(s3_conn, "events", if_table_exists="replace")
    assert dest.if_table_exists == "replace"


def test_if_table_exists_invalid_raises(s3_conn):
    with pytest.raises(ParameterValidationError) as exc:
        StarRocksDest(s3_conn, "events", if_table_exists="upsert")
    assert exc.value.error_code == ErrorCode.DECE110


# ── destination_tables normalisation ─────────────────────────────────────────────────


def test_str_table_normalised_to_list(s3_conn):
    dest = StarRocksDest(s3_conn, "events")
    assert dest.destination_tables == ["events"]


def test_multiple_tables(s3_conn):
    dest = StarRocksDest(s3_conn, ["t1", "t2", "t3"])
    assert dest.destination_tables == ["t1", "t2", "t3"]


def test_empty_str_table_raises(s3_conn):
    with pytest.raises(DestinationConfigurationError) as exc:
        StarRocksDest(s3_conn, "")
    assert exc.value.error_code == ErrorCode.DECE54


def test_whitespace_str_table_raises(s3_conn):
    with pytest.raises(DestinationConfigurationError) as exc:
        StarRocksDest(s3_conn, "   ")
    assert exc.value.error_code == ErrorCode.DECE54


def test_invalid_destination_tables_type_raises(s3_conn):
    with pytest.raises(ParameterValidationError) as exc:
        StarRocksDest(s3_conn, 12345)
    assert exc.value.error_code == ErrorCode.DECE110


def test_none_destination_tables_raises(s3_conn):
    with pytest.raises(ParameterValidationError) as exc:
        StarRocksDest(s3_conn, None)
    assert exc.value.error_code == ErrorCode.DECE110


def test_list_with_non_str_element_raises(s3_conn):
    with pytest.raises(ParameterValidationError) as exc:
        StarRocksDest(s3_conn, ["events", None])
    assert exc.value.error_code == ErrorCode.DECE110


# ── conn validation ──────────────────────────────────────────────────────────────────


def test_invalid_conn_type_raises():
    with pytest.raises(ParameterValidationError) as exc:
        StarRocksDest("not-a-conn", "events")
    assert exc.value.error_code == ErrorCode.DECE110


def test_conn_with_sr_credentials(s3_conn):
    staging = S3Config(
        bucket="b",
        region="us-east-1",
        base_path="p",
        credentials=S3AccessKeyCredentials("k", "s"),
    )
    conn = StarRocksConn(
        host="sr.example.com",
        staging=staging,
        credentials=UserPasswordCredentials("admin", "pass"),
    )
    dest = StarRocksDest(conn, "events")
    assert dest.conn.credentials is not None


# ── All three cloud providers ────────────────────────────────────────────────────────


def test_azure_staging(azure_conn):
    dest = StarRocksDest(azure_conn, "events")
    assert dest.conn == azure_conn
    assert dest.destination_tables == ["events"]


def test_gcs_staging(gcs_conn):
    dest = StarRocksDest(gcs_conn, "events")
    assert dest.conn == gcs_conn
    assert dest.destination_tables == ["events"]


def test_gcs_staging_with_upload_credentials():
    staging = GCSConfig(
        bucket="b",
        base_path="p",
        upload_credentials=GCPServiceAccountKeyCredentials('{"type": "service_account"}'),
    )
    conn = StarRocksConn(host="sr.example.com", staging=staging)
    dest = StarRocksDest(conn, "events")
    assert dest.conn.staging.upload_credentials is not None


# ── conn property ────────────────────────────────────────────────────────────────────


def test_conn_property_returns_same_object(s3_conn):
    dest = StarRocksDest(s3_conn, "events")
    assert dest.conn is s3_conn


# ── stream length mismatch ───────────────────────────────────────────────────────────


def test_stream_len_mismatch(s3_conn):
    dest = StarRocksDest(s3_conn, ["t1", "t2"])
    with pytest.raises(ValueError):
        dest.stream(".", pl.LazyFrame())


def test_stream_single_table_mismatch(s3_conn):
    dest = StarRocksDest(s3_conn, "t1")
    with pytest.raises(ValueError):
        dest.stream(".", pl.LazyFrame(), pl.LazyFrame())


# ── repr ─────────────────────────────────────────────────────────────────────────────


def test_repr_contains_class_name(s3_conn):
    dest = StarRocksDest(s3_conn, "events")
    assert "StarRocksDest" in repr(dest)


def test_repr_contains_table_name(s3_conn):
    dest = StarRocksDest(s3_conn, "events")
    assert "events" in repr(dest)


def test_repr_contains_if_table_exists(s3_conn):
    dest = StarRocksDest(s3_conn, "events", if_table_exists="replace")
    assert "replace" in repr(dest)


def test_repr_contains_schema_evolution(s3_conn):
    dest = StarRocksDest(s3_conn, "events", schema_evolution="iceberg")
    assert "iceberg" in repr(dest)


def test_repr_is_nonempty_string(s3_conn):
    dest = StarRocksDest(s3_conn, "events")
    assert isinstance(repr(dest), str)
    assert repr(dest)


# ── schema_evolution ─────────────────────────────────────────────────────────────────


def test_schema_evolution_default_is_none(s3_conn):
    dest = StarRocksDest(s3_conn, "events")
    assert dest.schema_evolution == "none"


def test_schema_evolution_none(s3_conn):
    dest = StarRocksDest(s3_conn, "events", schema_evolution="none")
    assert dest.schema_evolution == "none"


def test_schema_evolution_iceberg(s3_conn):
    dest = StarRocksDest(s3_conn, "events", schema_evolution="iceberg")
    assert dest.schema_evolution == "iceberg"


def test_schema_evolution_invalid_raises(s3_conn):
    with pytest.raises(ParameterValidationError) as exc:
        StarRocksDest(s3_conn, "events", schema_evolution="auto")
    assert exc.value.error_code == ErrorCode.DECE110


def test_schema_evolution_combined_with_replace(s3_conn):
    dest = StarRocksDest(
        s3_conn, "events", if_table_exists="replace", schema_evolution="iceberg"
    )
    assert dest.if_table_exists == "replace"
    assert dest.schema_evolution == "iceberg"


# ── Schema evolution: safe scenarios ─────────────────────────────────────────────────


class TestSchemaEvolutionSafe:
    """Scenarios where Iceberg-style schema evolution succeeds."""

    @staticmethod
    def _evolve(current_cols, desired_cols, table="`db`.`t`"):
        from tabsdata.connector.schema.evolution.iceberg.core import (
            compute_delta,
            validate_delta,
        )
        from tabsdata.connector.schema.evolution.iceberg.models import (
            ColumnDef,
            TableSchema,
        )
        from tabsdata.connector.schema.evolution.iceberg.renderers.starrocks import (
            STARROCKS_TYPE_WIDENING_MAP,
            render_starrocks_sql,
        )

        current = TableSchema(
            table_name=table,
            columns=[ColumnDef(**c) for c in current_cols],
        )
        desired = TableSchema(
            table_name=table,
            columns=[ColumnDef(**c) for c in desired_cols],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        is_safe = validate_delta(delta)
        stmts = render_starrocks_sql(delta)
        return delta, is_safe, stmts

    def test_add_nullable_column(self):
        delta, is_safe, stmts = self._evolve(
            [{"name": "id", "type": "BIGINT", "nullable": False}],
            [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "email", "type": "STRING", "nullable": True},
            ],
        )
        assert is_safe is True
        assert len(stmts) == 1
        assert "ADD COLUMN `email` STRING NULL" in stmts[0]

    def test_add_column_with_default(self):
        delta, is_safe, stmts = self._evolve(
            [{"name": "id", "type": "BIGINT", "nullable": False}],
            [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {
                    "name": "status",
                    "type": "STRING",
                    "nullable": False,
                    "default": "'active'",
                },
            ],
        )
        assert is_safe is True
        assert len(stmts) == 1
        assert "ADD COLUMN `status` STRING NOT NULL DEFAULT 'active'" in stmts[0]

    def test_drop_column(self):
        delta, is_safe, stmts = self._evolve(
            [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "old", "type": "STRING", "nullable": True},
            ],
            [{"name": "id", "type": "BIGINT", "nullable": False}],
        )
        assert is_safe is True
        assert len(stmts) == 1
        assert "DROP COLUMN `old`" in stmts[0]

    def test_widen_tinyint_to_int(self):
        delta, is_safe, stmts = self._evolve(
            [{"name": "id", "type": "TINYINT", "nullable": False}],
            [{"name": "id", "type": "INT", "nullable": False}],
        )
        assert is_safe is True
        assert len(stmts) == 1
        assert "MODIFY COLUMN `id` INT" in stmts[0]

    def test_widen_int_to_bigint(self):
        delta, is_safe, stmts = self._evolve(
            [{"name": "id", "type": "INT", "nullable": False}],
            [{"name": "id", "type": "BIGINT", "nullable": False}],
        )
        assert is_safe is True
        assert len(stmts) == 1
        assert "MODIFY COLUMN `id` BIGINT" in stmts[0]

    def test_widen_float_to_double(self):
        delta, is_safe, stmts = self._evolve(
            [{"name": "v", "type": "FLOAT", "nullable": False}],
            [{"name": "v", "type": "DOUBLE", "nullable": False}],
        )
        assert is_safe is True
        assert "MODIFY COLUMN `v` DOUBLE" in stmts[0]

    def test_set_nullable(self):
        delta, is_safe, stmts = self._evolve(
            [{"name": "name", "type": "STRING", "nullable": False}],
            [{"name": "name", "type": "STRING", "nullable": True}],
        )
        assert is_safe is True
        assert "MODIFY COLUMN `name` STRING NULL" in stmts[0]

    def test_set_default(self):
        delta, is_safe, stmts = self._evolve(
            [{"name": "s", "type": "STRING", "nullable": False}],
            [
                {
                    "name": "s",
                    "type": "STRING",
                    "nullable": False,
                    "default": "'x'",
                },
            ],
        )
        assert is_safe is True
        assert "DEFAULT 'x'" in stmts[0]

    def test_drop_default(self):
        delta, is_safe, stmts = self._evolve(
            [
                {
                    "name": "s",
                    "type": "STRING",
                    "nullable": False,
                    "default": "'x'",
                },
            ],
            [{"name": "s", "type": "STRING", "nullable": False}],
        )
        assert is_safe is True
        # StarRocks restates the type without DEFAULT when dropping it
        assert "MODIFY COLUMN `s` STRING" in stmts[0]

    def test_no_changes_empty_delta(self):
        delta, is_safe, stmts = self._evolve(
            [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "name", "type": "STRING", "nullable": True},
            ],
            [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "name", "type": "STRING", "nullable": True},
            ],
        )
        assert is_safe is True
        assert stmts == []
        assert delta.changes == []

    def test_create_table_from_scratch(self):
        from tabsdata.connector.schema.evolution.iceberg.core import compute_delta
        from tabsdata.connector.schema.evolution.iceberg.models import (
            ColumnDef,
            TableSchema,
        )
        from tabsdata.connector.schema.evolution.iceberg.renderers.starrocks import (
            STARROCKS_TYPE_WIDENING_MAP,
            render_starrocks_sql,
        )

        desired = TableSchema(
            table_name="`db`.`events`",
            columns=[
                ColumnDef(name="id", type="BIGINT", nullable=False),
                ColumnDef(name="name", type="STRING", nullable=True),
                ColumnDef(
                    name="value",
                    type="DOUBLE",
                    nullable=True,
                    default="0.0",
                ),
            ],
        )
        delta = compute_delta(None, desired, STARROCKS_TYPE_WIDENING_MAP)
        assert delta.is_create is True
        stmts = render_starrocks_sql(delta)
        assert len(stmts) == 1
        assert "CREATE TABLE" in stmts[0]

    def test_combined_safe_evolution(self):
        """Add + drop + widen + set nullable in a single delta."""
        delta, is_safe, stmts = self._evolve(
            [
                {"name": "id", "type": "INT", "nullable": False},
                {"name": "name", "type": "STRING", "nullable": False},
                {"name": "old", "type": "STRING", "nullable": True},
            ],
            [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "name", "type": "STRING", "nullable": True},
                {"name": "tag", "type": "STRING", "nullable": True},
            ],
        )
        assert is_safe is True
        assert len(stmts) == 4  # drop old, add tag, widen id, set_nullable name


# ── Schema evolution: unsafe / impossible scenarios ──────────────────────────────────


class TestSchemaEvolutionUnsafe:
    """Scenarios where evolution is unsafe or impossible."""

    @staticmethod
    def _evolve(current_cols, desired_cols, table="`db`.`t`"):
        from tabsdata.connector.schema.evolution.iceberg.core import (
            compute_delta,
            validate_delta,
        )
        from tabsdata.connector.schema.evolution.iceberg.models import (
            ColumnDef,
            TableSchema,
        )
        from tabsdata.connector.schema.evolution.iceberg.renderers.starrocks import (
            STARROCKS_TYPE_WIDENING_MAP,
            render_starrocks_sql,
        )

        current = TableSchema(
            table_name=table,
            columns=[ColumnDef(**c) for c in current_cols],
        )
        desired = TableSchema(
            table_name=table,
            columns=[ColumnDef(**c) for c in desired_cols],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        is_safe = validate_delta(delta)
        stmts = render_starrocks_sql(delta)
        return delta, is_safe, stmts

    def test_add_non_nullable_without_default(self):
        """Iceberg rule: adding a required column without a default is unsafe."""
        delta, is_safe, stmts = self._evolve(
            [{"name": "id", "type": "BIGINT", "nullable": False}],
            [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "required_col", "type": "STRING", "nullable": False},
            ],
        )
        assert is_safe is False

    def test_narrow_bigint_to_int(self):
        """Narrowing a type is never safe."""
        delta, is_safe, stmts = self._evolve(
            [{"name": "id", "type": "BIGINT", "nullable": False}],
            [{"name": "id", "type": "INT", "nullable": False}],
        )
        assert is_safe is False

    def test_incompatible_type_change(self):
        """Changing between unrelated types (e.g. BIGINT -> STRING) is unsafe."""
        delta, is_safe, stmts = self._evolve(
            [{"name": "id", "type": "BIGINT", "nullable": False}],
            [{"name": "id", "type": "STRING", "nullable": False}],
        )
        assert is_safe is False

    def test_drop_nullable_is_unsafe(self):
        """Making an optional column required is unsafe."""
        delta, is_safe, stmts = self._evolve(
            [{"name": "name", "type": "STRING", "nullable": True}],
            [{"name": "name", "type": "STRING", "nullable": False}],
        )
        assert is_safe is False

    def test_drop_nullable_not_rendered(self):
        """StarRocks does not support DROP_NULLABLE -- renderer skips it."""
        delta, is_safe, stmts = self._evolve(
            [{"name": "name", "type": "STRING", "nullable": True}],
            [{"name": "name", "type": "STRING", "nullable": False}],
        )
        assert stmts == []

    def test_strict_validation_raises(self):
        """strict=True raises UnsafeSchemaChangeError on any unsafe change."""
        from tabsdata.connector.schema.evolution.iceberg.core import (
            compute_delta,
            validate_delta,
        )
        from tabsdata.connector.schema.evolution.iceberg.models import (
            ColumnDef,
            TableSchema,
            UnsafeSchemaChangeError,
        )
        from tabsdata.connector.schema.evolution.iceberg.renderers.starrocks import (
            STARROCKS_TYPE_WIDENING_MAP,
        )

        current = TableSchema(
            table_name="`db`.`t`",
            columns=[ColumnDef(name="id", type="BIGINT", nullable=False)],
        )
        desired = TableSchema(
            table_name="`db`.`t`",
            columns=[ColumnDef(name="id", type="INT", nullable=False)],
        )
        delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
        with pytest.raises(UnsafeSchemaChangeError):
            validate_delta(delta, strict=True)

    def test_cross_family_widening_not_supported(self):
        """Widening across type families (e.g. INT -> DOUBLE) is not safe."""
        delta, is_safe, stmts = self._evolve(
            [{"name": "n", "type": "INT", "nullable": False}],
            [{"name": "n", "type": "DOUBLE", "nullable": False}],
        )
        assert is_safe is False

    def test_mixed_safe_and_unsafe_changes(self):
        """If any change is unsafe, the whole delta is unsafe."""
        delta, is_safe, stmts = self._evolve(
            [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "name", "type": "STRING", "nullable": True},
            ],
            [
                {"name": "id", "type": "INT", "nullable": False},  # unsafe
                {"name": "name", "type": "STRING", "nullable": True},
                {"name": "tag", "type": "STRING", "nullable": True},  # safe
            ],
        )
        assert is_safe is False
        # The safe ADD still renders, the unsafe ALTER_TYPE also renders
        # (rendering is independent of safety -- validation gates execution)
        assert len(stmts) >= 1
