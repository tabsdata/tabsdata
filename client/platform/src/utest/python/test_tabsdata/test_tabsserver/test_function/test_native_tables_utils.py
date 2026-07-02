#
# Copyright 2025 Tabs Data Inc.
#

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from tabsdata._tabsserver.function.native_tables_utils import (
    PendingCloudCopies,
    _build_transporter_source,
    _drop_virtual_columns,
    _get_option,
    _run_transporter_copy,
    _transport_cloud_file_to_local,
    scan_lf_from_location,
    scan_tf_from_table,
    sink_lf_to_location,
)
from tabsdata._tabsserver.function.yaml_parsing import (
    TransporterAzure,
    TransporterGCS,
    TransporterLiteral,
    TransporterS3,
)
from tabsdata._utils.temps import tabsdata_temp_folder
from tabsdata.tableframe.lazyframe.frame import TableFrame

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_location(uri=None, env_prefix=None):
    location = MagicMock()
    location.uri = uri
    location.env_prefix = env_prefix
    return location


def _make_execution_context(output_folder="/tmp/output", mount_options=None):
    ctx = MagicMock()
    ctx.paths.output_folder = output_folder
    if mount_options is not None:
        ctx.mount_options.get_options_for_prefix.return_value = mount_options
    else:
        ctx.mount_options.get_options_for_prefix.return_value = {}
    return ctx


def _make_table(
    uri=None,
    env_prefix=None,
    execution_id="exec1",
    transaction_id="txn1",
    table_data_version_id="ver1",
    triggered_on="1700000000000",
    input_idx=0,
    table_pos=0,
):
    table = MagicMock()
    table.location = _make_location(uri=uri, env_prefix=env_prefix)
    table.execution_id = execution_id
    table.transaction_id = transaction_id
    table.table_data_version_id = table_data_version_id
    table.triggered_on = triggered_on
    table.input_idx = input_idx
    table.table_pos = table_pos
    return table


def _write_parquet(path, data=None):
    """Write a small parquet file and return the path."""
    if data is None:
        data = {"col_a": [1, 2, 3], "col_b": ["x", "y", "z"]}
    df = pl.DataFrame(data)
    df.write_parquet(path)
    return path


# ===========================================================================
# _get_option
# ===========================================================================


class TestGetOption:
    def test_exact_key(self):
        assert _get_option({"aws_region": "us-east-1"}, "aws_region") == "us-east-1"

    def test_case_insensitive_upper(self):
        assert (
            _get_option({"AWS_REGION": "us-west-2"}, "aws_region") == "us-west-2"
        )

    def test_case_insensitive_mixed(self):
        assert (
            _get_option({"Aws_Region": "eu-west-1"}, "aws_region") == "eu-west-1"
        )

    def test_missing_key_returns_default_none(self):
        assert _get_option({}, "aws_region") is None

    def test_missing_key_returns_custom_default(self):
        assert _get_option({}, "aws_region", "fallback") == "fallback"

    def test_empty_dict(self):
        assert _get_option({}, "anything") is None

    def test_first_match_wins(self):
        # dict iteration order is insertion order in Python 3.7+
        opts = {"aws_region": "first", "AWS_REGION": "second"}
        assert _get_option(opts, "aws_region") == "first"

    def test_value_types_preserved(self):
        assert _get_option({"key": 42}, "key") == 42
        assert _get_option({"key": True}, "key") is True


# ===========================================================================
# _build_transporter_source
# ===========================================================================


class TestBuildTransporterSource:
    # --- S3 ---

    def test_s3_with_all_options(self):
        opts = {
            "aws_access_key_id": "AKID",
            "aws_secret_access_key": "SECRET",
            "aws_region": "us-east-1",
        }
        source = _build_transporter_source("s3://bucket/key.parquet", opts)
        assert isinstance(source, TransporterS3)
        assert source.content["url"] == "s3://bucket/key.parquet"
        configs = source.content["configs"]
        assert isinstance(configs["access_key"], TransporterLiteral)
        assert configs["access_key"].value == "AKID"
        assert isinstance(configs["secret_key"], TransporterLiteral)
        assert configs["secret_key"].value == "SECRET"
        assert isinstance(configs["region"], TransporterLiteral)
        assert configs["region"].value == "us-east-1"

    def test_s3_without_region(self):
        opts = {
            "aws_access_key_id": "AKID",
            "aws_secret_access_key": "SECRET",
        }
        source = _build_transporter_source("s3://bucket/key.parquet", opts)
        assert isinstance(source, TransporterS3)
        assert source.content["configs"]["region"] is None

    def test_s3_case_insensitive_keys(self):
        opts = {
            "AWS_ACCESS_KEY_ID": "AKID",
            "AWS_SECRET_ACCESS_KEY": "SECRET",
            "AWS_REGION": "eu-west-1",
        }
        source = _build_transporter_source("s3://bucket/key.parquet", opts)
        assert isinstance(source, TransporterS3)
        assert source.content["configs"]["access_key"].value == "AKID"
        assert source.content["configs"]["region"].value == "eu-west-1"

    def test_s3_empty_options(self):
        source = _build_transporter_source("s3://bucket/key.parquet", {})
        assert isinstance(source, TransporterS3)
        assert source.content["configs"]["access_key"] is None
        assert source.content["configs"]["secret_key"] is None
        assert source.content["configs"]["region"] is None

    # --- Azure ---

    def test_azure_az_scheme(self):
        opts = {"account_name": "myaccount", "account_key": "mykey"}
        source = _build_transporter_source("az://container/blob.parquet", opts)
        assert isinstance(source, TransporterAzure)
        assert source.content["url"] == "az://container/blob.parquet"
        assert source.content["configs"]["account_name"].value == "myaccount"
        assert source.content["configs"]["account_key"].value == "mykey"

    def test_azure_abfs_scheme(self):
        opts = {"account_name": "acct", "account_key": "key"}
        source = _build_transporter_source("abfs://container/path", opts)
        assert isinstance(source, TransporterAzure)

    def test_azure_abfss_scheme(self):
        opts = {"account_name": "acct", "account_key": "key"}
        source = _build_transporter_source("abfss://container/path", opts)
        assert isinstance(source, TransporterAzure)

    def test_azure_empty_options(self):
        source = _build_transporter_source("az://container/blob.parquet", {})
        assert isinstance(source, TransporterAzure)
        assert source.content["configs"]["account_name"] is None
        assert source.content["configs"]["account_key"] is None

    # --- GCS ---

    def test_gcs(self):
        opts = {"service_account_key": '{"type": "service_account"}'}
        source = _build_transporter_source("gs://bucket/obj.parquet", opts)
        assert isinstance(source, TransporterGCS)
        assert source.content["url"] == "gs://bucket/obj.parquet"
        assert (
            source.content["configs"]["service_account_key"].value
            == '{"type": "service_account"}'
        )

    def test_gcs_empty_options(self):
        source = _build_transporter_source("gs://bucket/obj.parquet", {})
        assert isinstance(source, TransporterGCS)
        assert source.content["configs"]["service_account_key"] is None

    # --- Unsupported ---

    def test_unsupported_scheme_raises(self):
        with pytest.raises(ValueError, match="Unsupported cloud URI scheme"):
            _build_transporter_source("ftp://host/file.parquet", {})

    def test_unsupported_http_raises(self):
        with pytest.raises(ValueError, match="Unsupported cloud URI scheme"):
            _build_transporter_source("https://example.com/file.parquet", {})


# ===========================================================================
# _transport_cloud_file_to_local
# ===========================================================================


class TestTransportCloudFileToLocal:
    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.subprocess.run"
    )
    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.store_copy_as_yaml"
    )
    def test_successful_transport(self, mock_store_yaml, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(returncode=0, stderr="")
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            local_file = _transport_cloud_file_to_local(
                "s3://bucket/data.parquet",
                tmpdir,
                {"aws_access_key_id": "AKID", "aws_secret_access_key": "SECRET"},
            )
            assert local_file.startswith(tmpdir)
            assert local_file.endswith(".parquet")
            assert "transported_" in os.path.basename(local_file)

            mock_store_yaml.assert_called_once()
            call_args = mock_store_yaml.call_args
            copy_format = call_args[0][0]
            assert len(copy_format.content["source_target_pairs"]) == 1

            mock_subprocess_run.assert_called_once()
            cmd = mock_subprocess_run.call_args[0][0]
            assert cmd[0] in ("td_transporter1", "td_transporter1.exe")
            assert "--request" in cmd
            assert "--report" in cmd

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.subprocess.run"
    )
    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.store_copy_as_yaml"
    )
    def test_transport_failure_raises(self, mock_store_yaml, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(
            returncode=1, stderr="connection refused"
        )
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            with pytest.raises(Exception, match="Error running transporter"):
                _transport_cloud_file_to_local(
                    "s3://bucket/data.parquet", tmpdir, {}
                )

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.subprocess.run"
    )
    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.store_copy_as_yaml"
    )
    def test_azure_transport(self, mock_store_yaml, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(returncode=0, stderr="")
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            local_file = _transport_cloud_file_to_local(
                "az://container/blob.parquet",
                tmpdir,
                {"account_name": "acct", "account_key": "key"},
            )
            assert local_file.endswith(".parquet")
            mock_store_yaml.assert_called_once()

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.subprocess.run"
    )
    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.store_copy_as_yaml"
    )
    def test_gcs_transport(self, mock_store_yaml, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(returncode=0, stderr="")
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            local_file = _transport_cloud_file_to_local(
                "gs://bucket/obj.parquet",
                tmpdir,
                {"service_account_key": "{}"},
            )
            assert local_file.endswith(".parquet")


# ===========================================================================
# scan_lf_from_location
# ===========================================================================


class TestScanLfFromLocation:
    def test_none_uri_returns_none(self):
        ctx = _make_execution_context()
        location = _make_location(uri=None)
        result = scan_lf_from_location(ctx, location)
        assert result is None

    def test_none_uri_with_fail_raises(self):
        ctx = _make_execution_context()
        location = _make_location(uri=None)
        with pytest.raises(ValueError, match="Location URI must be specified"):
            scan_lf_from_location(ctx, location, fail_on_none_uri=True)

    def test_local_file_scans_directly(self):
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            parquet_path = os.path.join(tmpdir, "test.parquet")
            _write_parquet(parquet_path)
            file_uri = Path(parquet_path).as_uri()

            ctx = _make_execution_context(output_folder=tmpdir)
            location = _make_location(uri=file_uri)
            lf = scan_lf_from_location(ctx, location)

            assert isinstance(lf, pl.LazyFrame)
            df = lf.collect()
            assert df.shape == (3, 2)
            assert df.columns == ["col_a", "col_b"]

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils._transport_cloud_file_to_local"
    )
    def test_cloud_uri_uses_transporter(self, mock_transport):
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            local_parquet = os.path.join(tmpdir, "transported.parquet")
            _write_parquet(local_parquet)
            mock_transport.return_value = local_parquet

            ctx = _make_execution_context(
                output_folder=tmpdir,
                mount_options={"aws_access_key_id": "AKID"},
            )
            location = _make_location(uri="s3://bucket/data.parquet", env_prefix="pfx")
            lf = scan_lf_from_location(ctx, location)

            assert isinstance(lf, pl.LazyFrame)
            mock_transport.assert_called_once_with(
                "s3://bucket/data.parquet",
                tmpdir,
                {"aws_access_key_id": "AKID"},
            )

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils._transport_cloud_file_to_local"
    )
    def test_cloud_uri_without_prefix_passes_empty_options(self, mock_transport):
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            local_parquet = os.path.join(tmpdir, "transported.parquet")
            _write_parquet(local_parquet)
            mock_transport.return_value = local_parquet

            ctx = _make_execution_context(output_folder=tmpdir)
            location = _make_location(uri="s3://bucket/data.parquet", env_prefix=None)
            scan_lf_from_location(ctx, location)

            mock_transport.assert_called_once_with(
                "s3://bucket/data.parquet",
                tmpdir,
                {},
            )


# ===========================================================================
# scan_tf_from_table
# ===========================================================================


class TestScanTfFromTable:
    def test_none_uri_returns_none(self):
        ctx = _make_execution_context()
        table = _make_table(uri=None)
        result = scan_tf_from_table(ctx, table)
        assert result is None

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.scan_lf_from_location"
    )
    def test_returns_tableframe_on_valid_scan(self, mock_scan_lf):
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_scan_lf.return_value = mock_lf

        ctx = _make_execution_context()
        # input_idx and table_pos are deliberately different so the assertion
        # below proves the provenance 'tab' is taken from input_idx (the
        # signature index) and not from table_pos.
        table = _make_table(
            uri="file:///data/test.parquet",
            execution_id="exec1",
            transaction_id="txn1",
            table_data_version_id="ver1",
            input_idx=7,
            table_pos=2,
        )

        with patch(
            "tabsdata._tabsserver.function.native_tables_utils.TableFrame.__build__"
        ) as mock_build:
            mock_tf = MagicMock(spec=TableFrame)
            mock_build.return_value = mock_tf

            result = scan_tf_from_table(ctx, table)

            assert result is mock_tf
            mock_build.assert_called_once()
            call_kwargs = mock_build.call_args[1]
            assert call_kwargs["df"] is mock_lf
            # Loading a stored table as a transformer/subscriber input
            # regenerates the pristine columns ("mut" mode) and stamps the
            # provenance 'tab' with the signature index (input_idx).
            assert call_kwargs["mode"] == "mut"
            assert call_kwargs["idx"] == 7

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.scan_lf_from_location"
    )
    def test_passes_fail_on_none_uri(self, mock_scan_lf):
        mock_scan_lf.return_value = None

        ctx = _make_execution_context()
        table = _make_table(uri=None)

        scan_tf_from_table(ctx, table, fail_on_none_uri=True)
        mock_scan_lf.assert_called_once_with(
            ctx, table.location, fail_on_none_uri=True
        )


# ===========================================================================
# _drop_virtual_columns
# ===========================================================================


class TestDropVirtualColumns:
    def test_drops_ver_columns(self):
        lf = pl.LazyFrame(
            {
                "col_a": [1],
                "$td.ver.execution": ["e1"],
                "$td.ver.transaction": ["t1"],
                "col_b": ["x"],
            }
        )
        result = _drop_virtual_columns(lf)
        assert result.collect_schema().names() == ["col_a", "col_b"]

    def test_keeps_non_virtual_td_columns(self):
        lf = pl.LazyFrame(
            {
                "$td.opt.tst.srv": ["s"],
                "col_a": [1],
            }
        )
        result = _drop_virtual_columns(lf)
        assert "$td.opt.tst.srv" in result.collect_schema().names()

    def test_no_virtual_columns_unchanged(self):
        lf = pl.LazyFrame({"col_a": [1], "col_b": [2]})
        result = _drop_virtual_columns(lf)
        assert result.collect_schema().names() == ["col_a", "col_b"]


# ===========================================================================
# _run_transporter_copy
# ===========================================================================


class TestRunTransporterCopy:
    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.subprocess.run"
    )
    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.store_copy_as_yaml"
    )
    def test_success(self, mock_store_yaml, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(returncode=0, stderr="")
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            _run_transporter_copy([["src", "tgt"]], tmpdir)
            mock_store_yaml.assert_called_once()
            mock_subprocess_run.assert_called_once()

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.subprocess.run"
    )
    @patch(
        "tabsdata._tabsserver.function.native_tables_utils.store_copy_as_yaml"
    )
    def test_failure_raises(self, mock_store_yaml, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(
            returncode=1, stderr="timeout"
        )
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            with pytest.raises(Exception, match="Error running transporter"):
                _run_transporter_copy([["src", "tgt"]], tmpdir)


# ===========================================================================
# PendingCloudCopies
# ===========================================================================


class TestPendingCloudCopies:
    def test_empty_is_falsy(self):
        pcc = PendingCloudCopies("/tmp")
        assert not pcc
        assert len(pcc) == 0

    def test_add_makes_truthy(self):
        pcc = PendingCloudCopies("/tmp")
        mock_sink = MagicMock()
        pcc.add(mock_sink, ["src", "tgt"])
        assert pcc
        assert len(pcc) == 1

    def test_add_none_is_noop(self):
        pcc = PendingCloudCopies("/tmp")
        pcc.add(None, ["src", "tgt"])
        assert not pcc
        assert len(pcc) == 0

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils._run_transporter_copy"
    )
    def test_collect_executes_sinks_and_transports(self, mock_run_copy):
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            pcc = PendingCloudCopies(tmpdir)

            sink1 = MagicMock()
            sink2 = MagicMock()
            pair1 = ["local1", "cloud1"]
            pair2 = ["local2", "cloud2"]

            pcc.add(sink1, pair1)
            pcc.add(sink2, pair2)
            assert len(pcc) == 2

            pcc.collect()

            sink1.collect.assert_called_once()
            sink2.collect.assert_called_once()
            mock_run_copy.assert_called_once_with(
                [pair1, pair2], tmpdir
            )
            assert not pcc

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils._run_transporter_copy"
    )
    def test_collect_empty_is_noop(self, mock_run_copy):
        pcc = PendingCloudCopies("/tmp")
        pcc.collect()
        mock_run_copy.assert_not_called()

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils._run_transporter_copy"
    )
    def test_collect_clears_state(self, mock_run_copy):
        pcc = PendingCloudCopies("/tmp")
        pcc.add(MagicMock(), ["s", "t"])
        pcc.collect()
        assert not pcc
        assert len(pcc) == 0


# ===========================================================================
# sink_lf_to_location
# ===========================================================================


class TestSinkLfToLocation:
    def test_none_uri_raises(self):
        ctx = _make_execution_context()
        location = _make_location(uri=None)
        lf = pl.LazyFrame({"col": [1]})
        with pytest.raises(ValueError, match="Location URI must be specified"):
            sink_lf_to_location(lf, ctx, location)

    def test_local_sink_immediate(self):
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            out_path = os.path.join(tmpdir, "out.parquet")
            file_uri = Path(out_path).as_uri()

            ctx = _make_execution_context(output_folder=tmpdir)
            location = _make_location(uri=file_uri)
            lf = pl.LazyFrame({"col_a": [1, 2], "col_b": ["x", "y"]})

            result = sink_lf_to_location(lf, ctx, location, lazy=False)

            assert result is None
            assert os.path.exists(out_path)
            df = pl.read_parquet(out_path)
            assert df.shape == (2, 2)

    def test_local_sink_lazy_returns_lazyframe(self):
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            out_path = os.path.join(tmpdir, "out.parquet")
            file_uri = Path(out_path).as_uri()

            ctx = _make_execution_context(output_folder=tmpdir)
            location = _make_location(uri=file_uri)
            lf = pl.LazyFrame({"col_a": [1, 2]})

            result = sink_lf_to_location(lf, ctx, location, lazy=True)

            assert result is not None
            # File not yet written until we collect
            assert not os.path.exists(out_path)
            result.collect()
            assert os.path.exists(out_path)

    def test_local_sink_drops_virtual_columns(self):
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            out_path = os.path.join(tmpdir, "out.parquet")
            file_uri = Path(out_path).as_uri()

            ctx = _make_execution_context(output_folder=tmpdir)
            location = _make_location(uri=file_uri)
            lf = pl.LazyFrame(
                {
                    "col_a": [1],
                    "$td.ver.execution": ["e1"],
                    "$td.ver.transaction": ["t1"],
                }
            )

            sink_lf_to_location(lf, ctx, location, lazy=False)

            df = pl.read_parquet(out_path)
            assert df.columns == ["col_a"]

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils._run_transporter_copy"
    )
    def test_cloud_sink_immediate(self, mock_run_copy):
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            ctx = _make_execution_context(
                output_folder=tmpdir,
                mount_options={"aws_access_key_id": "AKID"},
            )
            location = _make_location(
                uri="s3://bucket/output.parquet", env_prefix="pfx"
            )
            lf = pl.LazyFrame({"col_a": [1, 2]})

            result = sink_lf_to_location(lf, ctx, location, lazy=False)

            assert result is None
            mock_run_copy.assert_called_once()
            copy_pairs = mock_run_copy.call_args[0][0]
            assert len(copy_pairs) == 1

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils._run_transporter_copy"
    )
    def test_cloud_sink_lazy_adds_to_pending(self, mock_run_copy):
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            ctx = _make_execution_context(output_folder=tmpdir)
            ctx.pending_cloud_copies = None
            location = _make_location(uri="s3://bucket/out.parquet")
            lf = pl.LazyFrame({"col_a": [1, 2]})

            result = sink_lf_to_location(lf, ctx, location, lazy=True)

            assert result is None
            assert ctx.pending_cloud_copies is not None
            assert len(ctx.pending_cloud_copies) == 1
            # Transporter not called yet
            mock_run_copy.assert_not_called()

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils._run_transporter_copy"
    )
    def test_cloud_sink_lazy_multiple_accumulates(self, mock_run_copy):
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            ctx = _make_execution_context(output_folder=tmpdir)
            ctx.pending_cloud_copies = None

            for i in range(3):
                location = _make_location(uri=f"s3://bucket/out_{i}.parquet")
                lf = pl.LazyFrame({"col": [i]})
                sink_lf_to_location(lf, ctx, location, lazy=True)

            assert len(ctx.pending_cloud_copies) == 3
            mock_run_copy.assert_not_called()

    @patch(
        "tabsdata._tabsserver.function.native_tables_utils._run_transporter_copy"
    )
    def test_cloud_sink_lazy_collect_executes_all(self, mock_run_copy):
        with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
            ctx = _make_execution_context(output_folder=tmpdir)
            ctx.pending_cloud_copies = None

            for i in range(2):
                location = _make_location(uri=f"s3://bucket/out_{i}.parquet")
                lf = pl.LazyFrame({"col": [i]})
                sink_lf_to_location(lf, ctx, location, lazy=True)

            ctx.pending_cloud_copies.collect()

            mock_run_copy.assert_called_once()
            copy_pairs = mock_run_copy.call_args[0][0]
            assert len(copy_pairs) == 2
            assert not ctx.pending_cloud_copies
