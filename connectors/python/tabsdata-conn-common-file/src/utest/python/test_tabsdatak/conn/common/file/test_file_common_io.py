#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tabsdatak.common._transporter.invoker import transporter1


# --- Server-side env var name constants --------------------------


def test_server_side_env_constants_match_tabsdata_os():
    assert transporter1.SERVER_SIDE_AWS_ACCESS_KEY_ID == "LOCATION_AWS_ACCESS_KEY_ID"
    assert transporter1.SERVER_SIDE_AWS_SECRET_ACCESS_KEY == "LOCATION_AWS_SECRET_ACCESS_KEY"
    assert transporter1.SERVER_SIDE_AWS_REGION == "LOCATION_AWS_REGION"
    assert transporter1.SERVER_SIDE_AZURE_ACCOUNT_NAME == "LOCATION_ACCOUNT_NAME"
    assert transporter1.SERVER_SIDE_AZURE_ACCOUNT_KEY == "LOCATION_ACCOUNT_KEY"
    assert transporter1.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON == "LOCATION_GCS_SERVICE_ACCOUNT_JSON"


# --- transporter1.TransporterEnv / transporter1.TransporterLiteral -------------------------


def test_transporter_env_repr_carries_name():
    e = transporter1.TransporterEnv("FOO_BAR")
    assert e.name == "FOO_BAR"
    assert "FOO_BAR" in repr(e)


def test_transporter_literal_repr_redacts_value():
    lit = transporter1.TransporterLiteral("us-east-1")
    assert lit.value == "us-east-1"
    assert "us-east-1" not in repr(lit)
    assert "***redacted***" in repr(lit)


# --- Location classes --------------------------------------------


def test_transporter_s3_field_shape():
    loc = transporter1.TransporterS3(
        url="s3://bucket/prefix/",
        access_key=transporter1.TransporterEnv("LOCATION_AWS_ACCESS_KEY_ID"),
        secret_key=transporter1.TransporterEnv("LOCATION_AWS_SECRET_ACCESS_KEY"),
        region=transporter1.TransporterLiteral("us-east-1"),
        extra_configs={"endpoint_url": "https://localhost:9000"},
    )
    assert loc.url == "s3://bucket/prefix/"
    assert loc.region.value == "us-east-1"
    assert loc.extra_configs["endpoint_url"] == "https://localhost:9000"


def test_transporter_s3_optional_region_none():
    loc = transporter1.TransporterS3(
        url="s3://b/",
        access_key=transporter1.TransporterEnv("X"),
        secret_key=transporter1.TransporterEnv("Y"),
    )
    assert loc.region is None


def test_transporter_s3_repr_includes_content():
    loc = transporter1.TransporterS3(
        url="s3://b/",
        access_key=transporter1.TransporterEnv("X"),
        secret_key=transporter1.TransporterEnv("Y"),
    )
    assert "s3://b/" in repr(loc)


def test_transporter_gcs_field_shape():
    loc = transporter1.TransporterGCS(
        url="gs://bucket/prefix/",
        service_account_key=transporter1.TransporterEnv("LOCATION_GCS_SERVICE_ACCOUNT_JSON"),
        extra_configs={"timeout": 30},
    )
    assert loc.url == "gs://bucket/prefix/"
    assert loc.extra_configs["timeout"] == 30


def test_transporter_gcs_repr():
    loc = transporter1.TransporterGCS(url="gs://b/", service_account_key=transporter1.TransporterEnv("X"))
    assert "gs://b/" in repr(loc)


def test_transporter_azure_field_shape():
    loc = transporter1.TransporterAzure(
        url="az://container/prefix/",
        account_name=transporter1.TransporterEnv("LOCATION_ACCOUNT_NAME"),
        account_key=transporter1.TransporterEnv("LOCATION_ACCOUNT_KEY"),
    )
    assert loc.url == "az://container/prefix/"
    assert loc.account_name.name == "LOCATION_ACCOUNT_NAME"


def test_transporter_azure_repr():
    loc = transporter1.TransporterAzure(
        url="az://c/",
        account_name=transporter1.TransporterEnv("X"),
        account_key=transporter1.TransporterEnv("Y"),
    )
    assert "az://c/" in repr(loc)


def test_transporter_local_file_field_shape():
    loc = transporter1.TransporterLocalFile("file:///tmp/foo")
    assert loc.url == "file:///tmp/foo"


def test_transporter_local_file_repr():
    loc = transporter1.TransporterLocalFile("file:///tmp/foo")
    assert "file:///tmp/foo" in repr(loc)


# --- Format reprs ------------------------------------------------


def test_transporter_format_reprs():
    assert repr(transporter1.TransporterAvroFormat()) == "TransporterAvroFormat()"
    assert repr(transporter1.TransporterParquetFormat()) == "TransporterParquetFormat()"
    assert repr(transporter1.TransporterLogFormat()) == "TransporterLogFormat()"
    assert repr(transporter1.TransporterJsonFormat()) == "TransporterJsonFormat()"


def test_transporter_csv_format_with_options():
    fmt = transporter1.TransporterCSVFormat({"separator": 59, "quote_char": 34})
    assert fmt.options == {"separator": 59, "quote_char": 34}
    assert "59" in repr(fmt)


def test_transporter_csv_format_default_empty():
    fmt = transporter1.TransporterCSVFormat()
    assert fmt.options == {}


# --- V1 envelopes ------------------------------------------------


def test_v1_import_format_minimal():
    env = transporter1.V1ImportFormat(
        source=transporter1.TransporterLocalFile("file:///in/users.csv"),
        format=transporter1.TransporterParquetFormat(),
        target=transporter1.TransporterLocalFile("file:///tmp/work/"),
    )
    assert env.initial_lastmod is None
    assert env.lastmod_info is None
    assert env.file_info is None


def test_v1_import_format_with_file_info_and_watermarks():
    env = transporter1.V1ImportFormat(
        source=transporter1.TransporterLocalFile("file:///in/users.csv"),
        format=transporter1.TransporterParquetFormat(),
        target=transporter1.TransporterLocalFile("file:///tmp/work/"),
        initial_lastmod="2026-01-01T00:00:00.000000+00:00",
        lastmod_info="abc",
        file_info="FILENAME",
    )
    assert env.initial_lastmod == "2026-01-01T00:00:00.000000+00:00"
    assert env.lastmod_info == "abc"
    assert env.file_info == "FILENAME"


def test_v1_import_format_repr():
    env = transporter1.V1ImportFormat(
        source=transporter1.TransporterLocalFile("file:///x"),
        format=transporter1.TransporterParquetFormat(),
        target=transporter1.TransporterLocalFile("file:///y"),
    )
    assert "V1ImportFormat" in repr(env)


def test_v1_copy_format_pairs():
    src = transporter1.TransporterLocalFile("file:///a")
    tgt = transporter1.TransporterLocalFile("file:///b")
    fmt = transporter1.TransporterParquetFormat()
    env = transporter1.V1CopyFormat([[src, tgt]], format=fmt)
    assert env.source_target_pairs == [[src, tgt]]
    assert env.format is fmt
    assert "V1CopyFormat" in repr(env)


# --- YAML round-trip --------------------------------------------


def test_dump_v1_import_emits_expected_tags(tmp_path):
    out = tmp_path / "req.yaml"
    env = transporter1.V1ImportFormat(
        source=transporter1.TransporterS3(
            url="s3://b/k",
            access_key=transporter1.TransporterEnv("LOCATION_AWS_ACCESS_KEY_ID"),
            secret_key=transporter1.TransporterEnv("LOCATION_AWS_SECRET_ACCESS_KEY"),
            region=transporter1.TransporterLiteral("us-east-1"),
        ),
        format=transporter1.TransporterCSVFormat({"separator": 59}),
        target=transporter1.TransporterLocalFile("file:///tmp/work/"),
        initial_lastmod="2026-01-01T00:00:00+00:00",
        file_info="FULLPATH",
    )
    transporter1.dump_request_yaml(env, out, dumper_cls=transporter1.INPUT_DUMPER)
    text = out.read_text()
    # Every wire tag we depend on must show up literally.
    assert "!ImportV1" in text
    assert "!S3" in text
    assert "!Csv" in text
    assert "!Env" in text
    assert "!Literal" in text
    assert "!LocalFile" in text


def test_dump_v1_import_each_format_scalar(tmp_path):
    cases = [
        (transporter1.TransporterAvroFormat(), "Binary"),
        (transporter1.TransporterParquetFormat(), "Parquet"),
        (transporter1.TransporterLogFormat(), "Log"),
        (transporter1.TransporterJsonFormat(), "Json"),
    ]
    for fmt, scalar in cases:
        out = tmp_path / f"req_{scalar}.yaml"
        env = transporter1.V1ImportFormat(
            source=transporter1.TransporterLocalFile("file:///x"),
            format=fmt,
            target=transporter1.TransporterLocalFile("file:///y"),
        )
        transporter1.dump_request_yaml(env, out, dumper_cls=transporter1.INPUT_DUMPER)
        text = out.read_text()
        assert scalar in text


def test_dump_v1_copy_includes_pairs(tmp_path):
    out = tmp_path / "req.yaml"
    env = transporter1.V1CopyFormat(
        [
            [
                transporter1.TransporterLocalFile("file:///a"),
                transporter1.TransporterGCS(
                    url="gs://b/k",
                    service_account_key=transporter1.TransporterEnv("LOCATION_GCS_SERVICE_ACCOUNT_JSON"),
                ),
            ],
            [
                transporter1.TransporterLocalFile("file:///c"),
                transporter1.TransporterAzure(
                    url="az://c/k",
                    account_name=transporter1.TransporterEnv("LOCATION_ACCOUNT_NAME"),
                    account_key=transporter1.TransporterEnv("LOCATION_ACCOUNT_KEY"),
                ),
            ],
        ],
        format=transporter1.TransporterParquetFormat(),
    )
    transporter1.dump_request_yaml(env, out, dumper_cls=transporter1.OUTPUT_DUMPER)
    text = out.read_text()
    assert "!CopyV1" in text
    assert "!GCS" in text
    assert "!Azure" in text


# --- Report loader ------------------------------------------------


def test_load_report_yaml_minimal(tmp_path):
    p = tmp_path / "report.yaml"
    p.write_text("!ImportV1\nfiles: []\nlastmod_info: null\n")
    report = transporter1.load_report_yaml(p)
    assert isinstance(report, transporter1.TransporterReport)
    assert report.files == []
    assert report.lastmod_info is None


def test_load_report_yaml_with_files(tmp_path):
    p = tmp_path / "report.yaml"
    p.write_text(
        "!ImportV1\n"
        "files:\n"
        "  - {from: /a/x, to: /tmp/x.parquet}\n"
        "  - {from: /a/y, to: /tmp/y.parquet}\n"
        "lastmod_info: '2026-01-01T00:00:00.000000+00:00'\n"
    )
    report = transporter1.load_report_yaml(p)
    assert len(report.files) == 2
    assert report.files[0]["to"] == "/tmp/x.parquet"
    assert report.lastmod_info == "2026-01-01T00:00:00.000000+00:00"


def test_load_report_yaml_repr(tmp_path):
    p = tmp_path / "report.yaml"
    p.write_text("!ImportV1\nfiles: []\nlastmod_info: null\n")
    assert "TransporterReport" in repr(transporter1.load_report_yaml(p))


def test_load_report_yaml_missing_file_raises(tmp_path):
    with pytest.raises(transporter1.TransporterReportError, match="not found"):
        transporter1.load_report_yaml(tmp_path / "nope.yaml")


def test_load_report_yaml_unparseable_raises(tmp_path):
    p = tmp_path / "report.yaml"
    p.write_text("not: valid: yaml: : :\n")
    with pytest.raises(transporter1.TransporterReportError, match="failed to parse"):
        transporter1.load_report_yaml(p)


def test_load_report_yaml_non_mapping_raises(tmp_path):
    p = tmp_path / "report.yaml"
    p.write_text("- a\n- b\n")
    with pytest.raises(transporter1.TransporterReportError, match="not a mapping"):
        transporter1.load_report_yaml(p)


# --- transporter1.TransporterRunError ------------------------------------------


def test_run_error_carries_streams():
    err = transporter1.TransporterRunError(2, "stdout-data", "stderr-data")
    assert err.returncode == 2
    assert err.stdout == "stdout-data"
    assert err.stderr == "stderr-data"
    assert "stderr-data" in str(err)


def test_run_error_falls_back_to_stdout_when_stderr_empty():
    err = transporter1.TransporterRunError(1, "stdout-data", "")
    assert "stdout-data" in str(err)


# --- transporter1.path_to_file_uri --------------------------------------------


def test_path_to_file_uri_absolute_file(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("")
    uri = transporter1.path_to_file_uri(p)
    assert uri.startswith("file://")
    assert uri.endswith("x.csv")


def test_path_to_file_uri_existing_dir_gets_trailing_slash(tmp_path):
    uri = transporter1.path_to_file_uri(tmp_path)
    assert uri.endswith("/")


def test_path_to_file_uri_string_with_trailing_slash(tmp_path):
    nonexistent = str(tmp_path / "out") + "/"
    uri = transporter1.path_to_file_uri(nonexistent)
    assert uri.endswith("/")


def test_path_to_file_uri_relative_resolved(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path("rel.csv").write_text("")
    uri = transporter1.path_to_file_uri("rel.csv")
    assert uri.endswith("/rel.csv")
    assert "file://" in uri


