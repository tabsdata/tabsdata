from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from tabsdatak.common._transporter import _transporter1
from tabsdatak.conn.common.file import _input
from tabsdatak.conn.common.file import _output


# --- Server-side env var name constants --------------------------


def test_server_side_env_constants_match_tabsdata_os():
    assert _transporter1.SERVER_SIDE_AWS_ACCESS_KEY_ID == "LOCATION_AWS_ACCESS_KEY_ID"
    assert _transporter1.SERVER_SIDE_AWS_SECRET_ACCESS_KEY == "LOCATION_AWS_SECRET_ACCESS_KEY"
    assert _transporter1.SERVER_SIDE_AWS_REGION == "LOCATION_AWS_REGION"
    assert _transporter1.SERVER_SIDE_AZURE_ACCOUNT_NAME == "LOCATION_ACCOUNT_NAME"
    assert _transporter1.SERVER_SIDE_AZURE_ACCOUNT_KEY == "LOCATION_ACCOUNT_KEY"
    assert _transporter1.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON == "LOCATION_GCS_SERVICE_ACCOUNT_JSON"


# --- _transporter1.TransporterEnv / _transporter1.TransporterLiteral -------------------------


def test_transporter_env_repr_carries_name():
    e = _transporter1.TransporterEnv("FOO_BAR")
    assert e.name == "FOO_BAR"
    assert "FOO_BAR" in repr(e)


def test_transporter_literal_repr_redacts_value():
    lit = _transporter1.TransporterLiteral("us-east-1")
    assert lit.value == "us-east-1"
    assert "us-east-1" not in repr(lit)
    assert "***redacted***" in repr(lit)


# --- Location classes --------------------------------------------


def test_transporter_s3_field_shape():
    loc = _transporter1.TransporterS3(
        url="s3://bucket/prefix/",
        access_key=_transporter1.TransporterEnv("LOCATION_AWS_ACCESS_KEY_ID"),
        secret_key=_transporter1.TransporterEnv("LOCATION_AWS_SECRET_ACCESS_KEY"),
        region=_transporter1.TransporterLiteral("us-east-1"),
        extra_configs={"endpoint_url": "https://localhost:9000"},
    )
    assert loc.url == "s3://bucket/prefix/"
    assert loc.region.value == "us-east-1"
    assert loc.extra_configs["endpoint_url"] == "https://localhost:9000"


def test_transporter_s3_optional_region_none():
    loc = _transporter1.TransporterS3(
        url="s3://b/",
        access_key=_transporter1.TransporterEnv("X"),
        secret_key=_transporter1.TransporterEnv("Y"),
    )
    assert loc.region is None


def test_transporter_s3_repr_includes_content():
    loc = _transporter1.TransporterS3(
        url="s3://b/",
        access_key=_transporter1.TransporterEnv("X"),
        secret_key=_transporter1.TransporterEnv("Y"),
    )
    assert "s3://b/" in repr(loc)


def test_transporter_gcs_field_shape():
    loc = _transporter1.TransporterGCS(
        url="gs://bucket/prefix/",
        service_account_key=_transporter1.TransporterEnv("LOCATION_GCS_SERVICE_ACCOUNT_JSON"),
        extra_configs={"timeout": 30},
    )
    assert loc.url == "gs://bucket/prefix/"
    assert loc.extra_configs["timeout"] == 30


def test_transporter_gcs_repr():
    loc = _transporter1.TransporterGCS(url="gs://b/", service_account_key=_transporter1.TransporterEnv("X"))
    assert "gs://b/" in repr(loc)


def test_transporter_azure_field_shape():
    loc = _transporter1.TransporterAzure(
        url="az://container/prefix/",
        account_name=_transporter1.TransporterEnv("LOCATION_ACCOUNT_NAME"),
        account_key=_transporter1.TransporterEnv("LOCATION_ACCOUNT_KEY"),
    )
    assert loc.url == "az://container/prefix/"
    assert loc.account_name.name == "LOCATION_ACCOUNT_NAME"


def test_transporter_azure_repr():
    loc = _transporter1.TransporterAzure(
        url="az://c/",
        account_name=_transporter1.TransporterEnv("X"),
        account_key=_transporter1.TransporterEnv("Y"),
    )
    assert "az://c/" in repr(loc)


def test_transporter_local_file_field_shape():
    loc = _transporter1.TransporterLocalFile("file:///tmp/foo")
    assert loc.url == "file:///tmp/foo"


def test_transporter_local_file_repr():
    loc = _transporter1.TransporterLocalFile("file:///tmp/foo")
    assert "file:///tmp/foo" in repr(loc)


# --- Format reprs ------------------------------------------------


def test_transporter_format_reprs():
    assert repr(_transporter1.TransporterAvroFormat()) == "TransporterAvroFormat()"
    assert repr(_transporter1.TransporterParquetFormat()) == "TransporterParquetFormat()"
    assert repr(_transporter1.TransporterLogFormat()) == "TransporterLogFormat()"
    assert repr(_transporter1.TransporterJsonFormat()) == "TransporterJsonFormat()"


def test_transporter_csv_format_with_options():
    fmt = _transporter1.TransporterCSVFormat({"separator": 59, "quote_char": 34})
    assert fmt.options == {"separator": 59, "quote_char": 34}
    assert "59" in repr(fmt)


def test_transporter_csv_format_default_empty():
    fmt = _transporter1.TransporterCSVFormat()
    assert fmt.options == {}


# --- V1 envelopes ------------------------------------------------


def test_v1_import_format_minimal():
    env = _transporter1.V1ImportFormat(
        source=_transporter1.TransporterLocalFile("file:///in/users.csv"),
        format=_transporter1.TransporterParquetFormat(),
        target=_transporter1.TransporterLocalFile("file:///tmp/work/"),
    )
    assert env.initial_lastmod is None
    assert env.lastmod_info is None
    assert env.file_info is None


def test_v1_import_format_with_file_info_and_watermarks():
    env = _transporter1.V1ImportFormat(
        source=_transporter1.TransporterLocalFile("file:///in/users.csv"),
        format=_transporter1.TransporterParquetFormat(),
        target=_transporter1.TransporterLocalFile("file:///tmp/work/"),
        initial_lastmod="2026-01-01T00:00:00.000000+00:00",
        lastmod_info="abc",
        file_info="FILENAME",
    )
    assert env.initial_lastmod == "2026-01-01T00:00:00.000000+00:00"
    assert env.lastmod_info == "abc"
    assert env.file_info == "FILENAME"


def test_v1_import_format_repr():
    env = _transporter1.V1ImportFormat(
        source=_transporter1.TransporterLocalFile("file:///x"),
        format=_transporter1.TransporterParquetFormat(),
        target=_transporter1.TransporterLocalFile("file:///y"),
    )
    assert "V1ImportFormat" in repr(env)


def test_v1_copy_format_pairs():
    src = _transporter1.TransporterLocalFile("file:///a")
    tgt = _transporter1.TransporterLocalFile("file:///b")
    fmt = _transporter1.TransporterParquetFormat()
    env = _transporter1.V1CopyFormat([[src, tgt]], format=fmt)
    assert env.source_target_pairs == [[src, tgt]]
    assert env.format is fmt
    assert "V1CopyFormat" in repr(env)


# --- YAML round-trip --------------------------------------------


def test_dump_v1_import_emits_expected_tags(tmp_path):
    out = tmp_path / "req.yaml"
    env = _transporter1.V1ImportFormat(
        source=_transporter1.TransporterS3(
            url="s3://b/k",
            access_key=_transporter1.TransporterEnv("LOCATION_AWS_ACCESS_KEY_ID"),
            secret_key=_transporter1.TransporterEnv("LOCATION_AWS_SECRET_ACCESS_KEY"),
            region=_transporter1.TransporterLiteral("us-east-1"),
        ),
        format=_transporter1.TransporterCSVFormat({"separator": 59}),
        target=_transporter1.TransporterLocalFile("file:///tmp/work/"),
        initial_lastmod="2026-01-01T00:00:00+00:00",
        file_info="FULLPATH",
    )
    _transporter1.dump_request_yaml(env, out, dumper_cls=_transporter1.INPUT_DUMPER)
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
        (_transporter1.TransporterAvroFormat(), "Binary"),
        (_transporter1.TransporterParquetFormat(), "Parquet"),
        (_transporter1.TransporterLogFormat(), "Log"),
        (_transporter1.TransporterJsonFormat(), "Json"),
    ]
    for fmt, scalar in cases:
        out = tmp_path / f"req_{scalar}.yaml"
        env = _transporter1.V1ImportFormat(
            source=_transporter1.TransporterLocalFile("file:///x"),
            format=fmt,
            target=_transporter1.TransporterLocalFile("file:///y"),
        )
        _transporter1.dump_request_yaml(env, out, dumper_cls=_transporter1.INPUT_DUMPER)
        text = out.read_text()
        assert scalar in text


def test_dump_v1_copy_includes_pairs(tmp_path):
    out = tmp_path / "req.yaml"
    env = _transporter1.V1CopyFormat(
        [
            [
                _transporter1.TransporterLocalFile("file:///a"),
                _transporter1.TransporterGCS(
                    url="gs://b/k",
                    service_account_key=_transporter1.TransporterEnv("LOCATION_GCS_SERVICE_ACCOUNT_JSON"),
                ),
            ],
            [
                _transporter1.TransporterLocalFile("file:///c"),
                _transporter1.TransporterAzure(
                    url="az://c/k",
                    account_name=_transporter1.TransporterEnv("LOCATION_ACCOUNT_NAME"),
                    account_key=_transporter1.TransporterEnv("LOCATION_ACCOUNT_KEY"),
                ),
            ],
        ],
        format=_transporter1.TransporterParquetFormat(),
    )
    _transporter1.dump_request_yaml(env, out, dumper_cls=_transporter1.OUTPUT_DUMPER)
    text = out.read_text()
    assert "!CopyV1" in text
    assert "!GCS" in text
    assert "!Azure" in text


# --- Report loader ------------------------------------------------


def test_load_report_yaml_minimal(tmp_path):
    p = tmp_path / "report.yaml"
    p.write_text("!ImportV1\nfiles: []\nlastmod_info: null\n")
    report = _transporter1.load_report_yaml(p)
    assert isinstance(report, _transporter1.TransporterReport)
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
    report = _transporter1.load_report_yaml(p)
    assert len(report.files) == 2
    assert report.files[0]["to"] == "/tmp/x.parquet"
    assert report.lastmod_info == "2026-01-01T00:00:00.000000+00:00"


def test_load_report_yaml_repr(tmp_path):
    p = tmp_path / "report.yaml"
    p.write_text("!ImportV1\nfiles: []\nlastmod_info: null\n")
    assert "TransporterReport" in repr(_transporter1.load_report_yaml(p))


def test_load_report_yaml_missing_file_raises(tmp_path):
    with pytest.raises(_transporter1.TransporterReportError, match="not found"):
        _transporter1.load_report_yaml(tmp_path / "nope.yaml")


def test_load_report_yaml_unparseable_raises(tmp_path):
    p = tmp_path / "report.yaml"
    p.write_text("not: valid: yaml: : :\n")
    with pytest.raises(_transporter1.TransporterReportError, match="failed to parse"):
        _transporter1.load_report_yaml(p)


def test_load_report_yaml_non_mapping_raises(tmp_path):
    p = tmp_path / "report.yaml"
    p.write_text("- a\n- b\n")
    with pytest.raises(_transporter1.TransporterReportError, match="not a mapping"):
        _transporter1.load_report_yaml(p)


# --- Binary lookup -----------------------------------------------


def test_find_binary_via_env_override(monkeypatch, tmp_path):
    bin_path = tmp_path / "td_transporter1"
    bin_path.write_text("#!/bin/sh\nexit 0\n")
    bin_path.chmod(0o755)

    monkeypatch.setenv("TD_TRANSPORTER_BIN", str(bin_path))
    assert _transporter1.find_transporter_binary() == str(bin_path)


def test_find_binary_env_override_not_executable(monkeypatch, tmp_path):
    bin_path = tmp_path / "missing"
    monkeypatch.setenv("TD_TRANSPORTER_BIN", str(bin_path))
    with pytest.raises(_transporter1.TransporterBinaryNotFoundError, match="executable"):
        _transporter1.find_transporter_binary()


def test_find_binary_via_which(monkeypatch, tmp_path):
    monkeypatch.delenv("TD_TRANSPORTER_BIN", raising=False)
    fake_bin = tmp_path / "td_transporter1"
    fake_bin.write_text("#!/bin/sh\n")
    fake_bin.chmod(0o755)
    monkeypatch.setattr(_transporter, "shutil", _ShutilStub(str(fake_bin)))
    assert _transporter1.find_transporter_binary() == str(fake_bin)


def test_find_binary_not_found(monkeypatch):
    monkeypatch.delenv("TD_TRANSPORTER_BIN", raising=False)
    monkeypatch.setattr(_transporter, "shutil", _ShutilStub(None))
    with pytest.raises(_transporter1.TransporterBinaryNotFoundError, match="not found on PATH"):
        _transporter1.find_transporter_binary()


class _ShutilStub:

    def __init__(self, value):
        self._value = value

    def which(self, _name):
        return self._value


# --- _transporter1.TransporterRunError ------------------------------------------


def test_run_error_carries_streams():
    err = _transporter1.TransporterRunError(2, "stdout-data", "stderr-data")
    assert err.returncode == 2
    assert err.stdout == "stdout-data"
    assert err.stderr == "stderr-data"
    assert "stderr-data" in str(err)


def test_run_error_falls_back_to_stdout_when_stderr_empty():
    err = _transporter1.TransporterRunError(1, "stdout-data", "")
    assert "stdout-data" in str(err)


# --- _input.import_one / _output.copy_one drivers -------------------------------


def _patch_subprocess_with_report(
    monkeypatch,
    *,
    files: list[dict] | None = None,
    lastmod_info: str | None = None,
    returncode: int = 0,
    stderr: str = "",
):
    files = files or []

    def fake(args, **kwargs):
        report_path = Path(args[args.index("--report") + 1])
        if returncode == 0:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            with open(report_path, "w") as fh:
                yaml.dump(
                    {"files": files, "lastmod_info": lastmod_info},
                    fh,
                )

        class _C:
            pass

        c = _C()
        c.returncode = returncode
        c.stdout = ""
        c.stderr = stderr
        return c

    monkeypatch.setattr(_transporter1.subprocess, "run", fake)


def test_import_one_returns_parsed_report(monkeypatch, tmp_path):
    monkeypatch.setattr(
        _transporter, "find_transporter_binary", lambda: "/fake/bin",
    )
    _patch_subprocess_with_report(
        monkeypatch,
        files=[{"from": "/in/users.csv", "to": str(tmp_path / "out.parquet")}],
        lastmod_info="abc",
    )
    report = _input.import_one(
        source=_transporter1.TransporterLocalFile("file:///in/users.csv"),
        target_dir=tmp_path,
        format=_transporter1.TransporterParquetFormat(),
    )
    assert report.files[0]["to"] == str(tmp_path / "out.parquet")
    assert report.lastmod_info == "abc"


def test_import_one_passes_env_into_subprocess(monkeypatch, tmp_path):
    monkeypatch.setattr(
        _transporter, "find_transporter_binary", lambda: "/fake/bin",
    )
    captured: dict[str, Any] = {}

    def fake(args, **kwargs):
        captured["env"] = kwargs.get("env")
        report_path = Path(args[args.index("--report") + 1])
        with open(report_path, "w") as fh:
            yaml.dump({"files": [], "lastmod_info": None}, fh)

        class _C:
            returncode = 0
            stdout = ""
            stderr = ""

        return _C()

    monkeypatch.setattr(_transporter1.subprocess, "run", fake)

    _input.import_one(
        source=_transporter1.TransporterLocalFile("file:///x"),
        target_dir=tmp_path,
        format=_transporter1.TransporterParquetFormat(),
        env={"LOCATION_AWS_ACCESS_KEY_ID": "AKIAFAKE"},
    )
    assert captured["env"]["LOCATION_AWS_ACCESS_KEY_ID"] == "AKIAFAKE"


def test_import_one_uses_explicit_binary_path(monkeypatch, tmp_path):

    def boom():
        raise RuntimeError("must not call _transporter1.find_transporter_binary")

    monkeypatch.setattr(_transporter, "find_transporter_binary", boom)
    captured: dict[str, Any] = {}

    def fake(args, **kwargs):
        captured["bin"] = args[0]
        report_path = Path(args[args.index("--report") + 1])
        with open(report_path, "w") as fh:
            yaml.dump({"files": [], "lastmod_info": None}, fh)

        class _C:
            returncode = 0
            stdout = ""
            stderr = ""

        return _C()

    monkeypatch.setattr(_transporter1.subprocess, "run", fake)
    _input.import_one(
        source=_transporter1.TransporterLocalFile("file:///x"),
        target_dir=tmp_path,
        format=_transporter1.TransporterParquetFormat(),
        binary_path="/explicit/td_transporter1",
    )
    assert captured["bin"] == "/explicit/td_transporter1"


def test_import_one_run_error_on_nonzero_exit(monkeypatch, tmp_path):
    monkeypatch.setattr(
        _transporter, "find_transporter_binary", lambda: "/fake/bin",
    )
    _patch_subprocess_with_report(monkeypatch, returncode=2, stderr="boom")
    with pytest.raises(_transporter1.TransporterRunError) as ei:
        _input.import_one(
            source=_transporter1.TransporterLocalFile("file:///x"),
            target_dir=tmp_path,
            format=_transporter1.TransporterParquetFormat(),
        )
    assert ei.value.returncode == 2


def test_copy_one_runs_pairs(monkeypatch, tmp_path):
    monkeypatch.setattr(
        _transporter, "find_transporter_binary", lambda: "/fake/bin",
    )
    captured: dict[str, Any] = {}

    def fake(args, **kwargs):
        request_path = Path(args[args.index("--request") + 1])
        report_path = Path(args[args.index("--report") + 1])
        with open(request_path) as fh:
            captured["request"] = fh.read()
        with open(report_path, "w") as fh:
            yaml.dump({"files": [], "lastmod_info": None}, fh)

        class _C:
            returncode = 0
            stdout = ""
            stderr = ""

        return _C()

    monkeypatch.setattr(_transporter1.subprocess, "run", fake)

    src = _transporter1.TransporterLocalFile("file:///a")
    tgt = _transporter1.TransporterLocalFile("file:///b")
    report = _output.copy_one(
        source_target_pairs=[(src, tgt)],
        work_dir=tmp_path,
        format=_transporter1.TransporterParquetFormat(),
    )
    assert isinstance(report, _transporter1.TransporterReport)
    assert "!CopyV1" in captured["request"]


def test_copy_one_run_error(monkeypatch, tmp_path):
    monkeypatch.setattr(
        _transporter, "find_transporter_binary", lambda: "/fake/bin",
    )
    _patch_subprocess_with_report(monkeypatch, returncode=3, stderr="bad copy")
    with pytest.raises(_transporter1.TransporterRunError) as ei:
        _output.copy_one(
            source_target_pairs=[
                (
                    _transporter1.TransporterLocalFile("file:///a"),
                    _transporter1.TransporterLocalFile("file:///b"),
                ),
            ],
            work_dir=tmp_path,
            format=_transporter1.TransporterParquetFormat(),
        )
    assert ei.value.returncode == 3


# --- _transporter1.path_to_file_uri --------------------------------------------


def test_path_to_file_uri_absolute_file(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("")
    uri = _transporter1.path_to_file_uri(p)
    assert uri.startswith("file://")
    assert uri.endswith("x.csv")


def test_path_to_file_uri_existing_dir_gets_trailing_slash(tmp_path):
    uri = _transporter1.path_to_file_uri(tmp_path)
    assert uri.endswith("/")


def test_path_to_file_uri_string_with_trailing_slash(tmp_path):
    nonexistent = str(tmp_path / "out") + "/"
    uri = _transporter1.path_to_file_uri(nonexistent)
    assert uri.endswith("/")


def test_path_to_file_uri_relative_resolved(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path("rel.csv").write_text("")
    uri = _transporter1.path_to_file_uri("rel.csv")
    assert uri.endswith("/rel.csv")
    assert "file://" in uri


