#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from tabsdatak.common._fileformat.taxonomy import EXCEL_SHEET_KEY
from tabsdatak.common._transporter.invoker import transporter2 as t
from tabsdatak.conn.common.file.typing_ import DestFileFormat, SrcFileFormat


# --- Server-side env var name constants --------------------------


def test_server_side_env_constants_match_tabsdata_os():
    assert t.SERVER_SIDE_AWS_ACCESS_KEY_ID == "LOCATION_AWS_ACCESS_KEY_ID"
    assert t.SERVER_SIDE_AWS_SECRET_ACCESS_KEY == "LOCATION_AWS_SECRET_ACCESS_KEY"
    assert t.SERVER_SIDE_AWS_REGION == "LOCATION_AWS_REGION"
    assert t.SERVER_SIDE_AZURE_ACCOUNT_NAME == "LOCATION_ACCOUNT_NAME"
    assert t.SERVER_SIDE_AZURE_ACCOUNT_KEY == "LOCATION_ACCOUNT_KEY"
    assert t.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON == "LOCATION_GCS_SERVICE_ACCOUNT_JSON"


# --- TransporterEnv / TransporterLiteral -------------------------


def test_transporter_env_wire_and_repr():
    e = t.TransporterEnv("FOO_BAR")
    assert e.name == "FOO_BAR"
    assert e.wire() == "env:FOO_BAR"
    assert "FOO_BAR" in repr(e)


def test_transporter_literal_wire_and_redacted_repr():
    lit = t.TransporterLiteral("super-secret")
    assert lit.value == "super-secret"
    assert lit.wire() == "val:super-secret"
    # repr must never leak the literal value.
    assert "super-secret" not in repr(lit)
    assert "***redacted***" in repr(lit)


# --- Domain location classes -------------------------------------


def test_transporter_s3_field_shape():
    loc = t.TransporterS3(
        url="s3://bucket/prefix/",
        access_key=t.TransporterEnv("LOCATION_AWS_ACCESS_KEY_ID"),
        secret_key=t.TransporterEnv("LOCATION_AWS_SECRET_ACCESS_KEY"),
        region=t.TransporterLiteral("us-east-1"),
        extra_configs={"endpoint": "https://localhost:9000"},
    )
    assert loc.url == "s3://bucket/prefix/"
    assert loc.region.value == "us-east-1"
    assert loc.extra_configs["endpoint"] == "https://localhost:9000"


def test_transporter_s3_optional_region_none():
    loc = t.TransporterS3(
        url="s3://b/",
        access_key=t.TransporterEnv("X"),
        secret_key=t.TransporterEnv("Y"),
    )
    assert loc.region is None


def test_transporter_local_file_field_shape():
    loc = t.TransporterLocalFile("file:///tmp/foo")
    assert loc.url == "file:///tmp/foo"


# --- to_location (domain -> wire) --------------------------------


def test_to_location_local_has_no_configs():
    loc = t.to_location(t.TransporterLocalFile("file:///tmp/x"))
    assert isinstance(loc, t.Location)
    assert loc.uri == "file:///tmp/x"
    assert loc.configs == {}


def test_to_location_s3_renders_env_and_literal_refs():
    loc = t.to_location(
        t.TransporterS3(
            url="s3://b/k",
            access_key=t.TransporterEnv("AK"),
            secret_key=t.TransporterLiteral("sek"),
            region=t.TransporterEnv("RG"),
            extra_configs={"endpoint": "https://x", "allow_http": "true"},
        )
    )
    assert loc.uri == "s3://b/k"
    assert loc.configs == {
        "access_key_id": "env:AK",
        "secret_access_key": "val:sek",
        "region": "env:RG",
        "endpoint": "https://x",
        "allow_http": "true",
    }


def test_to_location_s3_omits_region_when_none():
    loc = t.to_location(
        t.TransporterS3(
            url="s3://b/k",
            access_key=t.TransporterEnv("AK"),
            secret_key=t.TransporterEnv("SK"),
        )
    )
    assert "region" not in loc.configs


def test_to_location_gcs():
    loc = t.to_location(
        t.TransporterGCS(
            url="gs://b/k",
            service_account_key=t.TransporterEnv("SA"),
        )
    )
    assert loc.uri == "gs://b/k"
    assert loc.configs == {"service_account_key": "env:SA"}


def test_to_location_azure():
    loc = t.to_location(
        t.TransporterAzure(
            url="az://c/k",
            account_name=t.TransporterEnv("AN"),
            account_key=t.TransporterEnv("AKY"),
        )
    )
    assert loc.uri == "az://c/k"
    assert loc.configs == {"account_name": "env:AN", "account_key": "env:AKY"}


def test_to_location_unsupported_raises():
    class _Other(t.TransporterLocation):
        pass

    with pytest.raises(ValueError, match="unsupported transporter location"):
        t.to_location(_Other())


# --- to_format_spec / CSV options --------------------------------


def test_to_format_spec_simple_formats():
    assert t.to_format_spec(t.TransporterParquetFormat()) == t.FormatSpec("parquet")
    assert t.to_format_spec(t.TransporterAvroFormat()) == t.FormatSpec("avro")
    # The connector's JSON is newline-delimited JSON; log -> text.
    assert t.to_format_spec(t.TransporterJsonFormat()) == t.FormatSpec("ndjson")
    assert t.to_format_spec(t.TransporterLogFormat()) == t.FormatSpec("text")


def test_to_format_spec_csv_char_separator():
    spec = t.to_format_spec(
        t.TransporterCSVFormat({"separator": ";", "has_header": False})
    )
    assert spec.format == "csv"
    assert spec.options == t.CsvOptions(has_header=False, delimiter=";")


def test_to_format_spec_csv_int_separator_is_chr():
    # callers may pass a byte/char code instead of a one-char string.
    spec = t.to_format_spec(t.TransporterCSVFormat({"separator": 59}))
    assert spec.options.delimiter == ";"  # chr(59)


def test_to_format_spec_csv_dest_include_header_alias():
    spec = t.to_format_spec(t.TransporterCSVFormat({"include_header": False}))
    assert spec.options.has_header is False


def test_to_format_spec_csv_defaults_when_empty():
    spec = t.to_format_spec(t.TransporterCSVFormat())
    assert spec.options == t.CsvOptions(has_header=True, delimiter=",")


def test_to_format_spec_excel_carries_the_sheet():
    spec = t.to_format_spec(t.TransporterExcelFormat(sheet=3))
    assert spec == t.FormatSpec(format="excel", options=t.ExcelOptions(sheet=3))


def test_to_format_spec_excel_defaults_to_the_first_sheet():
    assert t.to_format_spec(t.TransporterExcelFormat()) == t.FormatSpec(
        format="excel", options=t.ExcelOptions(sheet=1)
    )


def test_one_options_field_carries_whichever_block_the_format_takes():
    # There is no per-format option field: the binary reads `options` against
    # the sibling `format`, so the two blocks share the one key.
    assert isinstance(t.to_format_spec(t.TransporterCSVFormat()).options, t.CsvOptions)
    assert isinstance(
        t.to_format_spec(t.TransporterExcelFormat()).options, t.ExcelOptions
    )
    assert t.to_format_spec(t.TransporterParquetFormat()).options is None


def test_dump_excel_source_format_matches_the_binary_field_names():
    # Drift here is a runtime YAML parse failure in the binary rather than a
    # Python one, so the emitted keys are pinned: an untagged `options` block
    # beside `format`, with `sheet` inside it.
    req = t.FindCopyRequest(
        source=t.Location(uri="s3://b/2026/q2.xlsx"),
        target_dir=t.Location(uri="file:///work/slot0"),
        source_format=t.to_format_spec(t.TransporterExcelFormat(sheet=2)),
        target_format=t.FormatSpec(format="parquet"),
    )
    dumped = t._dump(req)
    assert dumped["source_format"] == {
        "format": "excel",
        "options": {"sheet": 2, "has_header": True},
    }
    assert dumped["target_format"] == {"format": "parquet"}
    # ...and it survives a real YAML round trip, which is what the binary reads.
    assert yaml.safe_load(yaml.safe_dump(dumped)) == dumped


def test_to_format_spec_unsupported_raises():
    class _Other(t.TransporterFormat):
        pass

    with pytest.raises(ValueError, match="unsupported transporter format"):
        t.to_format_spec(_Other())


# --- Generic request serialization (_dump) -----------------------


def test_dump_find_copy_drops_none_and_empty_configs():
    req = t.FindCopyRequest(
        source=t.Location("s3://b/in/*.csv", configs={"region": "env:R"}),
        target_dir=t.Location("file:///out/"),  # no configs
        source_format=t.FormatSpec("csv", t.CsvOptions(has_header=True, delimiter=",")),
        target_format=t.FormatSpec("parquet"),
        decompress="auto",
        # compress / max_files / lastmods left as None
    )
    wire = t._dump(req)
    assert wire == {
        "source": {"uri": "s3://b/in/*.csv", "configs": {"region": "env:R"}},
        "target_dir": {"uri": "file:///out/"},  # empty configs omitted
        "source_format": {
            "format": "csv",
            "options": {"has_header": True, "delimiter": ","},
        },
        "target_format": {"format": "parquet"},
        "decompress": "auto",
    }
    assert "compress" not in wire
    assert "max_files" not in wire
    assert "lastmod_info" not in wire
    assert "initial_lastmod" not in wire


def test_dump_find_copy_includes_optionals_when_set():
    req = t.FindCopyRequest(
        source=t.Location("file:///in/*.parquet"),
        target_dir=t.Location("file:///out/"),
        max_files=3,
        initial_lastmod="2026-01-01T00:00:00Z",
        lastmod_info="cursor-blob",
    )
    wire = t._dump(req)
    assert wire["max_files"] == 3
    assert wire["initial_lastmod"] == "2026-01-01T00:00:00Z"
    assert wire["lastmod_info"] == "cursor-blob"
    # byte-move shape: no formats at all
    assert "source_format" not in wire
    assert "target_format" not in wire


def test_dump_copy_aliases_from_keyword():
    req = t.CopyRequest(
        pairs=[
            t.CopyPair(
                from_=t.Location("file:///a.parquet"),
                to=t.Location("s3://b/c.csv", configs={"region": "env:R"}),
            )
        ],
        source_format=t.FormatSpec("parquet"),
        target_format=t.FormatSpec("csv", t.CsvOptions()),
    )
    wire = t._dump(req)
    assert wire["pairs"] == [
        {
            "from": {"uri": "file:///a.parquet"},
            "to": {"uri": "s3://b/c.csv", "configs": {"region": "env:R"}},
        }
    ]
    # the Python field is `from_`; the wire key must be `from`.
    assert "from_" not in wire["pairs"][0]


def test_dump_find_request_minimal():
    wire = t._dump(t.FindRequest(source=t.Location("file:///in/*.csv")))
    assert wire == {"source": {"uri": "file:///in/*.csv"}}


def test_dump_delete_request():
    wire = t._dump(
        t.DeleteRequest(
            files=[t.Location("file:///a"), t.Location("s3://b/c", {"region": "env:R"})]
        )
    )
    assert wire == {
        "files": [
            {"uri": "file:///a"},
            {"uri": "s3://b/c", "configs": {"region": "env:R"}},
        ]
    }


def test_dump_request_is_plain_yaml_safe(tmp_path):
    # _dump output must be safe_dump-able (plain dict/list/str/bool/int).
    req = t.CopyRequest(
        pairs=[t.CopyPair(from_=t.Location("file:///a"), to=t.Location("file:///b"))]
    )
    text = yaml.safe_dump(t._dump(req), sort_keys=False)
    assert "!!" not in text  # no python object tags
    assert yaml.safe_load(text) == t._dump(req)


# --- Generic response parsing (_load / _coerce) ------------------


def test_load_copy_response_rewrites_local_uris_and_aliases_from():
    data = yaml.safe_load(
        """
        files:
          - from: s3://bucket/in/data-001.csv.gz
            to: file:///data/in/data-001.parquet
            size: 524288
            rows: 10000
            last_modified: 2026-01-02T03:04:05Z
            started_at: 2026-01-02T03:05:00Z
            ended_at: 2026-01-02T03:05:02Z
        lastmod_info: cursor-blob
        """
    )
    resp = t._load(t.CopyResponse, data)
    assert isinstance(resp, t.CopyResponse)
    assert resp.lastmod_info == "cursor-blob"
    assert len(resp.files) == 1
    f = resp.files[0]
    assert isinstance(f, t.CopiedFile)
    # cloud uri passes through; local file:// uri rewritten to an OS path.
    assert f.from_ == "s3://bucket/in/data-001.csv.gz"
    assert not f.to.startswith("file://")
    assert Path(f.to) == Path("/data/in/data-001.parquet")
    assert f.size == 524288
    assert f.rows == 10000


def test_load_copy_response_byte_move_has_no_rows():
    data = yaml.safe_load(
        "files:\n"
        "  - {from: file:///a.parquet, to: file:///b.parquet, size: 10, "
        "last_modified: t, started_at: a, ended_at: b}\n"
    )
    resp = t._load(t.CopyResponse, data)
    assert resp.files[0].rows is None  # optional field absent -> None
    assert resp.lastmod_info is None


def test_load_find_response_keeps_uris():
    data = yaml.safe_load(
        "files:\n"
        "  - {uri: file:///d/x.csv, size: 3, last_modified: t}\n"
        "  - {uri: s3://b/y.csv, size: 4, last_modified: t}\n"
        "lastmod_info: cur\n"
    )
    resp = t._load(t.FindResponse, data)
    assert isinstance(resp, t.FindResponse)
    # `uri` is a listing, NOT a from/to pair -> left as a URI (not path-rewritten).
    assert resp.files[0].uri == "file:///d/x.csv"
    assert resp.files[1].uri == "s3://b/y.csv"
    assert resp.lastmod_info == "cur"


def test_load_delete_response():
    data = yaml.safe_load(
        "deleted: [s3://b/a, s3://b/b]\n"
        "failed:\n"
        "  - {uri: s3://b/missing, error: not found}\n"
    )
    resp = t._load(t.DeleteResponse, data)
    assert resp.deleted == ["s3://b/a", "s3://b/b"]
    assert len(resp.failed) == 1
    assert isinstance(resp.failed[0], t.DeleteFailure)
    assert resp.failed[0].uri == "s3://b/missing"
    assert resp.failed[0].error == "not found"


def test_transporter_report_is_copy_response_alias():
    assert t.TransporterReport is t.CopyResponse


# --- _load_response (file handling + error cases) ----------------


def _write_response(path: Path, body: str) -> Path:
    path.write_text(body)
    return path


def test_load_response_missing_file_raises(tmp_path):
    with pytest.raises(t.TransporterReportError, match="not found"):
        t._load_response(tmp_path / "nope.yaml", t.CopyResponse)


def test_load_response_unparseable_raises(tmp_path):
    p = _write_response(tmp_path / "r.yaml", "not: valid: yaml: : :\n")
    with pytest.raises(t.TransporterReportError, match="failed to parse"):
        t._load_response(p, t.CopyResponse)


def test_load_response_non_mapping_raises(tmp_path):
    p = _write_response(tmp_path / "r.yaml", "- a\n- b\n")
    with pytest.raises(t.TransporterReportError, match="not a mapping"):
        t._load_response(p, t.CopyResponse)


def test_load_response_error_only_raises(tmp_path):
    p = _write_response(tmp_path / "r.yaml", "error: something blew up\n")
    with pytest.raises(t.TransporterReportError, match="something blew up"):
        t._load_response(p, t.CopyResponse)


# --- TransporterRunError -----------------------------------------


def test_run_error_carries_streams():
    err = t.TransporterRunError(2, "stdout-data", "stderr-data")
    assert err.returncode == 2
    assert err.stdout == "stdout-data"
    assert err.stderr == "stderr-data"
    assert "stderr-data" in str(err)
    assert "td_transporter2" in str(err)


def test_run_error_falls_back_to_stdout_when_stderr_empty():
    err = t.TransporterRunError(1, "stdout-data", "")
    assert "stdout-data" in str(err)


# --- run_binary (argv assembly) ----------------------------------


def _fake_run(monkeypatch, *, capture=None, returncode=0, stderr="", response=None):
    """Patch ``subprocess.run`` to record argv/env, optionally write a
    response YAML, and return a fake completed-process with ``returncode``."""

    def fake(args, **kwargs):
        if capture is not None:
            capture["args"] = list(args)
            capture["env"] = kwargs.get("env")
            req_idx = args.index("--request") + 1
            capture["request"] = yaml.safe_load(Path(args[req_idx]).read_text())
        if returncode == 0:
            resp_path = Path(args[args.index("--response") + 1])
            resp_path.parent.mkdir(parents=True, exist_ok=True)
            resp_path.write_text(
                yaml.safe_dump(response if response is not None else {"files": []})
            )

        class _C:
            pass

        c = _C()
        c.returncode = returncode
        c.stdout = ""
        c.stderr = stderr
        return c

    monkeypatch.setattr(t.subprocess, "run", fake)


def test_run_binary_builds_argv_with_subcommand_and_extra_args(monkeypatch, tmp_path):
    capture: dict[str, Any] = {}
    _fake_run(monkeypatch, capture=capture)
    req = tmp_path / "req.yaml"
    req.write_text("source: {uri: 'file:///x'}\n")
    resp = tmp_path / "resp.yaml"
    t.run_binary(
        "find-copy",
        req,
        resp,
        env=None,
        binary_path="/explicit/td_transporter2",
        extra_args=("--alpha-suborder",),
    )
    args = capture["args"]
    assert args[0] == "/explicit/td_transporter2"
    assert args[1] == "find-copy"
    assert "--request" in args and "--response" in args
    assert args[-1] == "--alpha-suborder"


def test_run_binary_merges_env(monkeypatch, tmp_path):
    capture: dict[str, Any] = {}
    _fake_run(monkeypatch, capture=capture)
    req = tmp_path / "req.yaml"
    req.write_text("x: 1\n")
    t.run_binary(
        "find",
        req,
        tmp_path / "resp.yaml",
        env={"LOCATION_AWS_ACCESS_KEY_ID": "AKIAFAKE"},
        binary_path="/fake/bin",
    )
    # process env is a copy of os.environ + overrides
    assert capture["env"]["LOCATION_AWS_ACCESS_KEY_ID"] == "AKIAFAKE"


def test_run_binary_nonzero_raises_run_error(monkeypatch, tmp_path):
    _fake_run(monkeypatch, returncode=7, stderr="boom")
    req = tmp_path / "req.yaml"
    req.write_text("x: 1\n")
    with pytest.raises(t.TransporterRunError) as ei:
        t.run_binary(
            "copy", req, tmp_path / "resp.yaml", env=None, binary_path="/fake/bin"
        )
    assert ei.value.returncode == 7
    assert "boom" in str(ei.value)


# --- Drivers: find / find_copy / copy / delete -------------------


def test_find_copy_subcommand_request_and_parse(monkeypatch, tmp_path):
    capture: dict[str, Any] = {}
    _fake_run(
        monkeypatch,
        capture=capture,
        response={
            "files": [
                {
                    "from": "file:///in/a.csv",
                    "to": str(tmp_path / "a.parquet"),
                    "size": 12,
                    "rows": 2,
                    "last_modified": "t",
                    "started_at": "s",
                    "ended_at": "e",
                }
            ],
            "lastmod_info": "cur",
        },
    )
    req = t.FindCopyRequest(
        source=t.to_location(t.TransporterLocalFile("file:///in/*.csv")),
        target_dir=t.to_location(t.TransporterLocalFile(t.path_to_file_uri(tmp_path))),
        source_format=t.to_format_spec(t.TransporterCSVFormat({})),
        target_format=t.FormatSpec("parquet"),
        decompress="auto",
    )
    report = t.find_copy(req, tmp_path, binary_path="/fake/bin")
    assert capture["args"][1] == "find-copy"
    assert "--alpha-suborder" not in capture["args"]  # default off
    assert capture["request"]["source_format"] == {
        "format": "csv",
        "options": {"has_header": True, "delimiter": ","},
    }
    assert capture["request"]["decompress"] == "auto"
    assert isinstance(report, t.CopyResponse)
    assert report.files[0].rows == 2
    assert report.files[0].to == str(tmp_path / "a.parquet")
    assert report.lastmod_info == "cur"


def test_find_copy_alpha_suborder_appends_flag(monkeypatch, tmp_path):
    capture: dict[str, Any] = {}
    _fake_run(
        monkeypatch, capture=capture, response={"files": [], "lastmod_info": None}
    )
    req = t.FindCopyRequest(
        source=t.Location("file:///in/*.csv"),
        target_dir=t.Location(t.path_to_file_uri(tmp_path)),
    )
    t.find_copy(req, tmp_path, alpha_suborder=True, binary_path="/fake/bin")
    assert capture["args"][1] == "find-copy"
    assert "--alpha-suborder" in capture["args"]


def test_find_subcommand_and_parse(monkeypatch, tmp_path):
    capture: dict[str, Any] = {}
    _fake_run(
        monkeypatch,
        capture=capture,
        response={
            "files": [{"uri": "file:///in/a.csv", "size": 3, "last_modified": "t"}],
            "lastmod_info": "cur",
        },
    )
    resp = t.find(
        t.FindRequest(source=t.Location("file:///in/*.csv")),
        tmp_path,
        alpha_suborder=True,
        binary_path="/fake/bin",
    )
    assert capture["args"][1] == "find"
    assert "--alpha-suborder" in capture["args"]
    assert isinstance(resp, t.FindResponse)
    assert resp.files[0].uri == "file:///in/a.csv"
    assert resp.lastmod_info == "cur"


def test_copy_subcommand_request_and_parse(monkeypatch, tmp_path):
    capture: dict[str, Any] = {}
    _fake_run(
        monkeypatch,
        capture=capture,
        response={
            "files": [
                {
                    "from": str(tmp_path / "in.parquet"),
                    "to": "s3://b/out.csv",
                    "size": 9,
                    "rows": 1,
                    "last_modified": "t",
                    "started_at": "s",
                    "ended_at": "e",
                }
            ]
        },
    )
    req = t.CopyRequest(
        pairs=[
            t.CopyPair(
                from_=t.to_location(
                    t.TransporterLocalFile(t.path_to_file_uri(tmp_path / "in.parquet"))
                ),
                to=t.to_location(
                    t.TransporterS3(
                        url="s3://b/out.csv",
                        access_key=t.TransporterEnv("AK"),
                        secret_key=t.TransporterEnv("SK"),
                        region=t.TransporterEnv("RG"),
                    )
                ),
            )
        ],
        source_format=t.FormatSpec("parquet"),
        target_format=t.to_format_spec(t.TransporterCSVFormat({})),
    )
    report = t.copy(req, tmp_path, binary_path="/fake/bin")
    assert capture["args"][1] == "copy"
    # request carries the from/to pair with the from alias + env-rendered creds
    pair = capture["request"]["pairs"][0]
    assert "from" in pair and "from_" not in pair
    assert pair["to"]["configs"]["access_key_id"] == "env:AK"
    assert isinstance(report, t.CopyResponse)
    assert report.files[0].rows == 1
    # cloud target uri passes through unchanged
    assert report.files[0].to == "s3://b/out.csv"


def test_delete_subcommand_and_parse(monkeypatch, tmp_path):
    capture: dict[str, Any] = {}
    _fake_run(
        monkeypatch,
        capture=capture,
        response={
            "deleted": ["s3://b/a"],
            "failed": [{"uri": "s3://b/x", "error": "nope"}],
        },
    )
    resp = t.delete(
        t.DeleteRequest(files=[t.Location("s3://b/a", {"region": "env:R"})]),
        tmp_path,
        binary_path="/fake/bin",
    )
    assert capture["args"][1] == "delete"
    assert capture["request"]["files"][0]["uri"] == "s3://b/a"
    assert isinstance(resp, t.DeleteResponse)
    assert resp.deleted == ["s3://b/a"]
    assert resp.failed[0].error == "nope"


def test_driver_run_error_propagates(monkeypatch, tmp_path):
    _fake_run(monkeypatch, returncode=4, stderr="bad copy")
    with pytest.raises(t.TransporterRunError) as ei:
        t.copy(
            t.CopyRequest(
                pairs=[
                    t.CopyPair(
                        from_=t.Location("file:///a"), to=t.Location("file:///b")
                    )
                ]
            ),
            tmp_path,
            binary_path="/fake/bin",
        )
    assert ei.value.returncode == 4


# --- new_request_paths / URI helpers -----------------------------


def test_new_request_paths_distinct_and_under_work_dir(tmp_path):
    req, resp = t.new_request_paths(tmp_path / "work")
    assert req.parent == tmp_path / "work"
    assert (tmp_path / "work").is_dir()  # created
    assert req != resp
    assert req.name.startswith("request_") and req.suffix == ".yaml"
    assert resp.name.startswith("response_") and resp.suffix == ".yaml"
    # two calls never collide
    req2, _ = t.new_request_paths(tmp_path / "work")
    assert req2 != req


def test_path_to_file_uri_absolute_file(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("")
    uri = t.path_to_file_uri(p)
    assert uri.startswith("file://")
    assert uri.endswith("x.csv")


def test_path_to_file_uri_existing_dir_gets_trailing_slash(tmp_path):
    assert t.path_to_file_uri(tmp_path).endswith("/")


def test_file_uri_to_path_roundtrip_and_passthrough(tmp_path):
    p = tmp_path / "a.csv"
    uri = t.path_to_file_uri(p)
    assert t._file_uri_to_path(uri) == str(p)
    # non-file URIs pass through untouched
    assert t._file_uri_to_path("s3://b/k") == "s3://b/k"


# --- Format mappers (AUTO inference) -----------------------------


def test_resolve_auto_src_format_uniform():
    assert (
        t.resolve_auto_src_format(SrcFileFormat.AUTO, ["a.csv", "b.csv"])
        is SrcFileFormat.CSV
    )
    # already concrete -> unchanged
    assert (
        t.resolve_auto_src_format(SrcFileFormat.PARQUET, ["a.csv"])
        is SrcFileFormat.PARQUET
    )


def test_resolve_auto_src_format_infers_excel_from_xlsx():
    assert (
        t.resolve_auto_src_format(SrcFileFormat.AUTO, ["q1.xlsx", "q2.xlsx"])
        is SrcFileFormat.EXCEL
    )


def test_excel_is_source_only():
    # There is no Excel writer, so the dest side must not infer it.
    assert not hasattr(DestFileFormat, "EXCEL")
    with pytest.raises(Exception):
        t.resolve_auto_dest_format(DestFileFormat.AUTO, ["out.xlsx"])


def test_resolve_auto_src_format_mixed_raises():
    with pytest.raises(Exception):
        t.resolve_auto_src_format(SrcFileFormat.AUTO, ["a.csv", "b.parquet"])


def test_resolve_auto_src_format_unknown_extension_raises():
    with pytest.raises(Exception):
        t.resolve_auto_src_format(SrcFileFormat.AUTO, ["a.weird"])


def test_resolve_auto_dest_format_uniform():
    assert (
        t.resolve_auto_dest_format(DestFileFormat.AUTO, ["a.parquet"])
        is DestFileFormat.PARQUET
    )


def test_src_format_to_transporter_maps_each():
    assert isinstance(
        t.src_format_to_transporter(SrcFileFormat.CSV, None), t.TransporterCSVFormat
    )
    assert isinstance(
        t.src_format_to_transporter(SrcFileFormat.JSON, None), t.TransporterJsonFormat
    )
    assert isinstance(
        t.src_format_to_transporter(SrcFileFormat.AVRO, None), t.TransporterAvroFormat
    )
    assert isinstance(
        t.src_format_to_transporter(SrcFileFormat.PARQUET, None),
        t.TransporterParquetFormat,
    )
    assert isinstance(
        t.src_format_to_transporter(SrcFileFormat.LOG, None), t.TransporterLogFormat
    )
    assert isinstance(
        t.src_format_to_transporter(SrcFileFormat.EXCEL, None), t.TransporterExcelFormat
    )


def test_src_format_to_transporter_excel_reads_has_header_from_the_cfg():
    # Unlike the sheet, this one really is a user option: it stays in the
    # cfg dict and is read, not popped.
    assert t.src_format_to_transporter(SrcFileFormat.EXCEL, None).has_header is True
    assert t.src_format_to_transporter(SrcFileFormat.EXCEL, {}).has_header is True
    off = t.src_format_to_transporter(SrcFileFormat.EXCEL, {"has_header": False})
    assert off.has_header is False
    assert t.to_format_spec(off).options == t.ExcelOptions(sheet=1, has_header=False)


def test_the_two_excel_options_are_independent():
    fmt = t.src_format_to_transporter(
        SrcFileFormat.EXCEL, {EXCEL_SHEET_KEY: 4, "has_header": False}
    )
    assert (fmt.sheet, fmt.has_header) == (4, False)


def test_src_format_to_transporter_excel_takes_the_sheet():
    cfg = {EXCEL_SHEET_KEY: 4}
    assert t.src_format_to_transporter(SrcFileFormat.EXCEL, cfg).sheet == 4
    # The carrier key is consumed, never passed on as an option.
    assert cfg == {}
    # No fragment on the path -> the first sheet, as the binary also defaults.
    assert t.src_format_to_transporter(SrcFileFormat.EXCEL, None).sheet == 1
    assert t.src_format_to_transporter(SrcFileFormat.EXCEL, {}).sheet == 1


def test_a_sheet_never_reaches_another_format_as_an_option():
    with pytest.raises(Exception):
        t.src_format_to_transporter(SrcFileFormat.PARQUET, {EXCEL_SHEET_KEY: 4})
    # ...and a csv cfg that never had one is passed through untouched.
    csv = t.src_format_to_transporter(SrcFileFormat.CSV, {"separator": ";"})
    assert csv.options["separator"] == ";"
    assert EXCEL_SHEET_KEY not in csv.options


def test_dest_format_to_transporter_maps_each():
    assert isinstance(
        t.dest_format_to_transporter(DestFileFormat.CSV, None), t.TransporterCSVFormat
    )
    assert isinstance(
        t.dest_format_to_transporter(DestFileFormat.PARQUET, None),
        t.TransporterParquetFormat,
    )


# --- EBCDIC / copybook sources -----------------------------------


def test_ebcdic_is_source_only():
    assert SrcFileFormat.EBCDIC.value == "EBCDIC"
    assert not hasattr(DestFileFormat, "EBCDIC")


def test_auto_never_infers_ebcdic():
    # A mainframe extract has no conventional extension, so there is nothing
    # to infer from and the format must always be named.
    for path in ["rec.dat", "rec.bin", "rec.ebc", "rec"]:
        with pytest.raises(Exception):
            t.resolve_auto_src_format(SrcFileFormat.AUTO, [path])


def _copybook(record: str) -> str:
    """A minimal copybook source. `copybook` carries the COBOL itself, not a
    path to it, so fixtures have to be real (if tiny) layouts."""
    return f"      01  {record}.\n          05  FIELD PIC X(4).\n"


def test_src_format_to_transporter_ebcdic_takes_the_copybook_source():
    copybook = _copybook("CUSTOMER")
    fmt = t.src_format_to_transporter(SrcFileFormat.EBCDIC, {"copybook": copybook})
    assert isinstance(fmt, t.TransporterEbcdicFormat)
    assert fmt.copybook == copybook


def test_an_un_narrowed_copybook_list_is_a_wiring_bug_not_a_silent_first_entry():
    # A list here means `format_cfg_for` did not narrow it; taking entry 0
    # would read every path against the first path's copybook.
    with pytest.raises(Exception, match="list of 2"):
        t.src_format_to_transporter(
            SrcFileFormat.EBCDIC,
            {"copybook": [_copybook("JAN"), _copybook("FEB")]},
        )


def test_the_code_page_defaults_to_cp037():
    copybook = _copybook("REC")
    fmt = t.src_format_to_transporter(SrcFileFormat.EBCDIC, {"copybook": copybook})
    assert fmt.codepage == "cp037"
    other = t.src_format_to_transporter(
        SrcFileFormat.EBCDIC, {"copybook": copybook, "codepage": "cp273"}
    )
    assert other.codepage == "cp273"


@pytest.mark.parametrize("cfg", [None, {}, {"codepage": "cp037"}, {"copybook": ""}])
def test_ebcdic_without_a_copybook_is_refused(cfg):
    # The data carries no schema at all, so there is nothing to fall back to.
    with pytest.raises(Exception, match="copybook"):
        t.src_format_to_transporter(SrcFileFormat.EBCDIC, cfg)


def test_dump_ebcdic_source_format_matches_the_binary_field_names():
    # Drift is a runtime parse failure in the binary, so the keys are pinned.
    req = t.FindCopyRequest(
        source=t.Location(uri="s3://b/extract.dat"),
        target_dir=t.Location(uri="file:///work/slot0"),
        source_format=t.to_format_spec(
            t.TransporterEbcdicFormat(copybook=_copybook("REC"), codepage="cp500")
        ),
        target_format=t.FormatSpec(format="parquet"),
    )
    dumped = t._dump(req)
    assert dumped["source_format"] == {
        "format": "ebcdic",
        "options": {
            # The copybook source itself, inline: the binary fetches nothing.
            "copybook": _copybook("REC"),
            "codepage": "cp500",
        },
    }
    assert yaml.safe_load(yaml.safe_dump(dumped)) == dumped


def test_the_binary_only_knobs_are_left_off_the_wire():
    # `copybook_text`, `keep_filler`, `hfp` and `trim` are not reachable from
    # a connector's format_cfg, so nothing is emitted for them and the binary
    # applies its own defaults.
    dumped = t._dump(
        t.to_format_spec(t.TransporterEbcdicFormat(copybook=_copybook("REC")))
    )
    assert set(dumped["options"]) == {"copybook", "codepage"}


def test_a_multiline_copybook_survives_the_yaml_round_trip():
    # Newlines and leading columns are load-bearing: code starts at column 8.
    copybook = (
        "      01  REC.\n      05  NAME PIC X(20).\n      05  QTY  PIC S9(5) COMP-3.\n"
    )
    spec = t.to_format_spec(t.TransporterEbcdicFormat(copybook=copybook))
    dumped = t._dump(spec)
    assert yaml.safe_load(yaml.safe_dump(dumped))["options"]["copybook"] == copybook
