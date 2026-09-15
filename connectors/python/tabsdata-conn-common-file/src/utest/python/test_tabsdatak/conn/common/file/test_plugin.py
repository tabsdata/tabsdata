#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from tabsdatak.common._error import CommonInitException
from tabsdatak.common._fileformat.error import FileFormatErrorCode
from tabsdatak.common._fileformat.taxonomy import EXCEL_SHEET_KEY
from tabsdatak.common._transporter.handler import handler2
from tabsdatak.common._transporter.handler.handler import TransporterReport
from tabsdatak.common._transporter.invoker import transporter1, transporter2
from tabsdatak.conn.common.constants import SRC_META_DROP_KEY
from tabsdatak.conn.common.error import ConnException
from tabsdatak.conn.common.file import _plugin
from tabsdatak.conn.common.file.constants import FILE_META_PATH
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    SrcFileFormat,
)
from tabsdatak.spi import DestContext

# --- transporter1.src_format_to_transporter ----------------------------------


@pytest.mark.parametrize(
    "fmt,expected_cls",
    [
        (SrcFileFormat.CSV, "TransporterCSVFormat"),
        (SrcFileFormat.JSON, "TransporterJsonFormat"),
        (SrcFileFormat.AVRO, "TransporterAvroFormat"),
        (SrcFileFormat.PARQUET, "TransporterParquetFormat"),
        (SrcFileFormat.LOG, "TransporterLogFormat"),
    ],
)
def test_src_format_to_transporter_picks_right_class(fmt, expected_cls):
    out = transporter1.src_format_to_transporter(fmt, None)
    assert type(out).__name__ == expected_cls


def test_src_format_to_transporter_rejects_auto():
    """AUTO must be resolved via ``resolve_auto_src_format`` before
    reaching the transporter mapping; reaching this function with AUTO
    is a programming error."""
    with pytest.raises(CommonInitException):
        transporter1.src_format_to_transporter(SrcFileFormat.AUTO, None)


def test_src_format_to_transporter_rejects_excel():
    """transporter1 is the legacy polars driver and has no Excel reader.
    Adding ``EXCEL`` to the shared enum must therefore surface as an
    explicit unsupported-format error here, not as a silent fallthrough."""
    with pytest.raises(CommonInitException):
        transporter1.src_format_to_transporter(SrcFileFormat.EXCEL, None)


def test_resolve_auto_src_format_does_not_infer_excel():
    """...and AUTO must not route an .xlsx path to it either."""
    with pytest.raises(CommonInitException):
        transporter1.resolve_auto_src_format(SrcFileFormat.AUTO, ["q2.xlsx"])


def test_src_format_csv_carries_options():
    out = transporter1.src_format_to_transporter(SrcFileFormat.CSV, {"separator": 59})
    # User option layered on top of the binary-required defaults.
    assert out.options["separator"] == 59
    assert "has_header" in out.options


# --- transporter1.resolve_auto_src_format ------------------------------------


@pytest.mark.parametrize(
    "fmt",
    [
        SrcFileFormat.CSV,
        SrcFileFormat.JSON,
        SrcFileFormat.AVRO,
        SrcFileFormat.PARQUET,
        SrcFileFormat.LOG,
    ],
)
def test_resolve_auto_src_format_passes_explicit_through(fmt):
    """An already-concrete format is returned unchanged regardless of paths."""
    assert transporter1.resolve_auto_src_format(fmt, ["whatever.bin"]) is fmt


@pytest.mark.parametrize(
    "path,expected",
    [
        ("people.csv", SrcFileFormat.CSV),
        ("data.json", SrcFileFormat.JSON),
        ("events.jsonl", SrcFileFormat.JSON),
        ("events.ndjson", SrcFileFormat.JSON),
        ("avro_file.avro", SrcFileFormat.AVRO),
        ("rows.parquet", SrcFileFormat.PARQUET),
        ("rows.pq", SrcFileFormat.PARQUET),
        ("app.log", SrcFileFormat.LOG),
        ("nested/dir/people.csv", SrcFileFormat.CSV),
        ("People.CSV", SrcFileFormat.CSV),  # case-insensitive
        ("*.parquet", SrcFileFormat.PARQUET),  # glob keeps the extension
    ],
)
def test_resolve_auto_src_format_infers_from_single_path(path, expected):
    assert transporter1.resolve_auto_src_format(SrcFileFormat.AUTO, [path]) is expected


def test_resolve_auto_src_format_accepts_multiple_paths_with_same_ext():
    """Multiple paths whose extensions agree resolve to that one format."""
    out = transporter1.resolve_auto_src_format(
        SrcFileFormat.AUTO, ["a.csv", "b.csv", "nested/c.csv"]
    )
    assert out is SrcFileFormat.CSV


def test_resolve_auto_src_format_raises_on_mixed_extensions():
    with pytest.raises(CommonInitException) as ei:
        transporter1.resolve_auto_src_format(SrcFileFormat.AUTO, ["a.csv", "b.parquet"])
    msg = str(ei.value)
    assert "mixed" in msg.lower()


def test_resolve_auto_src_format_raises_on_unknown_extension():
    with pytest.raises(CommonInitException) as ei:
        transporter1.resolve_auto_src_format(SrcFileFormat.AUTO, ["data.bin"])
    assert "no recognised extension" in str(ei.value)


def test_resolve_auto_src_format_raises_on_extensionless_path():
    """A path with no extension can't be inferred."""
    with pytest.raises(CommonInitException):
        transporter1.resolve_auto_src_format(SrcFileFormat.AUTO, ["data/"])


# --- transporter1.dest_format_to_transporter ---------------------------------


@pytest.mark.parametrize(
    "fmt,expected_cls",
    [
        (DestFileFormat.CSV, "TransporterCSVFormat"),
        (DestFileFormat.JSON, "TransporterJsonFormat"),
        (DestFileFormat.AVRO, "TransporterAvroFormat"),
        (DestFileFormat.PARQUET, "TransporterParquetFormat"),
    ],
)
def test_dest_format_to_transporter_picks_right_class(fmt, expected_cls):
    out = transporter1.dest_format_to_transporter(fmt, None)
    assert type(out).__name__ == expected_cls


def test_dest_format_to_transporter_rejects_auto():
    with pytest.raises(CommonInitException):
        transporter1.dest_format_to_transporter(DestFileFormat.AUTO, None)


# --- transporter1.resolve_auto_dest_format -----------------------------------


@pytest.mark.parametrize(
    "fmt",
    [
        DestFileFormat.CSV,
        DestFileFormat.JSON,
        DestFileFormat.AVRO,
        DestFileFormat.PARQUET,
    ],
)
def test_resolve_auto_dest_format_passes_explicit_through(fmt):
    assert transporter1.resolve_auto_dest_format(fmt, ["whatever.bin"]) is fmt


@pytest.mark.parametrize(
    "path,expected",
    [
        ("out.csv", DestFileFormat.CSV),
        ("out.json", DestFileFormat.JSON),
        ("out.avro", DestFileFormat.AVRO),
        ("out.parquet", DestFileFormat.PARQUET),
        ("out.pq", DestFileFormat.PARQUET),
        ("Out.PARQUET", DestFileFormat.PARQUET),
    ],
)
def test_resolve_auto_dest_format_infers_from_single_path(path, expected):
    assert (
        transporter1.resolve_auto_dest_format(DestFileFormat.AUTO, [path]) is expected
    )


def test_resolve_auto_dest_format_raises_on_mixed_extensions():
    with pytest.raises(CommonInitException):
        transporter1.resolve_auto_dest_format(
            DestFileFormat.AUTO, ["out.csv", "out.parquet"]
        )


def test_resolve_auto_dest_format_raises_on_unknown_extension():
    with pytest.raises(CommonInitException):
        transporter1.resolve_auto_dest_format(DestFileFormat.AUTO, ["weird.bin"])


def test_dest_format_csv_carries_options():
    out = transporter1.dest_format_to_transporter(
        DestFileFormat.CSV, {"include_header": True}
    )
    assert out.options["include_header"] is True
    # ``separator`` etc. arrive via the defaults layer.
    assert "separator" in out.options


# --- _plugin.initial_lastmod_to_iso -------------------------------------


def test_initial_lastmod_to_iso_aware_utc():
    dt = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert _plugin.initial_lastmod_to_iso(dt) == "2026-01-01T12:00:00.000000+00:00"


def test_initial_lastmod_to_iso_naive_assumed_utc():
    dt = datetime(2026, 1, 1, 12, 0, 0)
    assert _plugin.initial_lastmod_to_iso(dt) == "2026-01-01T12:00:00.000000+00:00"


def test_initial_lastmod_to_iso_none():
    assert _plugin.initial_lastmod_to_iso(None) is None


# --- _plugin.resolve_dest_tokens ----------------------------------------


def test_resolve_dest_tokens_substitutes():
    ctx = _DestCtx(Path("."))
    resolved = _plugin.resolve_dest_tokens(
        "out/${EXEC_PLAN_ID}/${TRX_ID}/x.${EXEC_PLAN_TS}.csv", ctx
    )
    assert resolved == (
        "out/test-exec-plan-id/test-trx-id/x.2026-01-01T00:00:00.000000+00:00.csv"
    )


def test_resolve_dest_tokens_leaves_unknown_tokens():
    ctx = _DestCtx(Path("."))
    resolved = _plugin.resolve_dest_tokens("out/${UNKNOWN}/x.csv", ctx)
    assert resolved == "out/${UNKNOWN}/x.csv"


# --- src/transporter1.dest_format_to_transporter unhandled-format raises -----


class _BogusFormat:
    """Stand-in for a future enum member none of the helpers handle."""

    name = "BOGUS"
    value = "BOGUS"

    def __repr__(self) -> str:  # appears in the rendered error message
        return "BOGUS"


def test_src_format_to_transporter_unhandled_raises():
    with pytest.raises(CommonInitException) as ei:
        transporter1.src_format_to_transporter(_BogusFormat(), None)  # type: ignore[arg-type]
    assert ei.value.api_error is FileFormatErrorCode.FILE_FORMAT_1


def test_dest_format_to_transporter_unhandled_raises():
    with pytest.raises(CommonInitException) as ei:
        transporter1.dest_format_to_transporter(_BogusFormat(), None)  # type: ignore[arg-type]
    assert ei.value.api_error is FileFormatErrorCode.FILE_FORMAT_2


# --- _plugin.copy_out: slot/path count mismatch --------------------


class _DestCtx(DestContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "test-exec-plan-id"

    @property
    def trx_id(self) -> str:
        return "test-trx-id"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def set_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass


def _stub_build_location(_conn, _entry, _handler):  # pragma: no cover - never reached
    raise AssertionError("build_location should not be called on slot-mismatch path")


_STUB_ERRORS = _plugin.FilePluginErrors(
    binary_not_found=FileCommonErrorCode.FILE_COMMON_1,
    read_failed=FileCommonErrorCode.FILE_COMMON_1,
    write_failed=FileCommonErrorCode.FILE_COMMON_1,
    report_invalid=FileCommonErrorCode.FILE_COMMON_1,
)


# --- ReaderConf.from_src / WriterConf.from_dest -----------------------


class _StubSrc:
    """Duck-typed stand-in for any connector's Src dataclass --
    ReaderConf.from_src only reads ``.paths``, ``.format``,
    ``.format_cfg``, ``.initial_last_modified``, ``.src_cfg`` and
    ``.purge`` (``conn`` is now passed in separately by the plugin)."""

    def __init__(self):
        self.paths = ["a.csv", "b.csv"]
        self.format = SrcFileFormat.CSV
        self.format_cfg = {SrcFileFormat.CSV: {"separator": ","}}
        self.initial_last_modified = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.src_cfg = None
        self.purge = False


def test_reader_conf_from_src_translates_fields():
    rc = _plugin.ReaderConf.from_src(object(), _StubSrc())
    assert rc.paths == ["a.csv", "b.csv"]
    # The conf carries the raw format enum + cfg; the handler resolves
    # AUTO and maps it to a transporter format at request-build time.
    assert rc.file_format is SrcFileFormat.CSV
    assert rc.format_cfg == {SrcFileFormat.CSV: {"separator": ","}}
    assert rc.initial_lastmod == "2026-01-01T00:00:00.000000+00:00"
    assert rc.drop_metadata is False


def test_reader_conf_from_src_reads_the_drop_metadata_key():
    src = _StubSrc()
    src.src_cfg = {SRC_META_DROP_KEY: True}
    assert _plugin.ReaderConf.from_src(object(), src).drop_metadata is True


def test_reader_conf_from_src_handles_none_format_cfg():
    src = _StubSrc()
    src.format_cfg = None
    rc = _plugin.ReaderConf.from_src(object(), src)
    assert rc.file_format is SrcFileFormat.CSV
    # No fragment and no cfg: nothing to carry, so nothing is invented.
    assert rc.format_cfg is None
    assert rc.format_cfg_for(0) is None


class _StubDest:
    """Mirror of ``_StubSrc`` for the dest side."""

    def __init__(self):
        self.paths = ["out/x.csv"]
        self.format = DestFileFormat.CSV
        self.format_cfg = {DestFileFormat.CSV: {"include_header": True}}


def test_writer_conf_from_dest_translates_fields():
    wc = _plugin.WriterConf.from_dest(object(), _StubDest())
    assert wc.paths == ["out/x.csv"]
    assert wc.file_format is DestFileFormat.CSV
    assert wc.format_cfg == {DestFileFormat.CSV: {"include_header": True}}


def test_writer_conf_from_dest_handles_none_format_cfg():
    dest = _StubDest()
    dest.format_cfg = None
    wc = _plugin.WriterConf.from_dest(object(), dest)
    assert wc.file_format is DestFileFormat.CSV
    assert wc.format_cfg is None


def test_run_write_out_slot_count_mismatch_raises_file_common_16(tmp_path):
    cfg = _plugin.WriterConf(
        conn=None, paths=["a", "b"], file_format=DestFileFormat.PARQUET
    )
    parquet = Path(tmp_path / "x.parquet")
    with pytest.raises(ConnException) as ei:
        _plugin.copy_out(
            ctx=_DestCtx(tmp_path),
            cfg=cfg,
            tables=[parquet],
            build_location=_stub_build_location,
            errors=_STUB_ERRORS,
        )
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_16
    assert ei.value.extra == {"tables": 1, "paths": 2}


# --- _plugin.import_files / run_write_out orchestrators ----------------
#
# The orchestrators delegate to ``_input.import_one`` /
# ``_output.copy_one`` which shell out to the transporter binary. The
# tests below stub those entry points so the orchestrator's wiring,
# slot bookkeeping, and error-mapping are exercised in isolation --
# no real binary needed.

# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdatak.conn.common.file import (
    _input as _input_module,
    _output as _output_module,
)
from tabsdatak.error import ApiErrorCode
from tabsdatak.spi import SrcPluginCtx


class _SrcCtx(SrcPluginCtx):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "test-exec-plan-id"

    @property
    def trx_id(self) -> str:
        return "test-trx-id"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def set_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass


class _RecordingSrcCtx(_SrcCtx):
    """SrcPluginCtx stub that records get/store calls and serves a
    pre-populated attribute map -- so tests can assert the lastmod
    read/write contract end-to-end."""

    def __init__(self, work_dir: Path, attrs: dict | None = None) -> None:
        super().__init__(work_dir)
        self.attrs: dict = dict(attrs or {})
        self.gets: list[str] = []
        self.stores: list[tuple[str, object]] = []

    def get_attr(self, attr):
        self.gets.append(attr)
        return self.attrs.get(attr)

    def set_attr(self, attr, val):
        self.stores.append((attr, val))
        self.attrs[attr] = val


def _location_builder(_conn, entry, _handler):
    return transporter2.TransporterLocalFile(f"file:///srv/{entry}")


_RUN_ERRORS = _plugin.FilePluginErrors(
    binary_not_found=FileCommonErrorCode.FILE_COMMON_1,
    read_failed=FileCommonErrorCode.FILE_COMMON_3,
    write_failed=FileCommonErrorCode.FILE_COMMON_8,
    report_invalid=FileCommonErrorCode.FILE_COMMON_4,
    cred_resolution=FileCommonErrorCode.FILE_COMMON_13,
)


def _stub_report(
    targets: list[str],
    sources: list[str] | None = None,
) -> transporter1.TransporterReport:
    """A transporter report over ``targets``. Every real report entry
    pairs the imported file (``to``) with the location it was read from
    (``from``), so the stub does too."""
    froms = sources or [f"/srv/in{i}.csv" for i in range(len(targets))]
    files = [{"from": f, "to": t} for f, t in zip(froms, targets)]
    return TransporterReport(
        files=files,
        lastmod_info=None,
    )


# --- run_read_in ------------------------------------------------------


def test_run_read_in_per_slot_dir_and_parquet_collection(monkeypatch, tmp_path):
    calls: list[dict] = []

    def fake_import_one(**kw):
        calls.append(kw)
        # Synthesize one output file per slot
        return _stub_report([str(kw["target_dir"] / "out.parquet")])

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a/in.csv", "b/in.csv"],
        file_format=SrcFileFormat.PARQUET,
    )
    out = _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    # Two slot dirs created in the work dir
    assert (tmp_path / "slot0").is_dir()
    assert (tmp_path / "slot1").is_dir()
    # One TableFileInput per slot, each pointing at the synthesized parquet
    assert len(out) == 2
    assert len(out[0]) == 1
    assert isinstance(out[0][0].file, Path)
    assert Path(out[0][0].file).parts[-2:] == ("slot0", "out.parquet")
    assert Path(out[1][0].file).parts[-2:] == ("slot1", "out.parquet")
    # import_one called per path with that slot's target_dir
    assert [c["target_dir"].name for c in calls] == ["slot0", "slot1"]


def test_run_read_in_run_env_called_once_and_passed_through(monkeypatch, tmp_path):
    captured_env: list[dict | None] = []

    def fake_import_one(**kw):
        captured_env.append(kw.get("env"))
        return _stub_report([])

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a", "b"],
        file_format=SrcFileFormat.PARQUET,
    )
    _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
        run_env=lambda _c, _h: {"X": "1"},
    )
    # Same env dict reused across slots
    assert captured_env == [{"X": "1"}, {"X": "1"}]


def test_run_read_in_run_env_failure_wraps_as_cred_resolution(monkeypatch, tmp_path):
    def bad_run_env(_c, _h):
        raise ApiErrorCode.API_11.exception(name="X")

    monkeypatch.setattr(_input_module, "import_one", lambda **_kw: _stub_report([]))

    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a"],
        file_format=SrcFileFormat.PARQUET,
    )
    with pytest.raises(ConnException) as ei:
        _plugin.import_files(
            ctx=_SrcCtx(tmp_path),
            cfg=cfg,
            build_location=_location_builder,
            errors=_RUN_ERRORS,
            run_env=bad_run_env,
        )
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_13


def _make_exc(exc_cls):
    """``TransporterRunError`` takes ``(returncode, stdout, stderr)``;
    the other two take a single message string."""
    if exc_cls is transporter1.TransporterRunError:
        return exc_cls(1, "", "boom")
    return exc_cls("boom")


@pytest.mark.parametrize(
    ("exc_cls", "expected_code"),
    [
        (
            transporter1.TransporterBinaryNotFoundError,
            FileCommonErrorCode.FILE_COMMON_1,
        ),
        (
            transporter1.TransporterRunError,
            FileCommonErrorCode.FILE_COMMON_3,
        ),
        (
            transporter1.TransporterReportError,
            FileCommonErrorCode.FILE_COMMON_4,
        ),
    ],
)
def test_run_read_in_wraps_transporter_failures(
    monkeypatch,
    tmp_path,
    exc_cls,
    expected_code,
):
    def fake_import_one(**_kw):
        raise _make_exc(exc_cls)

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a"],
        file_format=SrcFileFormat.PARQUET,
    )
    with pytest.raises(ConnException) as ei:
        _plugin.import_files(
            ctx=_SrcCtx(tmp_path),
            cfg=cfg,
            build_location=_location_builder,
            errors=_RUN_ERRORS,
        )
    assert ei.value.api_error is expected_code


def test_run_read_in_skips_report_entries_without_to_field(monkeypatch, tmp_path):
    def fake_import_one(**_kw):
        report = _stub_report(["/x/keep.parquet"])
        # Inject a few junk entries the orchestrator must ignore
        report.files.append({"meta": "no-to-field"})
        report.files.append("not-a-dict")
        return report

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a"],
        file_format=SrcFileFormat.PARQUET,
    )
    out = _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert len(out[0]) == 1
    assert Path(out[0][0].file) == Path("/x/keep.parquet")


def test_run_read_in_reports_source_path_as_metadata(monkeypatch, tmp_path):
    def fake_import_one(**kw):
        slot = kw["target_dir"].name
        return _stub_report(
            [
                str(kw["target_dir"] / "a.parquet"),
                str(kw["target_dir"] / "b.parquet"),
            ],
            sources=[f"s3://bucket/{slot}/a.csv", f"s3://bucket/{slot}/b.csv"],
        )

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["in/*.csv", "other/*.csv"],
        file_format=SrcFileFormat.CSV,
    )
    out = _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    # Each file of a globbed slot reports the file it was read from.
    assert [[f.metadata[FILE_META_PATH] for f in slot] for slot in out] == [
        ["s3://bucket/slot0/a.csv", "s3://bucket/slot0/b.csv"],
        ["s3://bucket/slot1/a.csv", "s3://bucket/slot1/b.csv"],
    ]


def test_run_read_in_metadata_key_present_when_report_omits_source(
    monkeypatch,
    tmp_path,
):
    def fake_import_one(**kw):
        target = str(kw["target_dir"] / "out.parquet")
        files = [{"to": target}]
        return TransporterReport(
            files=files,
            lastmod_info=None,
        )

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a"],
        file_format=SrcFileFormat.PARQUET,
    )
    out = _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    # The metadata never changes shape: the key stays, empty when the
    # report carries no source for the file.
    assert out[0][0].metadata == {FILE_META_PATH: ""}


def test_run_read_in_drops_metadata_when_configured(monkeypatch, tmp_path):
    def fake_import_one(**kw):
        return _stub_report(
            [str(kw["target_dir"] / "a.parquet")],
            sources=["s3://bucket/in/a.csv"],
        )

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["in/*.csv"],
        file_format=SrcFileFormat.CSV,
        drop_metadata=True,
    )
    out = _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert out[0][0].metadata is None


# --- run_read_in: lastmod cursor via ctx attrs ------------------------


def test_run_read_in_passes_prior_lastmod_from_ctx_per_slot(monkeypatch, tmp_path):
    # The plugin reads the per-slot cursor (``file-lastmod.slot<N>``)
    # from the trx context and forwards it as ``lastmod_info`` to
    # ``import_one``. Different slots get different prior values.
    seen_lastmod: list[str | None] = []

    def fake_import_one(**kw):
        seen_lastmod.append(kw.get("lastmod_info"))
        return _stub_report([])

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    ctx = _RecordingSrcCtx(
        tmp_path,
        attrs={
            "file-lastmod.slot0": "cursor-A",
            "file-lastmod.slot1": "cursor-B",
        },
    )
    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a", "b"],
        file_format=SrcFileFormat.PARQUET,
    )
    _plugin.import_files(
        ctx=ctx,
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert seen_lastmod == ["cursor-A", "cursor-B"]
    assert ctx.gets == ["file-lastmod.slot0", "file-lastmod.slot1"]


def test_run_read_in_no_prior_lastmod_passes_none(monkeypatch, tmp_path):
    # First-run / cold-start: ctx has no cursor yet, so ``lastmod_info``
    # arrives at ``import_one`` as ``None``. The Src's
    # ``initial_lastmod`` is what the transporter uses to seed.
    seen: list[dict] = []

    def fake_import_one(**kw):
        seen.append(kw)
        return _stub_report([])

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a"],
        file_format=SrcFileFormat.PARQUET,
        initial_lastmod="2026-01-01T00:00:00.000000+00:00",
    )
    _plugin.import_files(
        ctx=_RecordingSrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert seen[0]["lastmod_info"] is None
    assert seen[0]["initial_lastmod"] == "2026-01-01T00:00:00.000000+00:00"


def test_run_read_in_stores_report_lastmod_back_to_ctx(monkeypatch, tmp_path):
    # After import, the transporter's fresh ``lastmod_info`` is staged
    # on the ctx under the per-slot key so the next trx can resume.
    def fake_import_one(**kw):
        target = str(kw["target_dir"] / "out.parquet")
        slot_name = kw["target_dir"].name  # slot0 / slot1 / ...
        files = [{"to": target}]
        return TransporterReport(
            files=files,
            lastmod_info=f"cursor-{slot_name}-fresh",
        )

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    ctx = _RecordingSrcCtx(tmp_path)
    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a", "b"],
        file_format=SrcFileFormat.PARQUET,
    )
    _plugin.import_files(
        ctx=ctx,
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert ctx.stores == [
        ("file-lastmod.slot0", "cursor-slot0-fresh"),
        ("file-lastmod.slot1", "cursor-slot1-fresh"),
    ]


def test_run_read_in_skips_store_when_report_has_no_lastmod(monkeypatch, tmp_path):
    # Transporter may return ``lastmod_info=None`` (no observable files
    # in this slot). Don't clobber the prior cursor in that case --
    # leave it in place so the next trx still resumes from where the
    # last productive trx left off.
    monkeypatch.setattr(
        _input_module,
        "import_one",
        lambda **_kw: _stub_report([]),  # lastmod_info=None by default
    )

    ctx = _RecordingSrcCtx(
        tmp_path,
        attrs={"file-lastmod.slot0": "stays-put"},
    )
    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a"],
        file_format=SrcFileFormat.PARQUET,
    )
    _plugin.import_files(
        ctx=ctx,
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert ctx.stores == []
    assert ctx.attrs["file-lastmod.slot0"] == "stays-put"


# --- run_write_out ----------------------------------------------------


def test_run_write_out_passes_source_target_pairs(monkeypatch, tmp_path):
    captured: dict = {}

    def fake_copy_one(
        *,
        handler,
        source_target_pairs,
        work_dir,
        file_format,
        paths,
        format_cfg=None,
        env=None,
    ):
        captured["pairs"] = source_target_pairs
        captured["work_dir"] = work_dir
        captured["env"] = env

    monkeypatch.setattr(_output_module, "copy_one", fake_copy_one)

    parquet_a = Path(tmp_path / "a.parquet")
    parquet_b = Path(tmp_path / "b.parquet")
    cfg = _plugin.WriterConf(
        conn=object(),
        paths=["x/y", "z"],
        file_format=DestFileFormat.PARQUET,
    )
    _plugin.copy_out(
        ctx=_DestCtx(tmp_path),
        cfg=cfg,
        tables=[parquet_a, parquet_b],
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert captured["work_dir"] == tmp_path
    assert captured["env"] is None
    assert len(captured["pairs"]) == 2
    # Each pair is (source_local_file, target_location)
    for src_loc, tgt_loc in captured["pairs"]:
        assert isinstance(src_loc, transporter2.TransporterLocalFile)
        assert isinstance(tgt_loc, transporter2.TransporterLocalFile)


def test_run_write_out_run_env_propagates_to_copy_one(monkeypatch, tmp_path):
    captured: dict = {}

    def fake_copy_one(
        *,
        handler,
        source_target_pairs,
        work_dir,
        file_format,
        paths,
        format_cfg=None,
        env=None,
    ):
        captured["env"] = env

    monkeypatch.setattr(_output_module, "copy_one", fake_copy_one)

    cfg = _plugin.WriterConf(
        conn=object(),
        paths=["x"],
        file_format=DestFileFormat.PARQUET,
    )
    _plugin.copy_out(
        ctx=_DestCtx(tmp_path),
        cfg=cfg,
        tables=[Path(tmp_path / "a.parquet")],
        build_location=_location_builder,
        errors=_RUN_ERRORS,
        run_env=lambda _c, _h: {"K": "V"},
    )
    assert captured["env"] == {"K": "V"}


def test_run_write_out_run_env_failure_wraps_as_cred_resolution(monkeypatch, tmp_path):
    def bad_run_env(_c, _h):
        raise ApiErrorCode.API_11.exception(name="X")

    monkeypatch.setattr(_output_module, "copy_one", lambda **_kw: None)

    cfg = _plugin.WriterConf(
        conn=object(),
        paths=["x"],
        file_format=DestFileFormat.PARQUET,
    )
    with pytest.raises(ConnException) as ei:
        _plugin.copy_out(
            ctx=_DestCtx(tmp_path),
            cfg=cfg,
            tables=[Path(tmp_path / "a.parquet")],
            build_location=_location_builder,
            errors=_RUN_ERRORS,
            run_env=bad_run_env,
        )
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_13


@pytest.mark.parametrize(
    ("exc_cls", "expected_code"),
    [
        (
            transporter1.TransporterBinaryNotFoundError,
            FileCommonErrorCode.FILE_COMMON_1,
        ),
        (
            transporter1.TransporterRunError,
            FileCommonErrorCode.FILE_COMMON_8,
        ),
        (
            transporter1.TransporterReportError,
            FileCommonErrorCode.FILE_COMMON_4,
        ),
    ],
)
def test_run_write_out_wraps_transporter_failures(
    monkeypatch,
    tmp_path,
    exc_cls,
    expected_code,
):
    def fake_copy_one(**_kw):
        raise _make_exc(exc_cls)

    monkeypatch.setattr(_output_module, "copy_one", fake_copy_one)

    cfg = _plugin.WriterConf(
        conn=object(),
        paths=["x"],
        file_format=DestFileFormat.PARQUET,
    )
    with pytest.raises(ConnException) as ei:
        _plugin.copy_out(
            ctx=_DestCtx(tmp_path),
            cfg=cfg,
            tables=[Path(tmp_path / "a.parquet")],
            build_location=_location_builder,
            errors=_RUN_ERRORS,
        )
    assert ei.value.api_error is expected_code


# --- worksheet fragment split ----------------------------------------


class _Src:
    """The handful of attributes ``ReaderConf.from_src`` reads."""

    def __init__(self, paths, fmt=SrcFileFormat.AUTO):
        self.paths = paths
        self.format = fmt
        self.format_cfg = None
        self.initial_last_modified = None
        self.src_cfg = None
        self.purge = False


@pytest.mark.parametrize(
    "path,expected",
    [
        ("2026/q2.xlsx", ("2026/q2.xlsx", None)),
        ("2026/q2.xlsx#1", ("2026/q2.xlsx", 1)),
        ("2026/q2.xlsx#12", ("2026/q2.xlsx", 12)),
        ("s3://b/2026/q2.xlsx#3", ("s3://b/2026/q2.xlsx", 3)),
        ("2026/*.xlsx#2", ("2026/*.xlsx", 2)),  # a glob keeps its fragment
        ("2026/q2.xlsx#02", ("2026/q2.xlsx", 2)),  # a leading zero is harmless
    ],
)
def test_split_sheet_fragment_returns_a_clean_path_and_the_sheet(path, expected):
    assert _plugin.split_sheet_fragment(path, SrcFileFormat.AUTO) == expected


def test_split_sheet_fragment_splits_on_the_first_hash():
    # Splitting on the *last* '#' would read this as path "q2.xlsx#2" plus
    # sheet 3; splitting on the first leaves the malformed fragment "2#3",
    # which is rejected. Matches what ``Url::parse`` already does to the uri
    # inside the binary, so a literal '#' in a key is no more expressible
    # than it was before.
    with pytest.raises(ConnException):
        _plugin.split_sheet_fragment("q2.xlsx#2#3", SrcFileFormat.AUTO)


@pytest.mark.parametrize("fragment", ["", "abc", "-1", "2.5", " 2", "2 ", "²", "٢"])
def test_split_sheet_fragment_rejects_a_malformed_fragment(fragment):
    with pytest.raises(ConnException) as ei:
        _plugin.split_sheet_fragment(f"q2.xlsx#{fragment}", SrcFileFormat.AUTO)
    assert "q2.xlsx" in str(ei.value)


def test_split_sheet_fragment_rejects_a_fragment_wider_than_the_wire():
    # The wire field is a u32; anything past it never had a sheet to name.
    with pytest.raises(ConnException):
        _plugin.split_sheet_fragment("q2.xlsx#4294967296", SrcFileFormat.AUTO)
    assert _plugin.split_sheet_fragment("q2.xlsx#4294967295", SrcFileFormat.AUTO) == (
        "q2.xlsx",
        4294967295,
    )


def test_split_sheet_fragment_rejects_sheet_zero_distinctly():
    with pytest.raises(ConnException) as ei:
        _plugin.split_sheet_fragment("q2.xlsx#0", SrcFileFormat.AUTO)
    assert "start at 1" in str(ei.value)


def test_split_sheet_fragment_rejects_a_fragment_on_a_declared_non_excel_format():
    with pytest.raises(ConnException) as ei:
        _plugin.split_sheet_fragment("rows.csv#2", SrcFileFormat.CSV)
    assert "CSV" in str(ei.value)


def test_split_sheet_fragment_accepts_one_under_excel_and_auto():
    for fmt in (SrcFileFormat.AUTO, SrcFileFormat.EXCEL):
        assert _plugin.split_sheet_fragment("q2.xlsx#2", fmt) == ("q2.xlsx", 2)


def test_a_path_with_no_fragment_is_untouched_by_any_format():
    for fmt in SrcFileFormat:
        assert _plugin.split_sheet_fragment("rows.csv", fmt) == ("rows.csv", None)


# --- ReaderConf.from_src ---------------------------------------------


def test_from_src_strips_fragments_and_carries_the_sheets_as_a_list():
    cfg = _plugin.ReaderConf.from_src(
        object(),
        _Src(["2026/q1.xlsx", "2026/q2.xlsx#2", "2026/q2.xlsx#3"]),
    )
    # Downstream sees clean paths...
    assert cfg.paths == ["2026/q1.xlsx", "2026/q2.xlsx", "2026/q2.xlsx"]
    # ...and one config carries every slot's worksheet, in path order.
    assert cfg.format_cfg == {SrcFileFormat.EXCEL: {EXCEL_SHEET_KEY: [None, 2, 3]}}


def test_from_src_keeps_the_users_own_format_cfg_alongside_the_sheets():
    src = _Src(["rows.csv", "2026/q2.xlsx#2"])
    src.format_cfg = {SrcFileFormat.CSV: {"separator": ";"}}
    cfg = _plugin.ReaderConf.from_src(object(), src)
    assert cfg.format_cfg == {
        SrcFileFormat.CSV: {"separator": ";"},
        SrcFileFormat.EXCEL: {EXCEL_SHEET_KEY: [None, 2]},
    }
    # The user's own dict is never mutated -- it was copied.
    assert src.format_cfg == {SrcFileFormat.CSV: {"separator": ";"}}


def test_from_src_adds_nothing_when_no_path_names_a_worksheet():
    src = _Src(["a.csv", "b.csv"])
    src.format_cfg = {SrcFileFormat.CSV: {"separator": ";"}}
    cfg = _plugin.ReaderConf.from_src(object(), src)
    assert cfg.format_cfg == {SrcFileFormat.CSV: {"separator": ";"}}


def test_from_src_reports_the_offending_path_not_the_stripped_one():
    with pytest.raises(ConnException) as ei:
        _plugin.ReaderConf.from_src(object(), _Src(["ok.xlsx", "2026/q2.xlsx#nope"]))
    assert "2026/q2.xlsx#nope" in str(ei.value)


def test_format_cfg_for_narrows_the_list_to_one_slots_sheet():
    cfg = _plugin.ReaderConf.from_src(
        object(), _Src(["2026/q1.xlsx", "2026/q2.xlsx#2", "2026/q2.xlsx#3"])
    )
    # A slot whose path named a sheet gets that one number...
    assert cfg.format_cfg_for(1) == {SrcFileFormat.EXCEL: {EXCEL_SHEET_KEY: 2}}
    assert cfg.format_cfg_for(2) == {SrcFileFormat.EXCEL: {EXCEL_SHEET_KEY: 3}}
    # ...and one whose path named none has the key dropped, not nulled: the
    # reader reads a missing key as "the first sheet".
    assert cfg.format_cfg_for(0) == {SrcFileFormat.EXCEL: {}}
    # Narrowing does not disturb the stored list.
    assert cfg.format_cfg == {SrcFileFormat.EXCEL: {EXCEL_SHEET_KEY: [None, 2, 3]}}


def test_format_cfg_for_passes_a_sheetless_cfg_straight_through():
    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a.csv", "b.csv"],
        file_format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";"}},
    )
    assert cfg.format_cfg_for(1) is cfg.format_cfg


def test_format_cfg_for_is_none_on_a_conf_built_without_one():
    cfg = _plugin.ReaderConf(
        conn=object(), paths=["a.csv", "b.csv"], file_format=SrcFileFormat.CSV
    )
    assert [cfg.format_cfg_for(i) for i in range(2)] == [None, None]


# --- import_files threading ------------------------------------------


def test_import_files_passes_each_slot_its_own_sheet(monkeypatch, tmp_path):
    calls: list[dict] = []

    def fake_import_one(**kw):
        calls.append(kw)
        return _stub_report([str(kw["target_dir"] / "out.parquet")])

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    cfg = _plugin.ReaderConf.from_src(
        object(), _Src(["2026/q2.xlsx#2", "2026/q2.xlsx#3", "2026/q1.xlsx"])
    )
    _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert [c["format_cfg"] for c in calls] == [
        {SrcFileFormat.EXCEL: {EXCEL_SHEET_KEY: 2}},
        {SrcFileFormat.EXCEL: {EXCEL_SHEET_KEY: 3}},
        {SrcFileFormat.EXCEL: {}},
    ]


def test_build_location_never_sees_a_fragment(monkeypatch, tmp_path):
    # The whole reason the split happens in ``from_src``: a surviving
    # fragment would reach the object store as part of the key.
    seen: list[str] = []

    def recording_builder(conn, entry, handler):
        seen.append(entry)
        return _location_builder(conn, entry, handler)

    monkeypatch.setattr(
        _input_module,
        "import_one",
        lambda **kw: _stub_report([str(kw["target_dir"] / "out.parquet")]),
    )
    cfg = _plugin.ReaderConf.from_src(object(), _Src(["2026/q2.xlsx#2"]))
    _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=recording_builder,
        errors=_RUN_ERRORS,
    )
    assert seen == ["2026/q2.xlsx"]


# --- the handlers ----------------------------------------------------


def test_handler2_import_request_carries_the_sheet(tmp_path):
    handler = handler2.TransporterHandler2()
    request = handler.import_request(
        source=transporter2.TransporterLocalFile("file:///in/q2.xlsx"),
        target_dir=tmp_path,
        file_format=SrcFileFormat.AUTO,
        paths=["2026/q2.xlsx"],
        format_cfg={SrcFileFormat.EXCEL: {EXCEL_SHEET_KEY: 4}},
    )
    assert request.source_format == transporter2.FormatSpec(
        format="excel", options=transporter2.ExcelOptions(sheet=4)
    )


def test_handler2_import_request_defaults_to_the_first_sheet(tmp_path):
    handler = handler2.TransporterHandler2()
    request = handler.import_request(
        source=transporter2.TransporterLocalFile("file:///in/q2.xlsx"),
        target_dir=tmp_path,
        file_format=SrcFileFormat.AUTO,
        paths=["2026/q2.xlsx"],
    )
    assert request.source_format.options == transporter2.ExcelOptions(sheet=1)


def test_the_sheet_is_taken_back_out_of_the_cfg():
    # It is a carrier, not an option: no format may see a key its own
    # options do not define.
    cfg = {EXCEL_SHEET_KEY: 4}
    assert transporter2.src_format_to_transporter(SrcFileFormat.EXCEL, cfg).sheet == 4
    assert cfg == {}


def test_a_sheet_on_a_non_excel_format_is_refused_not_dropped():
    # The case the split cannot catch: AUTO plus a fragment on something
    # that turns out not to be a workbook. Dropping it would read the file
    # and quietly ignore what the user asked for.
    with pytest.raises(CommonInitException) as ei:
        transporter2.src_format_to_transporter(
            SrcFileFormat.CSV, {EXCEL_SHEET_KEY: 2, "separator": ";"}
        )
    assert "EXCEL" in str(ei.value)


# --- worksheet provenance --------------------------------------------


def test_provenance_carries_the_worksheet_back_onto_the_uri(monkeypatch, tmp_path):
    # The transporter never saw the fragment -- it was split off before the
    # request -- so the metadata has to put it back, or two slots reading
    # different sheets of one workbook would be indistinguishable.
    def fake_import_one(**kw):
        return _stub_report(
            [str(kw["target_dir"] / "out.parquet")],
            sources=["s3://bucket/2026/q2.xlsx"],
        )

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    cfg = _plugin.ReaderConf.from_src(
        object(), _Src(["2026/q2.xlsx#2", "2026/q2.xlsx#3", "2026/q1.xlsx"])
    )
    out = _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert [slot[0].metadata[FILE_META_PATH] for slot in out] == [
        "s3://bucket/2026/q2.xlsx#2",
        "s3://bucket/2026/q2.xlsx#3",
        # No fragment asked for, none invented.
        "s3://bucket/2026/q2.xlsx",
    ]


def test_provenance_gives_every_globbed_file_its_own_uri_and_the_slots_sheet(
    monkeypatch, tmp_path
):
    # One slot, many files: they differ in uri and share the one worksheet.
    # Appending to each file's own ``from`` is what keeps that right --
    # substituting the configured path would give them all the same value.
    def fake_import_one(**kw):
        return _stub_report(
            [
                str(kw["target_dir"] / "a.parquet"),
                str(kw["target_dir"] / "b.parquet"),
            ],
            sources=[
                "s3://bucket/2026/north/sales.xlsx",
                "s3://bucket/2026/south/sales.xlsx",
            ],
        )

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    cfg = _plugin.ReaderConf.from_src(object(), _Src(["2026/*/sales.xlsx#4"]))
    out = _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert [f.metadata[FILE_META_PATH] for f in out[0]] == [
        "s3://bucket/2026/north/sales.xlsx#4",
        "s3://bucket/2026/south/sales.xlsx#4",
    ]


def test_provenance_round_trips_back_into_paths():
    # The point of keeping the fragment: the value names a readable source.
    cfg = _plugin.ReaderConf.from_src(object(), _Src(["2026/q2.xlsx#2"]))
    published = "s3://bucket/2026/q2.xlsx#2"
    again = _plugin.ReaderConf.from_src(object(), _Src([published]))
    assert again.paths == ["s3://bucket/2026/q2.xlsx"]
    assert again.format_cfg == cfg.format_cfg


def test_dropping_metadata_still_drops_it_for_a_worksheet(monkeypatch, tmp_path):
    def fake_import_one(**kw):
        return _stub_report(
            [str(kw["target_dir"] / "out.parquet")],
            sources=["s3://bucket/2026/q2.xlsx"],
        )

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    src = _Src(["2026/q2.xlsx#2"])
    src.src_cfg = {SRC_META_DROP_KEY: True}
    out = _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=_plugin.ReaderConf.from_src(object(), src),
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert out[0][0].metadata is None


def test_sheet_in_reads_a_narrowed_cfg():
    assert _plugin.sheet_in(None) is None
    assert _plugin.sheet_in({}) is None
    assert _plugin.sheet_in({SrcFileFormat.CSV: {"separator": ";"}}) is None
    assert _plugin.sheet_in({SrcFileFormat.EXCEL: {}}) is None
    assert _plugin.sheet_in({SrcFileFormat.EXCEL: {EXCEL_SHEET_KEY: 7}}) == 7


# --- EBCDIC: one copybook per path ---------------------------------


def _copybook(record: str) -> str:
    """A minimal copybook source. `copybook` carries the COBOL itself, not a
    path to it, so fixtures have to be real (if tiny) layouts."""
    return f"      01  {record}.\n          05  FIELD PIC X(4).\n"


def _ebcdic_conf(paths: list[str], **cfg) -> _plugin.ReaderConf:
    return _plugin.ReaderConf(
        conn=object(),
        paths=paths,
        file_format=SrcFileFormat.EBCDIC,
        format_cfg={SrcFileFormat.EBCDIC: cfg},
    )


def test_format_cfg_for_narrows_the_copybook_list_to_one_slot():
    copybooks = [_copybook(m) for m in ("JAN", "FEB", "MAR")]
    conf = _ebcdic_conf(
        ["jan.dat", "feb.dat", "mar.dat"], copybook=copybooks, codepage="cp500"
    )
    for index, expected in enumerate(copybooks):
        assert conf.format_cfg_for(index) == {
            SrcFileFormat.EBCDIC: {"copybook": expected, "codepage": "cp500"}
        }
    # Narrowing does not disturb the stored list.
    assert conf.format_cfg[SrcFileFormat.EBCDIC]["copybook"] == copybooks


def test_paths_sharing_one_layout_repeat_the_copybook():
    conf = _ebcdic_conf(["jan.dat", "feb.dat"], copybook=[_copybook("CUSTOMER")] * 2)
    assert conf.format_cfg_for(0) == conf.format_cfg_for(1)


def test_an_ebcdic_cfg_with_no_copybook_passes_straight_through():
    # Validation refuses this at declaration time; narrowing has nothing
    # to do rather than inventing a key.
    conf = _ebcdic_conf(["a.dat"], codepage="cp037")
    assert conf.format_cfg_for(0) is conf.format_cfg


def test_narrowing_one_format_leaves_the_others_untouched():
    conf = _plugin.ReaderConf(
        conn=object(),
        paths=["a.dat", "b.dat"],
        file_format=SrcFileFormat.EBCDIC,
        format_cfg={
            SrcFileFormat.EBCDIC: {"copybook": [_copybook("A"), _copybook("B")]},
            SrcFileFormat.CSV: {"separator": ";"},
        },
    )
    assert conf.format_cfg_for(1) == {
        SrcFileFormat.EBCDIC: {"copybook": _copybook("B")},
        SrcFileFormat.CSV: {"separator": ";"},
    }


def test_a_narrowed_copybook_reaches_the_reader_as_one_source():
    # The binary is handed one copybook for the one path it covers.
    copybooks = [_copybook("JAN"), _copybook("FEB")]
    conf = _ebcdic_conf(["jan.dat", "feb.dat"], copybook=copybooks)
    fmt = transporter2.src_format_to_transporter(
        SrcFileFormat.EBCDIC, conf.format_cfg_for(1)[SrcFileFormat.EBCDIC]
    )
    assert fmt.copybook == copybooks[1]
