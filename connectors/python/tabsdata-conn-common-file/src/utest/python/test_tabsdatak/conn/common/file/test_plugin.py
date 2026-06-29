from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.common.file import _plugin, _transporter
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    FileInfo,
    SrcFileFormat,
)
from tabsdatak.spi import DestContext

# --- _transporter.src_format_to_transporter ----------------------------------


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
    out = _transporter.src_format_to_transporter(fmt, None)
    assert type(out).__name__ == expected_cls


def test_src_format_to_transporter_rejects_auto():
    """AUTO must be resolved via ``resolve_auto_src_format`` before
    reaching the transporter mapping; reaching this function with AUTO
    is a programming error."""
    with pytest.raises(ConnInitException):
        _transporter.src_format_to_transporter(SrcFileFormat.AUTO, None)


def test_src_format_csv_carries_options():
    out = _transporter.src_format_to_transporter(SrcFileFormat.CSV, {"separator": 59})
    # User option layered on top of the binary-required defaults.
    assert out.options["separator"] == 59
    assert "has_header" in out.options


# --- _transporter.resolve_auto_src_format ------------------------------------


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
    assert _transporter.resolve_auto_src_format(fmt, ["whatever.bin"]) is fmt


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
    assert _transporter.resolve_auto_src_format(SrcFileFormat.AUTO, [path]) is expected


def test_resolve_auto_src_format_accepts_multiple_paths_with_same_ext():
    """Multiple paths whose extensions agree resolve to that one format."""
    out = _transporter.resolve_auto_src_format(
        SrcFileFormat.AUTO, ["a.csv", "b.csv", "nested/c.csv"]
    )
    assert out is SrcFileFormat.CSV


def test_resolve_auto_src_format_raises_on_mixed_extensions():
    with pytest.raises(ConnInitException) as ei:
        _transporter.resolve_auto_src_format(SrcFileFormat.AUTO, ["a.csv", "b.parquet"])
    msg = str(ei.value)
    assert "mixed" in msg.lower()


def test_resolve_auto_src_format_raises_on_unknown_extension():
    with pytest.raises(ConnInitException) as ei:
        _transporter.resolve_auto_src_format(SrcFileFormat.AUTO, ["data.bin"])
    assert "no recognised extension" in str(ei.value)


def test_resolve_auto_src_format_raises_on_extensionless_path():
    """A path with no extension can't be inferred."""
    with pytest.raises(ConnInitException):
        _transporter.resolve_auto_src_format(SrcFileFormat.AUTO, ["data/"])


# --- _transporter.dest_format_to_transporter ---------------------------------


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
    out = _transporter.dest_format_to_transporter(fmt, None)
    assert type(out).__name__ == expected_cls


def test_dest_format_to_transporter_rejects_auto():
    with pytest.raises(ConnInitException):
        _transporter.dest_format_to_transporter(DestFileFormat.AUTO, None)


# --- _transporter.resolve_auto_dest_format -----------------------------------


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
    assert _transporter.resolve_auto_dest_format(fmt, ["whatever.bin"]) is fmt


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
        _transporter.resolve_auto_dest_format(DestFileFormat.AUTO, [path]) is expected
    )


def test_resolve_auto_dest_format_raises_on_mixed_extensions():
    with pytest.raises(ConnInitException):
        _transporter.resolve_auto_dest_format(
            DestFileFormat.AUTO, ["out.csv", "out.parquet"]
        )


def test_resolve_auto_dest_format_raises_on_unknown_extension():
    with pytest.raises(ConnInitException):
        _transporter.resolve_auto_dest_format(DestFileFormat.AUTO, ["weird.bin"])


def test_dest_format_csv_carries_options():
    out = _transporter.dest_format_to_transporter(
        DestFileFormat.CSV, {"include_header": True}
    )
    assert out.options["include_header"] is True
    # ``separator`` etc. arrive via the defaults layer.
    assert "separator" in out.options


# --- _plugin.file_info_to_str -------------------------------------------


def test_file_info_to_str():
    assert _plugin.file_info_to_str(None) is None
    assert _plugin.file_info_to_str(FileInfo.FILENAME) == "FILENAME"
    assert _plugin.file_info_to_str(FileInfo.FULLPATH) == "FULLPATH"


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


# --- src/_transporter.dest_format_to_transporter unhandled-format raises -----


class _BogusFormat:
    """Stand-in for a future enum member none of the helpers handle."""

    name = "BOGUS"
    value = "BOGUS"

    def __repr__(self) -> str:  # appears in the rendered error message
        return "BOGUS"


def test_src_format_to_transporter_unhandled_raises():
    with pytest.raises(ConnInitException) as ei:
        _transporter.src_format_to_transporter(_BogusFormat(), None)  # type: ignore[arg-type]
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_14


def test_dest_format_to_transporter_unhandled_raises():
    with pytest.raises(ConnInitException) as ei:
        _transporter.dest_format_to_transporter(_BogusFormat(), None)  # type: ignore[arg-type]
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_15


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

    def store_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass


def _stub_build_location(_conn, _entry):  # pragma: no cover - never reached
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
    ``.format_cfg``, ``.initial_last_modified`` and ``.file_info``
    (``conn`` is now passed in separately by the plugin)."""

    def __init__(self):
        self.paths = ["a.csv", "b.csv"]
        self.format = SrcFileFormat.CSV
        self.format_cfg = {SrcFileFormat.CSV: {"separator": ","}}
        self.initial_last_modified = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.file_info = FileInfo.FILENAME


def test_reader_conf_from_src_translates_fields():
    rc = _plugin.ReaderConf.from_src(object(), _StubSrc())
    assert rc.paths == ["a.csv", "b.csv"]
    assert isinstance(rc.format, _transporter.TransporterCSVFormat)
    assert rc.format.options["separator"] == ","
    assert "has_header" in rc.format.options
    assert rc.initial_lastmod == "2026-01-01T00:00:00.000000+00:00"
    assert rc.file_info == "FILENAME"


def test_reader_conf_from_src_handles_none_format_cfg():
    src = _StubSrc()
    src.format_cfg = None
    rc = _plugin.ReaderConf.from_src(object(), src)
    assert isinstance(rc.format, _transporter.TransporterCSVFormat)
    # No user options -> binary-required defaults only.
    assert "has_header" in rc.format.options
    assert "separator" in rc.format.options


class _StubDest:
    """Mirror of ``_StubSrc`` for the dest side."""

    def __init__(self):
        self.paths = ["out/x.csv"]
        self.format = DestFileFormat.CSV
        self.format_cfg = {DestFileFormat.CSV: {"include_header": True}}


def test_writer_conf_from_dest_translates_fields():
    wc = _plugin.WriterConf.from_dest(object(), _StubDest())
    assert wc.paths == ["out/x.csv"]
    assert isinstance(wc.format, _transporter.TransporterCSVFormat)
    assert wc.format.options["include_header"] is True
    assert "separator" in wc.format.options


def test_writer_conf_from_dest_handles_none_format_cfg():
    dest = _StubDest()
    dest.format_cfg = None
    wc = _plugin.WriterConf.from_dest(object(), dest)
    assert isinstance(wc.format, _transporter.TransporterCSVFormat)
    assert "include_header" in wc.format.options
    assert "separator" in wc.format.options


def test_run_write_out_slot_count_mismatch_raises_file_common_16(tmp_path):
    cfg = _plugin.WriterConf(
        conn=None, paths=["a", "b"], format=_transporter.TransporterParquetFormat()
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

    def store_attr(self, attr, val):
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

    def store_attr(self, attr, val):
        self.stores.append((attr, val))
        self.attrs[attr] = val


def _location_builder(_conn, entry):
    return _transporter.TransporterLocalFile(f"file:///srv/{entry}")


_RUN_ERRORS = _plugin.FilePluginErrors(
    binary_not_found=FileCommonErrorCode.FILE_COMMON_1,
    read_failed=FileCommonErrorCode.FILE_COMMON_3,
    write_failed=FileCommonErrorCode.FILE_COMMON_8,
    report_invalid=FileCommonErrorCode.FILE_COMMON_4,
    cred_resolution=FileCommonErrorCode.FILE_COMMON_13,
)


def _stub_report(targets: list[str]) -> _transporter.TransporterReport:
    files = [{"to": t} for t in targets]
    return _transporter.TransporterReport(
        files=files,
        lastmod_info=None,
        raw={"files": files, "lastmod_info": None},
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
        format=_transporter.TransporterParquetFormat(),
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
    # One TableFileSpec per slot, each pointing at the synthesized parquet
    assert len(out) == 2
    assert len(out[0]) == 1
    assert isinstance(out[0][0], Path)
    assert str(out[0][0]).endswith("slot0/out.parquet")
    assert str(out[1][0]).endswith("slot1/out.parquet")
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
        format=_transporter.TransporterParquetFormat(),
    )
    _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
        run_env=lambda _c: {"X": "1"},
    )
    # Same env dict reused across slots
    assert captured_env == [{"X": "1"}, {"X": "1"}]


def test_run_read_in_run_env_failure_wraps_as_cred_resolution(monkeypatch, tmp_path):
    def bad_run_env(_c):
        raise ApiErrorCode.API_6.exception(name="X")

    monkeypatch.setattr(_input_module, "import_one", lambda **_kw: _stub_report([]))

    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a"],
        format=_transporter.TransporterParquetFormat(),
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
    if exc_cls is _transporter.TransporterRunError:
        return exc_cls(1, "", "boom")
    return exc_cls("boom")


@pytest.mark.parametrize(
    ("exc_cls", "expected_code"),
    [
        (
            _transporter.TransporterBinaryNotFoundError,
            FileCommonErrorCode.FILE_COMMON_1,
        ),
        (
            _transporter.TransporterRunError,
            FileCommonErrorCode.FILE_COMMON_3,
        ),
        (
            _transporter.TransporterReportError,
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
        format=_transporter.TransporterParquetFormat(),
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
        format=_transporter.TransporterParquetFormat(),
    )
    out = _plugin.import_files(
        ctx=_SrcCtx(tmp_path),
        cfg=cfg,
        build_location=_location_builder,
        errors=_RUN_ERRORS,
    )
    assert len(out[0]) == 1
    assert str(out[0][0]) == "/x/keep.parquet"


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
        format=_transporter.TransporterParquetFormat(),
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
        format=_transporter.TransporterParquetFormat(),
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
        return _transporter.TransporterReport(
            files=files,
            lastmod_info=f"cursor-{slot_name}-fresh",
            raw={},
        )

    monkeypatch.setattr(_input_module, "import_one", fake_import_one)

    ctx = _RecordingSrcCtx(tmp_path)
    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=["a", "b"],
        format=_transporter.TransporterParquetFormat(),
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
        format=_transporter.TransporterParquetFormat(),
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

    def fake_copy_one(*, source_target_pairs, work_dir, format, env=None):
        captured["pairs"] = source_target_pairs
        captured["work_dir"] = work_dir
        captured["env"] = env

    monkeypatch.setattr(_output_module, "copy_one", fake_copy_one)

    parquet_a = Path(tmp_path / "a.parquet")
    parquet_b = Path(tmp_path / "b.parquet")
    cfg = _plugin.WriterConf(
        conn=object(),
        paths=["x/y", "z"],
        format=_transporter.TransporterParquetFormat(),
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
        assert isinstance(src_loc, _transporter.TransporterLocalFile)
        assert isinstance(tgt_loc, _transporter.TransporterLocalFile)


def test_run_write_out_run_env_propagates_to_copy_one(monkeypatch, tmp_path):
    captured: dict = {}

    def fake_copy_one(*, source_target_pairs, work_dir, format, env=None):
        captured["env"] = env

    monkeypatch.setattr(_output_module, "copy_one", fake_copy_one)

    cfg = _plugin.WriterConf(
        conn=object(),
        paths=["x"],
        format=_transporter.TransporterParquetFormat(),
    )
    _plugin.copy_out(
        ctx=_DestCtx(tmp_path),
        cfg=cfg,
        tables=[Path(tmp_path / "a.parquet")],
        build_location=_location_builder,
        errors=_RUN_ERRORS,
        run_env=lambda _c: {"K": "V"},
    )
    assert captured["env"] == {"K": "V"}


def test_run_write_out_run_env_failure_wraps_as_cred_resolution(monkeypatch, tmp_path):
    def bad_run_env(_c):
        raise ApiErrorCode.API_6.exception(name="X")

    monkeypatch.setattr(_output_module, "copy_one", lambda **_kw: None)

    cfg = _plugin.WriterConf(
        conn=object(),
        paths=["x"],
        format=_transporter.TransporterParquetFormat(),
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
            _transporter.TransporterBinaryNotFoundError,
            FileCommonErrorCode.FILE_COMMON_1,
        ),
        (
            _transporter.TransporterRunError,
            FileCommonErrorCode.FILE_COMMON_8,
        ),
        (
            _transporter.TransporterReportError,
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
        format=_transporter.TransporterParquetFormat(),
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
