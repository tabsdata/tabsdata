#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.server._tdrunner.function._commons.document.request import (
    Info,
    InputTable,
    Location,
    OutputTable,
    Request,
)
from tabsdatak.server._tdrunner.function._handlers import state
from tabsdatak.server._tdrunner.function._handlers.state import (
    STATE_FILE,
    USER_NAMESPACE_COLUMN,
    StateError,
    State,
    write_state,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin import (
    _SrcContext,
)
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.error import (
    BadStateInError,
    BadStateOutError,
    RunSourcePluginErrorCode,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def read_state(path: Path) -> dict:
    return state.read_state(TableFile.local(path))


# --- round-trip happy paths ---


def test_roundtrip_single_str(tmp_path):
    p = tmp_path / "iv.parquet"
    write_state({"last_modified": "2026-05-28T00:00:00Z"}, p)
    assert read_state(p) == {"last_modified": "2026-05-28T00:00:00Z"}


def test_roundtrip_mixed_primitive_types(tmp_path):
    p = tmp_path / "iv.parquet"
    values = {
        "a": "x",
        "b": 42,
        "c": 3.14,
        "d_true": True,
        "d_false": False,
    }
    write_state(values, p)
    assert read_state(p) == values


def test_roundtrip_heterogeneous_dict(tmp_path):
    """The actual SQL-incremental-ingest use case: a bind dict whose
    values are a mix of primitives. Previously broken under the
    ``dict[str, str]`` codec; now round-trips end-to-end."""
    p = tmp_path / "iv.parquet"
    values = {"last_modified": "2026-01-15T00:00:00Z", "min_id": 1234}
    write_state(values, p)
    assert read_state(p) == values


def test_write_list_value_rejected(tmp_path):
    """A ``list`` value is not a ``BasicTypeSpec`` (scalar only) -- a
    connector that needs a sequence serialises it to a ``str`` itself."""
    p = tmp_path / "iv.parquet"
    with pytest.raises(StateError, match="BasicTypeSpec"):
        write_state({"tags": ["alpha", "beta"]}, p)
    assert not p.exists()


def test_write_dict_value_rejected(tmp_path):
    """A ``dict`` value is not a ``BasicTypeSpec`` (scalar only)."""
    p = tmp_path / "iv.parquet"
    with pytest.raises(StateError, match="BasicTypeSpec"):
        write_state({"labels": {"env": "prod"}}, p)
    assert not p.exists()


def test_roundtrip_empty_dict(tmp_path):
    p = tmp_path / "iv.parquet"
    write_state({}, p)
    assert read_state(p) == {}


def test_roundtrip_underscored_keys_allowed(tmp_path):
    """Plain ``str`` keys -- underscores, dashes, dots, mixed, all
    legal. The old DNS-1123 regex is gone."""
    p = tmp_path / "iv.parquet"
    values = {
        "last_modified": "2026-01-01T00:00:00Z",
        "min-id": 1,
        "snake_case_key": "v",
        "kebab-case-key": "v",
        "dotted.namespace": "v",
    }
    write_state(values, p)
    assert read_state(p) == values


# --- read: error paths ---


def test_read_missing_file_raises_invalid(tmp_path):
    with pytest.raises(StateError, match="Cannot read"):
        read_state(tmp_path / "no-such.parquet")


def test_read_unreadable_file_raises_invalid(tmp_path):
    bogus = tmp_path / "iv.parquet"
    bogus.write_bytes(b"this is not parquet")
    with pytest.raises(StateError, match="Cannot read"):
        read_state(bogus)


def test_read_multi_column_parquet_raises(tmp_path):
    """The codec expects exactly one ``user_namespace`` column.
    Anything else (e.g. legacy per-key columns) is rejected."""
    p = tmp_path / "iv.parquet"
    pl.DataFrame({"a": ["v"], "b": ["w"]}).write_parquet(p)
    with pytest.raises(StateError, match="user_namespace"):
        read_state(p)


def test_read_wrong_column_name_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    pl.DataFrame({"some_other_column": [{"k": "v"}]}).write_parquet(p)
    with pytest.raises(StateError, match="user_namespace"):
        read_state(p)


def test_read_multi_row_parquet_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    pl.DataFrame({USER_NAMESPACE_COLUMN: [{"k": "v1"}, {"k": "v2"}]}).write_parquet(p)
    with pytest.raises(StateError, match="2 row"):
        read_state(p)


def test_read_zero_row_parquet_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    pl.DataFrame({
        USER_NAMESPACE_COLUMN: pl.Series([], dtype=pl.Struct({"k": pl.Utf8}))
    }).write_parquet(p)
    with pytest.raises(StateError, match="0 row"):
        read_state(p)


# --- write: validation ---


def test_write_non_str_key_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    with pytest.raises(StateError, match="ContextKeySpec"):
        write_state({42: "v"}, p)
    assert not p.exists()


def test_write_unsupported_value_type_raises(tmp_path):
    p = tmp_path / "iv.parquet"
    with pytest.raises(StateError, match="BasicTypeSpec"):
        write_state({"k": object()}, p)
    assert not p.exists()


def test_write_validates_before_io(tmp_path):
    """If any key/value is invalid, no file is written -- the previous
    file (or absence) is preserved."""
    p = tmp_path / "iv.parquet"
    write_state({"good": "v1"}, p)
    snapshot = p.read_bytes()
    with pytest.raises(StateError):
        write_state({"good": "v2", "bad": object()}, p)
    assert p.read_bytes() == snapshot


def test_write_unwritable_path_raises_oserror(tmp_path):
    a_dir = tmp_path / "iam-a-dir"
    a_dir.mkdir()
    with pytest.raises(OSError):
        write_state({"k": "v"}, a_dir)


# --- type discipline: bool vs int ---


def test_bool_round_trips_as_bool_not_int(tmp_path):
    p = tmp_path / "iv.parquet"
    write_state({"flag": True}, p)
    out = read_state(p)
    assert out == {"flag": True}
    assert isinstance(out["flag"], bool)


def test_int_does_not_become_bool(tmp_path):
    p = tmp_path / "iv.parquet"
    write_state({"count": 1}, p)
    out = read_state(p)
    assert out["count"] == 1
    assert not isinstance(out["count"], bool)


# --- crash-safe write ---------------------------------------------------


def test_write_does_not_leave_tmp_file_behind(tmp_path):
    """Successful writes ``os.replace`` the tmp file; no stray
    ``<path>.tmp`` should remain afterwards."""
    p = tmp_path / "iv.parquet"
    write_state({"k": "v"}, p)
    assert p.is_file()
    assert not (tmp_path / "iv.parquet.tmp").exists()


def test_write_crash_mid_write_leaves_previous_content_intact(tmp_path, monkeypatch):
    """If the underlying ``write_parquet`` call fails mid-flight,
    ``path`` keeps its previous content -- the tmp file never gets
    renamed over the destination."""
    p = tmp_path / "iv.parquet"
    write_state({"k": "before"}, p)
    snapshot = p.read_bytes()

    real_write_parquet = pl.DataFrame.write_parquet

    def boom(self, target, *a, **kw):
        Path(target).write_bytes(b"corrupt")
        raise RuntimeError("simulated mid-write crash")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", boom)
    with pytest.raises(RuntimeError, match="simulated mid-write crash"):
        write_state({"k": "after"}, p)

    assert p.read_bytes() == snapshot
    assert not (tmp_path / "iv.parquet.tmp").exists()

    pl.DataFrame.write_parquet = real_write_parquet


def test_write_crash_after_partial_tmp_cleans_up_orphan(tmp_path, monkeypatch):
    p = tmp_path / "iv.parquet"

    def boom(self, target, *a, **kw):
        Path(target).write_bytes(b"corrupt")
        raise OSError("simulated I/O failure")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", boom)
    with pytest.raises(OSError, match="simulated I/O failure"):
        write_state({"k": "v"}, p)

    assert not p.exists()
    assert not (tmp_path / "iv.parquet.tmp").exists()


# --- drift-prevention regression: pydantic-backed validators ------------


def test_value_validator_uses_pydantic_against_basic_type_spec():
    """If anyone replaces the pydantic-backed validator with a
    hand-rolled isinstance ladder, that ladder will diverge from
    ``BasicTypeSpec`` over time. This test pins the
    pydantic-based approach in place by verifying the validator
    chains a ``pydantic.ValidationError``."""
    from pydantic import ValidationError

    with pytest.raises(StateError) as ei:
        state.validate_value("k", object())
    assert isinstance(ei.value.__cause__, ValidationError)


def test_key_validator_uses_pydantic_against_context_key_spec():
    from pydantic import ValidationError

    with pytest.raises(StateError) as ei:
        state.validate_key(123)  # non-str key
    assert isinstance(ei.value.__cause__, ValidationError)


# --- State: hydrate, drain and output of one run --------------------------

STATE_TABLE = "td.fn_state__F-1"
PREVIOUS = {"last-modified": "2026-05-27T00:00:00Z"}
CURRENT = {"last-modified": "2026-05-28T00:00:00Z"}


def _context() -> _SrcContext:
    return _SrcContext(
        work_folder=Path("/work"),
        project_id="p-1",
        project="p",
        collection_id="cid",
        collection="c",
        function_id="fn",
        function="publish_people",
        function_version_id="fv-1",
        execution_id="ep",
        execution=None,
        transaction_id="tx",
        logical_function_run_id="lfr-1",
        physical_function_run_id="pfr-1",
        triggered_on=datetime.fromisoformat("2026-06-04T07:14:28.376000+00:00"),
    )


def _info() -> Info:
    return Info(
        project_id="P-1",
        project="p",
        collection_id=1,
        collection="c",
        function_id="F-1",
        function="fn",
        function_version_id="FV-1",
        function_defined_on=1780000000000,
        function_bundle=Location(uri="file:///bundle.tar.zst"),
        function_stage=Location(uri="file:///stage"),
        execution_id=2,
        execution=None,
        transaction_id=3,
        logical_function_run_id=4,
        physical_function_run_id=4,
        triggered_on=1780000000100,
        function_data=Location(uri="file:///data"),
        function_landing=Location(uri="file:///data/r"),
        function_scratch=Location(uri="file:///data/s"),
        scheduled_on=1780000000200,
    )


def _input(location: Location | None) -> InputTable:
    return InputTable(
        collection_id=1,
        collection="c",
        table_id="T-state",
        table=STATE_TABLE,
        table_version_id="TV-state",
        location=location,
        input_idx=0,
        table_pos=-1,
        version_pos=-1,
    )


def _output(location: Location | None) -> OutputTable:
    return OutputTable.model_construct(
        collection_id=1,
        collection="c",
        table_id="T-state",
        table=STATE_TABLE,
        table_version_id="TV-state",
        logical_table_data_version_id="LDV-state",
        physical_table_data_version_id="PDV-state",
        location=location,
        table_pos=-1,
    )


def _request(*, system_input=(), system_output=()) -> Request:
    return Request(info=_info(), system_input=system_input, system_output=system_output)


def _stored(tmp_path: Path, name: str, values: dict | None = None) -> Location:
    path = tmp_path / "storage" / name
    if values is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        write_state(values, path)
    return Location(uri=path.as_uri(), env_prefix="ROOT")


def _hydrated(request: Request, folder: Path, **flags) -> dict:
    context = _context()
    State(request, folder).hydrate(
        context, error_code=RunSourcePluginErrorCode.RSP_9, **flags
    )
    return context.drain()


def test_the_run_state_file_has_a_fixed_name(tmp_path):
    assert State(_request(), tmp_path).file == tmp_path / STATE_FILE


def test_hydrate_without_system_input_starts_empty(tmp_path):
    assert _hydrated(_request(), tmp_path) == {}


def test_hydrate_without_a_previous_state_starts_empty(tmp_path):
    assert _hydrated(_request(system_input=(_input(None),)), tmp_path) == {}


def test_hydrate_reads_the_previous_state_where_it_is_stored(tmp_path):
    request = _request(system_input=(_input(_stored(tmp_path, "s.t", PREVIOUS)),))
    assert _hydrated(request, tmp_path / "state") == PREVIOUS


def test_hydrate_ignores_this_run_state_unless_preferred(tmp_path):
    request = _request(system_input=(_input(_stored(tmp_path, "s.t", PREVIOUS)),))
    write_state(CURRENT, tmp_path / STATE_FILE)
    assert _hydrated(request, tmp_path) == PREVIOUS


def test_hydrate_prefers_this_run_state(tmp_path):
    request = _request(system_input=(_input(_stored(tmp_path, "s.t", PREVIOUS)),))
    write_state(CURRENT, tmp_path / STATE_FILE)
    assert _hydrated(request, tmp_path, prefer_local=True) == CURRENT


def test_hydrate_falls_back_to_the_previous_state(tmp_path):
    request = _request(system_input=(_input(_stored(tmp_path, "s.t", PREVIOUS)),))
    assert _hydrated(request, tmp_path / "state", prefer_local=True) == PREVIOUS


def test_hydrate_a_missing_previous_state_raises(tmp_path):
    request = _request(system_input=(_input(_stored(tmp_path, "missing.t")),))
    with pytest.raises(BadStateInError, match="missing.t"):
        _hydrated(request, tmp_path / "state")


def test_hydrate_a_malformed_previous_state_raises(tmp_path):
    location = _stored(tmp_path, "s.t")
    Path(tmp_path / "storage").mkdir()
    (tmp_path / "storage" / "s.t").write_bytes(b"not parquet")
    with pytest.raises(BadStateInError):
        _hydrated(_request(system_input=(_input(location),)), tmp_path / "state")


def _drain(context: _SrcContext, request: Request, folder: Path) -> State:
    run_state = State(request, folder)
    run_state.drain(context, error_code=RunSourcePluginErrorCode.RSP_10)
    return run_state


def test_drain_without_system_output_writes_nothing(tmp_path):
    context = _context()
    context.set_attr("foo", "bar")
    _drain(context, _request(), tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_drain_without_an_output_location_writes_nothing(tmp_path):
    context = _context()
    context.set_attr("foo", "bar")
    _drain(context, _request(system_output=(_output(None),)), tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_drain_writes_the_run_state_file(tmp_path):
    context = _context()
    context.set_attr("last-modified", CURRENT["last-modified"])
    folder = tmp_path / "fresh"
    request = _request(system_output=(_output(_stored(tmp_path, "s.t")),))
    run_state = _drain(context, request, folder)
    assert read_state(folder / STATE_FILE) == CURRENT
    assert not (tmp_path / "storage" / "s.t").exists()
    assert run_state.file == folder / STATE_FILE


def test_drain_an_empty_ctx_writes_an_empty_state(tmp_path):
    request = _request(system_output=(_output(_stored(tmp_path, "s.t")),))
    _drain(_context(), request, tmp_path)
    assert read_state(tmp_path / STATE_FILE) == {}


def test_drain_a_bad_value_raises(tmp_path):
    context = _context()
    context._scratch["k"] = object()  # type: ignore[assignment]
    request = _request(system_output=(_output(_stored(tmp_path, "s.t")),))
    with pytest.raises(BadStateOutError):
        _drain(context, request, tmp_path)


def test_output_copies_this_run_state_to_the_output_location(tmp_path):
    location = _stored(tmp_path, "s.t")
    write_state(CURRENT, tmp_path / STATE_FILE)
    output = State(_request(system_output=(_output(location),)), tmp_path).output()
    assert output.name == STATE_TABLE
    assert output.source.uri == (tmp_path / STATE_FILE).as_uri()
    assert output.target.uri == location.uri
    pl.collect_all([output.source.copy(output.target)], engine="streaming")
    assert read_state(tmp_path / "storage" / "s.t") == CURRENT


def test_no_output_without_a_run_state_file(tmp_path):
    request = _request(system_output=(_output(_stored(tmp_path, "s.t")),))
    assert State(request, tmp_path).output() is None


def test_no_output_without_an_output_location(tmp_path):
    write_state(CURRENT, tmp_path / STATE_FILE)
    assert State(_request(system_output=(_output(None),)), tmp_path).output() is None
    assert State(_request(), tmp_path).output() is None
