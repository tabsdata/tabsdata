#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for `_lib/stage_layout.py`.

Pure-utility coverage: path helpers, envelope round-trip, parse error
paths. No I/O.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._lib.stage_layout import (
    FILES_SUBDIR,
    MANIFESTS_SUBDIR,
    STREAM_STATE_API_VERSION,
    STREAM_STATE_FILENAME,
    STREAM_STATE_KIND,
    InvalidStateEnvelopeError,
    StreamStateView,
    files_dir,
    format_state_envelope,
    manifests_dir,
    parse_state_envelope,
    stream_state_path,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- Constants pin --------------------------------------------------------
#
# Pin the on-wire names. Any change here is a wire-format change that the
# publisher pod (ManifestStageSrcPlugin) and the manifest-trigger sidecar
# would need to migrate to in lockstep — so we want a test to fail loudly
# if someone tweaks them without thinking.


def test_directory_subdir_names_are_pinned():
    assert MANIFESTS_SUBDIR == "manifests"
    assert FILES_SUBDIR == "files"
    assert STREAM_STATE_FILENAME == "stream-state.json"


def test_state_envelope_kind_and_apiversion_are_pinned():
    assert STREAM_STATE_KIND == "streamState"
    assert STREAM_STATE_API_VERSION == 1


# --- Path helpers ---------------------------------------------------------


def test_manifests_dir_joins_under_stage_root():
    assert manifests_dir(Path("/stage")) == Path("/stage/manifests")


def test_files_dir_joins_under_stage_root():
    assert files_dir(Path("/stage")) == Path("/stage/files")


def test_stream_state_path_joins_under_stage_root():
    assert stream_state_path(Path("/stage")) == Path("/stage/stream-state.json")


def test_path_helpers_work_with_relative_roots():
    """The helpers are pure path joins — no filesystem assumptions, so
    relative roots are valid too."""
    root = Path("relative/stage")
    assert manifests_dir(root) == Path("relative/stage/manifests")
    assert files_dir(root) == Path("relative/stage/files")
    assert stream_state_path(root) == Path("relative/stage/stream-state.json")


# --- format_state_envelope ------------------------------------------------


def test_format_state_envelope_has_exact_schema_shape():
    """Locks the on-wire field order / names — consumers (the trigger
    sidecar, future migration tools) depend on this exact shape."""
    env = format_state_envelope(
        {"offset": 42},
        external_id="2026-01-15T12:00:00.000",
        tabsdata_version="2.0.0",
    )
    assert env == {
        "kind": "streamState",
        "apiVersion": 1,
        "tabsdataVersion": "2.0.0",
        "externalId": "2026-01-15T12:00:00.000",
        "state": {"offset": 42},
    }


def test_format_state_envelope_accepts_empty_state_marker():
    """The first-invocation marker case (TED: when the user callback is
    not a StoreStreamState, the runtime persists an empty-dict state to
    signal 'we ran once' without claiming an offset)."""
    env = format_state_envelope(
        {},
        external_id="2026-01-15T12:00:00.000",
        tabsdata_version="2.0.0",
    )
    assert env["state"] == {}


# --- parse_state_envelope: happy path -------------------------------------


def test_parse_state_envelope_returns_full_view():
    raw = {
        "kind": "streamState",
        "apiVersion": 1,
        "tabsdataVersion": "2.0.0",
        "externalId": "2026-01-15T12:00:00.000",
        "state": {"offset": 42},
    }
    view = parse_state_envelope(raw)
    assert view == StreamStateView(
        state={"offset": 42},
        external_id="2026-01-15T12:00:00.000",
        tabsdata_version="2.0.0",
        kind="streamState",
        api_version=1,
    )


def test_parse_state_envelope_round_trips_with_format():
    """format → parse yields the same logical state. Guards against the
    two functions drifting apart."""
    original = {"offset": 42, "topic": "people"}
    raw = format_state_envelope(
        original, external_id="2026-01-15T12:00:00.000", tabsdata_version="2.0.0"
    )
    view = parse_state_envelope(raw)
    assert view.state == original
    assert view.external_id == "2026-01-15T12:00:00.000"
    assert view.tabsdata_version == "2.0.0"


def test_parse_state_envelope_accepts_empty_state_dict():
    """First-invocation marker (state={}) is structurally valid — must
    not be confused with a missing-state error."""
    raw = {
        "kind": "streamState",
        "apiVersion": 1,
        "tabsdataVersion": "2.0.0",
        "externalId": "2026-01-15T12:00:00.000",
        "state": {},
    }
    view = parse_state_envelope(raw)
    assert view.state == {}


def test_parse_state_envelope_tolerates_missing_external_id():
    """Legacy files may predate the externalId field — present as None."""
    raw = {
        "kind": "streamState",
        "apiVersion": 1,
        "state": {"offset": 1},
    }
    view = parse_state_envelope(raw)
    assert view.external_id is None
    assert view.tabsdata_version is None
    assert view.state == {"offset": 1}


# --- parse_state_envelope: error paths ------------------------------------


def test_parse_state_envelope_rejects_non_dict_top_level():
    with pytest.raises(
        InvalidStateEnvelopeError, match=r"valid dictionary or instance"
    ):
        parse_state_envelope([1, 2, 3])  # type: ignore[arg-type]


def test_parse_state_envelope_rejects_wrong_kind():
    raw = {"kind": "somethingElse", "apiVersion": 1, "state": {}}
    with pytest.raises(
        InvalidStateEnvelopeError, match=r"Input should be 'streamState'"
    ):
        parse_state_envelope(raw)


def test_parse_state_envelope_rejects_missing_kind():
    raw = {"apiVersion": 1, "state": {}}
    with pytest.raises(InvalidStateEnvelopeError, match=r"kind\n\s*Field required"):
        parse_state_envelope(raw)


def test_parse_state_envelope_rejects_unsupported_api_version():
    """If we ever bump the schema, parse must refuse the old apiVersion
    rather than silently mis-parsing a future shape."""
    raw = {"kind": "streamState", "apiVersion": 2, "state": {}}
    with pytest.raises(InvalidStateEnvelopeError, match=r"Input should be 1"):
        parse_state_envelope(raw)


def test_parse_state_envelope_rejects_missing_state_field():
    raw = {"kind": "streamState", "apiVersion": 1}
    with pytest.raises(InvalidStateEnvelopeError, match=r"state\n\s*Field required"):
        parse_state_envelope(raw)


def test_parse_state_envelope_rejects_non_dict_state():
    raw = {"kind": "streamState", "apiVersion": 1, "state": "not a dict"}
    with pytest.raises(
        InvalidStateEnvelopeError, match=r"state\n\s*Input should be a valid dictionary"
    ):
        parse_state_envelope(raw)


def test_parse_state_envelope_rejects_extra_keys():
    """``extra="forbid"`` mirrors the sibling envelope models in
    ``_lib/request.py``: an unknown key signals either schema drift or
    a malformed file, both worth surfacing rather than silently dropping."""
    raw = {
        "kind": "streamState",
        "apiVersion": 1,
        "state": {},
        "unknownField": "x",
    }
    with pytest.raises(
        InvalidStateEnvelopeError, match=r"Extra inputs are not permitted"
    ):
        parse_state_envelope(raw)
