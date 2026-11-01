#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdrunner.function._commons.document.state.stream import (
    API_VERSION,
    KIND,
    InvalidStreamStateError,
    StreamState,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

EXTERNAL_ID = "2026-01-15T12-00-00.000000"
TABSDATA_VERSION = "2.0.0"


def envelope(**changes: object) -> dict:
    return {
        "kind": KIND,
        "apiVersion": API_VERSION,
        "tabsdataVersion": TABSDATA_VERSION,
        "externalId": EXTERNAL_ID,
        "state": {"offset": 42},
        **changes,
    }


def test_the_envelope_kind_and_api_version_are_pinned():
    assert KIND == "streamState"
    assert API_VERSION == 1


def test_a_stream_state_writes_the_envelope_keys():
    state = StreamState.of(
        {"offset": 42},
        external_id=EXTERNAL_ID,
        tabsdata_version=TABSDATA_VERSION,
    )
    assert state.envelope() == envelope()


def test_an_empty_state_is_a_valid_envelope():
    state = StreamState.of(
        {},
        external_id=EXTERNAL_ID,
        tabsdata_version=TABSDATA_VERSION,
    )
    assert StreamState.parse(state.envelope()).state == {}


def test_an_envelope_round_trips():
    state = StreamState.of(
        {"offset": 42, "topic": "people"},
        external_id=EXTERNAL_ID,
        tabsdata_version=TABSDATA_VERSION,
    )
    assert StreamState.parse(state.envelope()) == state


def test_an_envelope_without_its_optional_keys_parses():
    state = StreamState.parse({"kind": KIND, "apiVersion": API_VERSION, "state": {}})
    assert state.external_id is None
    assert state.tabsdata_version is None


@pytest.mark.parametrize(
    "value",
    [
        [1, 2, 3],
        envelope(kind="somethingElse"),
        envelope(apiVersion=2),
        envelope(state="not a dict"),
        envelope(unknownField="x"),
        {key: value for key, value in envelope().items() if key != "kind"},
        {key: value for key, value in envelope().items() if key != "state"},
    ],
    ids=[
        "not a dict",
        "wrong kind",
        "unsupported api version",
        "state not a dict",
        "unknown key",
        "no kind",
        "no state",
    ],
)
def test_an_invalid_envelope_is_refused(value):
    with pytest.raises(InvalidStreamStateError):
        StreamState.parse(value)
