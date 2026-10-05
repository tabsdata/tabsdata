#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import DEBUG_FRAMES_ENV_VAR, TabsdataException
from tabsdatak.conn.common.error import (
    ConnErrorCode,
    ConnException,
    ConnInitException,
    ConnStreamException,
)


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_conn_init_exception_is_a_conn_exception():
    assert issubclass(ConnInitException, ConnException)


def test_conn_stream_exception_is_a_conn_exception():
    assert issubclass(ConnStreamException, ConnException)


# ---------------------------------------------------------------------------
# Error codes
# ---------------------------------------------------------------------------


def test_every_code_constructs_its_conn_exception(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    for code in ConnErrorCode:
        exc = code.exception()
        assert isinstance(exc, ConnException)
        assert type(exc) is code.value.exception


def test_dataclass_tag_codes_carry_a_cannot_create_message(monkeypatch):
    # Per the conn-common convention, every code used as a dataclass
    # decorator tag has a "Cannot create <X>" message template. CONN_8
    # is raised programmatically from ``_config.get_cfg`` and carries
    # a different shape, so it's exempt.
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    dataclass_tag_codes = [
        ConnErrorCode.CONN_1,
        ConnErrorCode.CONN_2,
        ConnErrorCode.CONN_3,
        ConnErrorCode.CONN_4,
        ConnErrorCode.CONN_9,
        ConnErrorCode.CONN_10,
    ]
    for code in dataclass_tag_codes:
        assert code.value.message_template.startswith("Cannot create ")


def test_codes_have_expected_names_with_intentional_gap():
    # Four credential dataclass-tag codes (CONN_1..CONN_4), CONN_8 for
    # the ``get_cfg`` invalid-value path, CONN_9 for ``FqnDef``,
    # CONN_10 for ``GCPHmac``, CONN_11..CONN_16 for the explorer
    # meta-model types (Column, ReportingEntity, ObjectsEntity,
    # ReportingConnMetaModel, ObjectsConnMetaModel, Property), and
    # CONN_17..CONN_30 for the shared field-value rules -- one code per
    # spec in ``types.py`` plus the two FQN rules ``name_validate``
    # builds -- CONN_31 / CONN_32 for the two ways a cfg tree can
    # break the JSON-like value rule, CONN_33 for the stream buffer
    # sentinel, CONN_34 for the stream checkpoint, CONN_35 for the source
    # acknowledgement that keeps failing, and CONN_36 for the stream that
    # restarts too often without staging.
    # CONN_5..CONN_7 were intentionally retired with the CloudLocation
    # move into the per-connector packages; the gap is preserved so
    # callers that still imported the conn-common variants surface
    # AttributeError rather than silently picking up an unrelated code.
    names = [code.name for code in ConnErrorCode]
    assert names == [
        "CONN_1",
        "CONN_2",
        "CONN_3",
        "CONN_4",
        "CONN_8",
        "CONN_9",
        "CONN_10",
        "CONN_11",
        "CONN_12",
        "CONN_13",
        "CONN_14",
        "CONN_15",
        "CONN_16",
        "CONN_17",
        "CONN_18",
        "CONN_19",
        "CONN_20",
        "CONN_21",
        "CONN_22",
        "CONN_23",
        "CONN_24",
        "CONN_25",
        "CONN_26",
        "CONN_27",
        "CONN_28",
        "CONN_29",
        "CONN_30",
        "CONN_31",
        "CONN_32",
        "CONN_33",
        "CONN_34",
        "CONN_35",
        "CONN_36",
    ]
