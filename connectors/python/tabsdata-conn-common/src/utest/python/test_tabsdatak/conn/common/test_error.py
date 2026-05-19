from __future__ import annotations

from tabsdatak.error import DEBUG_FRAMES_ENV_VAR, TabsdataException
from tabsdatak.conn.common.error import (
    ConnErrorCode,
    ConnException,
    ConnInitException,
)


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_conn_init_exception_is_a_conn_exception():
    assert issubclass(ConnInitException, ConnException)


# ---------------------------------------------------------------------------
# Error codes
# ---------------------------------------------------------------------------


def test_every_code_constructs_a_conn_init_exception(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    for code in ConnErrorCode:
        exc = code.exception()
        # Every code in conn-common is currently tagged with
        # ConnInitException (none of the validators raise typed
        # exceptions; cross-field hooks moved to per-connector codes).
        assert isinstance(exc, ConnInitException)
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
    # CONN_10 for ``GCPHmac``. CONN_5..CONN_7 were intentionally
    # retired with the CloudLocation move into the per-connector
    # packages; the gap is preserved so callers that still imported
    # the conn-common variants surface AttributeError rather than
    # silently picking up an unrelated code.
    names = [code.name for code in ConnErrorCode]
    assert names == ["CONN_1", "CONN_2", "CONN_3", "CONN_4", "CONN_8", "CONN_9", "CONN_10"]
