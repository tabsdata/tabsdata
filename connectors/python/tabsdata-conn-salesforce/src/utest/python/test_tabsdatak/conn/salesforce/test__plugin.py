from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.salesforce import (
    SalesforceSrcConn,
    SalesforceReportSrc,
    SalesforceSrc,
    SalesforceTokenCredentials,
)
# noinspection PyProtectedMember
from tabsdatak.conn.salesforce import _plugin as plugin

# noinspection PyProtectedMember
from tabsdatak.conn.salesforce import _reader as reader
from tabsdatak.conn.salesforce.error import (
    SalesforceErrorCode,
    SalesforceRuntimeException,
)
from tabsdatak.spi import TableFileInput


pytestmark = pytest.mark.salesforce


# ---------------------------------------------------------------------------
# Test context shim for SrcContext
# ---------------------------------------------------------------------------


@dataclass
class _Ctx:
    _work: Path

    @property
    def work_dir(self) -> Path:
        return self._work


def _src_ctx(work: Path) -> _Ctx:
    return _Ctx(work)


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


def _credentials() -> SalesforceTokenCredentials:
    return SalesforceTokenCredentials(
        username=Secret("etl@example.com"),
        password=Secret("hunter2"),
        security_token=Secret("salesforce-token"),
    )


def _conn() -> SalesforceSrcConn:
    return SalesforceSrcConn(credentials=_credentials())


# ---------------------------------------------------------------------------
# Fake simple_salesforce.Salesforce client
# ---------------------------------------------------------------------------


class _FakeSF:
    """Drop-in for ``simple_salesforce.Salesforce`` that records inputs
    and returns canned data. Each test customises the canned data on
    the instance before invoking the plugin."""

    def __init__(self):
        self.query_all_iter_calls: list[tuple[str, bool]] = []
        self.records_by_query: list[list[dict]] = []
        # Set ``query_all_iter_raise`` to an Exception to simulate
        # API failures.
        self.query_all_iter_raise: Exception | None = None
        # Report side -- minimal shape so the report flow round-trips
        # without an HTTP layer.
        self.restful_returns: list[Any] = []
        self.query_returns: dict | None = None
        self.session_id = "fake-session"
        self.base_url = "https://fake.my.salesforce.com/services/data/v60.0/"
        # ``_obtain_report_metadata`` reaches into ``session`` + ``headers``;
        # tests can override these if they want to exercise it.
        self.session = MagicMock()
        self.headers = {}

    def query_all_iter(self, query: str, include_deleted: bool = False):
        self.query_all_iter_calls.append((query, include_deleted))
        if self.query_all_iter_raise is not None:
            raise self.query_all_iter_raise
        idx = len(self.query_all_iter_calls) - 1
        if idx < len(self.records_by_query):
            return iter(self.records_by_query[idx])
        return iter([])

    def restful(self, url, method=None, json=None):
        if not self.restful_returns:
            return {}
        return self.restful_returns.pop(0)

    def query(self, soql):
        return self.query_returns or {"records": []}


# ---------------------------------------------------------------------------
# _login -- credentials wiring + error wrapping
# ---------------------------------------------------------------------------


def test_login_wraps_simple_salesforce_failure_as_salesforce_10(monkeypatch):
    def boom(**kwargs):
        raise RuntimeError("auth went sideways")

    fake_module = MagicMock()
    fake_module.Salesforce = boom
    fake_module.api.DEFAULT_API_VERSION = "60.0"
    with patch.dict(
        "sys.modules",
        {"simple_salesforce": fake_module},
    ):
        with pytest.raises(SalesforceRuntimeException) as ei:
            reader.login(_conn())
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_9


def test_login_passes_credentials_to_simple_salesforce(monkeypatch):
    captured: dict[str, Any] = {}

    def fake_ctor(**kwargs):
        captured.update(kwargs)
        return "client"

    fake_module = MagicMock()
    fake_module.Salesforce = fake_ctor
    fake_module.api.DEFAULT_API_VERSION = "60.0"
    with patch.dict(
        "sys.modules",
        {"simple_salesforce": fake_module},
    ):
        out = reader.login(_conn(), api_version="55.0")
    assert out == "client"
    assert captured["username"] == "etl@example.com"
    assert captured["password"] == "hunter2"
    assert captured["security_token"] == "salesforce-token"
    assert captured["version"] == "55.0"


def test_login_rejects_unsupported_credentials_type():
    from tabsdatak.conn.salesforce import SalesforceCredentials

    # Build a SalesforceSrcConn through __new__ to bypass the dataclass'
    # validator -- we want to exercise the runtime check inside
    # ``_reader.login`` directly.
    class _OtherCreds(SalesforceCredentials):
        pass

    conn = MagicMock()
    conn.credentials = _OtherCreds.__new__(_OtherCreds)
    conn.instance_url = None
    with pytest.raises(SalesforceRuntimeException) as ei:
        reader.login(conn)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_12


# ---------------------------------------------------------------------------
# _SalesforceSrcPlugin.read_in -- happy path with mocked SOQL client
# ---------------------------------------------------------------------------


def _patch_login(monkeypatch, fake_sf: _FakeSF) -> None:
    monkeypatch.setattr(reader, "login", lambda conn, api_version=None: fake_sf)


def _read_parquet(tfi: TableFileInput) -> pl.DataFrame:
    return pl.read_parquet(tfi.file)


def test_src_plugin_writes_one_parquet_per_query(monkeypatch, work_dir):
    fake = _FakeSF()
    fake.records_by_query = [
        [
            {"attributes": {"_": 1}, "Id": "001", "Name": "Acme"},
            {"attributes": {"_": 1}, "Id": "002", "Name": "Initech"},
        ],
    ]
    _patch_login(monkeypatch, fake)

    src = SalesforceSrc(
        queries=["SELECT Id, Name FROM Account"],
    )
    out = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)

    assert len(out) == 1
    assert len(out[0]) == 1
    df0 = _read_parquet(out[0][0])
    assert df0["Name"].to_list() == ["Acme", "Initech"]
    # SOQL passthrough: the query string is unchanged in this case.
    assert fake.query_all_iter_calls[0][0] == "SELECT Id, Name FROM Account"


def test_src_plugin_strips_attributes_column(monkeypatch, work_dir):
    fake = _FakeSF()
    fake.records_by_query = [
        [{"attributes": {"x": 1}, "Id": "001"}],
    ]
    _patch_login(monkeypatch, fake)
    src = SalesforceSrc(queries=["SELECT Id FROM Account"])
    out = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    df = _read_parquet(out[0][0])
    assert list(df.columns) == ["Id"]


def test_src_plugin_replaces_last_modified_token(monkeypatch, work_dir):
    fake = _FakeSF()
    fake.records_by_query = [[{"attributes": {}, "Id": "001"}]]
    _patch_login(monkeypatch, fake)
    src = SalesforceSrc(
        queries=[
            "SELECT Id FROM Account WHERE SystemModstamp > $lastModified",
        ],
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    # The token must have been replaced inside the rewritten query.
    actual_query = fake.query_all_iter_calls[0][0]
    assert "$lastModified" not in actual_query
    assert "2026-01-01" in actual_query


def test_src_plugin_rejects_token_without_initial_value(monkeypatch, work_dir):
    fake = _FakeSF()
    _patch_login(monkeypatch, fake)
    src = SalesforceSrc(
        queries=[
            "SELECT Id FROM Account WHERE SystemModstamp > $lastModified",
        ],
    )
    with pytest.raises(SalesforceRuntimeException) as ei:
        plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_11


def test_src_plugin_wraps_query_failure_as_salesforce_11(monkeypatch, work_dir):
    fake = _FakeSF()
    fake.query_all_iter_raise = RuntimeError("API explode")
    _patch_login(monkeypatch, fake)
    src = SalesforceSrc(queries=["SELECT Id FROM Account"])
    with pytest.raises(SalesforceRuntimeException) as ei:
        plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_10


def test_src_plugin_handles_empty_result_set(monkeypatch, work_dir):
    fake = _FakeSF()
    fake.records_by_query = [[]]
    _patch_login(monkeypatch, fake)
    src = SalesforceSrc(queries=["SELECT Id FROM Account"])
    out = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    # Even on empty results we must emit one parquet so the framework
    # has a slot to consume.
    assert len(out) == 1
    assert out[0][0].file.exists()


# ---------------------------------------------------------------------------
# _SalesforceReportSrcPlugin -- mock the metadata + restful surface
# ---------------------------------------------------------------------------


def test_report_plugin_raises_salesforce_14_when_lookup_misses(
    monkeypatch, work_dir,
):
    fake = _FakeSF()
    # Empty query result -- _lookup_report returns None and
    # _resolve_report_id raises SALESFORCE_13.
    fake.query_returns = {"records": []}
    _patch_login(monkeypatch, fake)
    src = SalesforceReportSrc(
        reports=["NotARealReport"],
        find_report_by="name",
    )
    with pytest.raises(SalesforceRuntimeException) as ei:
        plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_13


def test_report_plugin_raises_salesforce_16_on_unsupported_format(
    monkeypatch, work_dir,
):
    fake = _FakeSF()
    fake.query_returns = {
        "records": [{"Id": "00O000000000001", "Format": "Matrix"}]
    }
    _patch_login(monkeypatch, fake)
    src = SalesforceReportSrc(
        reports=["00O000000000001"],
        find_report_by="id",
    )
    with pytest.raises(SalesforceRuntimeException) as ei:
        plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_15


# ---------------------------------------------------------------------------
# Explorer stubs -- the methods are TODO placeholders
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    exp = plugin._SalesforceExplorer()
    assert exp.metadata(_conn()) is None
    assert exp.data(_conn(), "SELECT Id FROM Account") is None


# ---------------------------------------------------------------------------
# SrcDef wiring
# ---------------------------------------------------------------------------


def test_salesforce_src_def_wires_plugin_and_explorer():
    assert plugin.SALESFORCE_SRC.conn is SalesforceSrcConn
    assert plugin.SALESFORCE_SRC.src is SalesforceSrc
    assert plugin.SALESFORCE_SRC.plugin is plugin._SalesforceSrcPlugin
    assert plugin.SALESFORCE_SRC.explorer is plugin._SalesforceExplorer
    assert plugin.SALESFORCE_SRC.type_ == "salesforce-sosql-in"


def test_salesforce_report_src_def_wires_plugin_and_explorer():
    assert plugin.SALESFORCE_REPORT_SRC.conn is SalesforceSrcConn
    assert plugin.SALESFORCE_REPORT_SRC.src is SalesforceReportSrc
    assert plugin.SALESFORCE_REPORT_SRC.plugin is plugin._SalesforceReportSrcPlugin
    assert plugin.SALESFORCE_REPORT_SRC.explorer is plugin._SalesforceExplorer
    assert plugin.SALESFORCE_REPORT_SRC.type_ == "salesforce-report-in"


def test_salesforce_src_cardinality_uses_queries_length():
    src = SalesforceSrc(
        queries=["SELECT Id FROM Account"],
    )
    assert plugin.SALESFORCE_SRC.cardinality(src) == 1


def test_salesforce_report_src_cardinality_uses_reports_length():
    src = SalesforceReportSrc(
        reports=["A", "B", "C"],
    )
    assert plugin.SALESFORCE_REPORT_SRC.cardinality(src) == 3


# ---------------------------------------------------------------------------
# _resolve_src_cfg
# ---------------------------------------------------------------------------


def test_resolve_src_cfg_returns_defaults_when_empty():
    import logging as _logging
    cfg = plugin._resolve_src_cfg(None)
    assert cfg.logging_level == _logging.ERROR


def test_resolve_src_cfg_applies_overrides():
    cfg = plugin._resolve_src_cfg({"salesforce.logging_level": 20})
    assert cfg.logging_level == 20


def test_resolve_src_cfg_warns_on_unknown_key_via_raw_dict(caplog):
    with caplog.at_level("WARNING", logger=plugin.logger.name):
        plugin._resolve_src_cfg({"bogus.key": "value"})
    assert any("Unknown src_cfg key" in r.getMessage() for r in caplog.records)
