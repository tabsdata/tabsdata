#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.common._storage.failures import collected
from tabsdatak.common._storage.landed import StoredMetadata, serve

# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdatak.conn.salesforce import (
    SalesforceReportSrc,
    SalesforceSrc,
    SalesforceSrcConn,
    SalesforceTokenCredentials,
    _plugin as plugin,
    _reader as reader,
)
from tabsdatak.conn.salesforce._explorer._report_explorer import (
    SalesforceReportsConnectionExplorer,
)
from tabsdatak.conn.salesforce._explorer._soql_explorer import (
    SalesforceSOQLConnectionExplorer,
)
from tabsdatak.conn.salesforce.constants import (
    SALESFORCE_META_LAST_MODIFIED,
    SALESFORCE_META_REPORT_ID,
    SALESFORCE_META_SOQL,
)
from tabsdatak.conn.salesforce.error import (
    SalesforceErrorCode,
    SalesforceRuntimeException,
)
from tabsdatak.spi import FrameMetadata, LazyFrameInput

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
        # describe() of each sObject, by name; one missing fails to describe.
        self.describes: dict[str, dict] = {}
        self.described: list[str] = []

    def query_all_iter(self, query: str, include_deleted: bool = False):
        self.query_all_iter_calls.append((query, include_deleted))
        if self.query_all_iter_raise is not None:
            raise self.query_all_iter_raise
        idx = len(self.query_all_iter_calls) - 1
        if idx < len(self.records_by_query):
            return iter(self.records_by_query[idx])
        return iter([])

    def restful(self, url, method=None, json=None):
        if url.startswith("sobjects/") and url.endswith("/describe"):
            sobject = url.split("/")[1]
            self.described.append(sobject)
            if sobject not in self.describes:
                raise RuntimeError(f"INVALID_TYPE: {sobject}")
            return self.describes[sobject]
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


def _patch_login(monkeypatch, fake_sf: _FakeSF) -> list:
    logins: list = []

    def login(conn, api_version=None):
        logins.append(api_version)
        return fake_sf

    monkeypatch.setattr(reader, "login", login)
    return logins


def _metadata(table: LazyFrameInput) -> FrameMetadata | None:
    return table.metadata() if callable(table.metadata) else table.metadata


def _served(table: LazyFrameInput) -> pl.DataFrame:
    raw = collected(table.frame)
    metadata = _metadata(table) or FrameMetadata()
    return serve(raw.lazy(), StoredMetadata.read(StoredMetadata.of(metadata))).collect()


def _soql(monkeypatch, *records: list[dict], **options) -> tuple:
    fake = _FakeSF()
    fake.records_by_query = list(records)
    logins = _patch_login(monkeypatch, fake)
    queries = [f"SELECT Id FROM T{i}" for i in range(len(records))]
    src = SalesforceSrc(queries=queries, **options)
    return fake, logins, src


def test_src_plugin_reads_nothing_until_an_input_lands(monkeypatch, work_dir):
    fake, logins, src = _soql(monkeypatch, [{"Id": "001"}])
    out = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    assert len(out) == 1
    assert (logins, fake.query_all_iter_calls) == ([], [])
    [[table]] = out
    _served(table)
    assert fake.query_all_iter_calls == [("SELECT Id FROM T0", False)]


def test_src_plugin_hands_over_the_records_of_its_query(monkeypatch, work_dir):
    _, logins, src = _soql(
        monkeypatch,
        [
            {"attributes": {"_": 1}, "Id": "001", "Name": "Acme"},
            {"attributes": {"_": 1}, "Id": "002", "Name": "Initech"},
        ],
    )
    out = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    [[table]] = out
    assert _served(table)["Name"].to_list() == ["Acme", "Initech"]
    assert len(logins) == 1


def test_src_plugin_strips_attributes_at_every_level(monkeypatch, work_dir):
    _, _, src = _soql(
        monkeypatch,
        [
            {
                "attributes": {"type": "Account"},
                "Id": "001",
                "Owner": {"attributes": {"type": "User"}, "Name": "Ada"},
            }
        ],
    )
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    frame = _served(table)
    assert frame.columns == ["Id", "Owner"]
    assert frame["Owner"].to_list() == [{"Name": "Ada"}]


def test_src_plugin_types_each_field_over_the_whole_result(monkeypatch, work_dir):
    _, _, src = _soql(
        monkeypatch,
        [{"Id": "001", "Amount": 1, "Active": True}]
        + [{"Id": "002", "Amount": 2.5, "Active": False}],
    )
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    frame = _served(table)
    assert frame.schema == pl.Schema({
        "Id": pl.String,
        "Amount": pl.Float64,
        "Active": pl.Boolean,
    })


def _field(name, kind, precision=0, scale=0):
    return {"name": name, "type": kind, "precision": precision, "scale": scale}


OPPORTUNITY = {
    "fields": [
        _field("Id", "id"),
        _field("Name", "string"),
        _field("Amount", "currency", 18, 2),
        _field("Probability", "percent", 3, 0),
        _field("ExpectedRevenue", "double", 18, 2),
        _field("TotalOpportunityQuantity", "int", 9, 0),
        _field("CloseDate", "date"),
        _field("LastActivityDate", "date"),
        _field("CreatedDate", "datetime"),
        _field("IsWon", "boolean"),
        _field("StageTime", "time"),
        _field("BillingAddress", "address"),
        _field("OwnerId", "reference"),
    ]
}
USER = {"fields": [_field("Name", "string"), _field("IsActive", "boolean")]}


def _opportunity(**fields):
    return {"attributes": {"type": "Opportunity"}, **fields}


def test_src_plugin_types_each_field_as_its_object_describes_it(monkeypatch, work_dir):
    fake, _, src = _soql(
        monkeypatch,
        [
            _opportunity(
                Id="006",
                Name="Deal",
                Amount=1234.5,
                Probability=90,
                ExpectedRevenue=None,
                TotalOpportunityQuantity=None,
                CloseDate="2026-04-19",
                LastActivityDate=None,
                CreatedDate="2026-04-19T06:30:24.000+0000",
                IsWon=None,
                StageTime="06:30:24.000Z",
                OwnerId=None,
            )
        ],
    )
    fake.describes = {"Opportunity": OPPORTUNITY}
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    frame = _served(table)
    assert frame.schema == pl.Schema({
        "Id": pl.String,
        "Name": pl.String,
        "Amount": pl.Decimal(18, 2),
        "Probability": pl.Decimal(3, 0),
        "ExpectedRevenue": pl.Float64,
        "TotalOpportunityQuantity": pl.Int64,
        "CloseDate": pl.Date,
        "LastActivityDate": pl.Date,
        "CreatedDate": pl.Datetime("ns", "UTC"),
        "IsWon": pl.Boolean,
        "StageTime": pl.String,
        "OwnerId": pl.String,
    })
    assert frame.row(0) == (
        "006",
        "Deal",
        Decimal("1234.50"),
        Decimal("90"),
        None,
        None,
        date(2026, 4, 19),
        None,
        datetime(2026, 4, 19, 6, 30, 24, tzinfo=timezone.utc),
        None,
        "06:30:24.000Z",
        None,
    )
    assert fake.described == ["Opportunity"]


def test_src_plugin_types_a_parents_fields_as_its_object_describes_them(
    monkeypatch, work_dir
):
    fake, _, src = _soql(
        monkeypatch,
        [
            _opportunity(Id="006", Owner=None),
            _opportunity(
                Id="007",
                Owner={"attributes": {"type": "User"}, "Name": "Ada", "IsActive": None},
            ),
        ],
    )
    fake.describes = {"Opportunity": OPPORTUNITY, "User": USER}
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    frame = _served(table)
    assert frame.schema["Owner"] == pl.Struct({
        "Name": pl.String,
        "IsActive": pl.Boolean,
    })
    assert frame["Owner"].to_list() == [None, {"Name": "Ada", "IsActive": None}]


def test_src_plugin_types_an_aggregate_from_its_values(monkeypatch, work_dir):
    fake, _, src = _soql(
        monkeypatch,
        [{"attributes": {"type": "AggregateResult"}, "expr0": 3, "StageName": "Won"}],
    )
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    frame = _served(table)
    assert frame.schema == pl.Schema({"expr0": pl.Int64, "StageName": pl.String})
    assert fake.described == []


def test_src_plugin_types_from_values_an_object_it_cannot_describe(
    monkeypatch, work_dir, caplog
):
    fake, _, src = _soql(monkeypatch, [_opportunity(Id="006", Amount=1.5, IsWon=None)])
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    frame = _served(table)
    assert frame.schema == pl.Schema({
        "Id": pl.String,
        "Amount": pl.Float64,
        "IsWon": pl.String,
    })
    assert any(
        "Cannot describe the Salesforce object 'Opportunity'" in record.getMessage()
        for record in caplog.records
    )


def test_src_plugin_describes_each_object_once(monkeypatch, work_dir):
    fake, _, src = _soql(monkeypatch, [_opportunity(Id="006"), _opportunity(Id="007")])
    fake.describes = {"Opportunity": OPPORTUNITY}
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    _served(table)
    _metadata(table)
    assert fake.described == ["Opportunity"]


@pytest.mark.parametrize(
    "field,dtype",
    [
        (_field("A", "boolean"), pl.Boolean),
        (_field("A", "int", 9), pl.Int64),
        (_field("A", "long", 18), pl.Int64),
        (_field("A", "double", 18, 2), pl.Float64),
        (_field("A", "currency", 18, 2), pl.Decimal(18, 2)),
        (_field("A", "currency", 16, 0), pl.Decimal(16, 0)),
        (_field("A", "percent", 5, 2), pl.Decimal(5, 2)),
        (_field("A", "currency", 0, 0), pl.Float64),
        (_field("A", "currency", 40, 2), pl.Float64),
        (_field("A", "date"), pl.Date),
        (_field("A", "datetime"), pl.Datetime("ns", "UTC")),
        (_field("A", "time"), pl.String),
        (_field("A", "id"), pl.String),
        (_field("A", "reference"), pl.String),
        (_field("A", "picklist"), pl.String),
        (_field("A", "multipicklist"), pl.String),
        (_field("A", "textarea"), pl.String),
        (_field("A", "email"), pl.String),
        (_field("A", "address"), None),
        (_field("A", "location"), None),
        (_field("A", "anyType"), None),
    ],
)
def test_a_described_field_is_typed_as_its_value_decodes(field, dtype):
    assert reader.field_dtype(field) == dtype


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
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    _served(table)
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


def test_src_plugin_wraps_query_failure_as_salesforce_10(monkeypatch, work_dir):
    fake = _FakeSF()
    fake.query_all_iter_raise = RuntimeError("API explode")
    _patch_login(monkeypatch, fake)
    src = SalesforceSrc(queries=["SELECT Id FROM Account"])
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    with pytest.raises(SalesforceRuntimeException) as ei:
        _served(table)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_10


def test_src_plugin_wraps_a_failed_page_as_salesforce_10(monkeypatch, work_dir):
    def pages():
        yield {"Id": "001"}
        raise RuntimeError("page lost")

    fake = _FakeSF()
    fake.query_all_iter = lambda query, include_deleted=False: pages()
    _patch_login(monkeypatch, fake)
    src = SalesforceSrc(queries=["SELECT Id FROM Account"])
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    with pytest.raises(SalesforceRuntimeException) as ei:
        _served(table)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_10
    assert "query_index=0" in str(ei.value)


def test_src_plugin_surfaces_a_failed_login_when_reading(monkeypatch, work_dir):
    def login(conn, api_version=None):
        raise SalesforceErrorCode.SALESFORCE_9.exception(instance_url=None)

    monkeypatch.setattr(reader, "login", login)
    src = SalesforceSrc(queries=["SELECT Id FROM Account"])
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    with pytest.raises(SalesforceRuntimeException) as ei:
        _served(table)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_9


def test_src_plugin_handles_empty_result_set(monkeypatch, work_dir):
    _, _, src = _soql(monkeypatch, [])
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    assert _served(table).shape == (0, 0)
    assert not _metadata(table).known


def _empty(monkeypatch, work_dir, query: str) -> tuple:
    fake = _FakeSF()
    fake.describes = {"Opportunity": OPPORTUNITY}
    _patch_login(monkeypatch, fake)
    src = SalesforceSrc(queries=[query])
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    return fake, table


def test_an_empty_result_is_typed_as_its_object_describes_the_selected_fields(
    monkeypatch, work_dir
):
    fake, table = _empty(
        monkeypatch,
        work_dir,
        "select amount, ID,\n closedate FROM Opportunity WHERE IsWon = true",
    )
    frame = _served(table)
    assert frame.schema == pl.Schema({
        "Amount": pl.Decimal(18, 2),
        "Id": pl.String,
        "CloseDate": pl.Date,
    })
    assert frame.height == 0
    assert _metadata(table).known
    assert fake.described == ["Opportunity"]


@pytest.mark.parametrize(
    "query",
    [
        "SELECT Id, BillingAddress FROM Opportunity",
        "SELECT Id, Missing FROM Opportunity",
        "SELECT Id, Owner.Name FROM Opportunity",
        "SELECT Id, (SELECT Id FROM OpportunityLineItems) FROM Opportunity",
        "SELECT COUNT() FROM Opportunity",
        "SELECT FIELDS(STANDARD) FROM Opportunity",
        "SELECT Id FROM Undescribable",
    ],
    ids=[
        "untyped field",
        "unknown field",
        "relationship",
        "subquery",
        "function",
        "fields",
        "undescribable object",
    ],
)
def test_an_empty_result_whose_fields_cannot_be_typed_is_unknown(
    monkeypatch, work_dir, query
):
    _, table = _empty(monkeypatch, work_dir, query)
    assert _served(table).shape == (0, 0)
    assert not _metadata(table).known


def test_src_plugin_reports_soql_and_cutoff_as_metadata(monkeypatch, work_dir):
    fake = _FakeSF()
    fake.records_by_query = [[{"attributes": {}, "Id": "001"}]]
    _patch_login(monkeypatch, fake)
    src = SalesforceSrc(
        queries=[
            "SELECT Id FROM Account WHERE SystemModstamp > $lastModified",
        ],
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    _served(table)
    columns = _metadata(table).columns
    # The reported SOQL is the query as sent -- token already substituted.
    assert columns[SALESFORCE_META_SOQL] == fake.query_all_iter_calls[0][0]
    assert "$lastModified" not in columns[SALESFORCE_META_SOQL]
    assert columns[SALESFORCE_META_LAST_MODIFIED].startswith("2026-01-01")


def test_src_plugin_metadata_cutoff_empty_without_initial_last_modified(
    monkeypatch,
    work_dir,
):
    fake = _FakeSF()
    fake.records_by_query = [[{"attributes": {}, "Id": "001"}]]
    _patch_login(monkeypatch, fake)
    src = SalesforceSrc(queries=["SELECT Id FROM Account"])
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    _served(table)
    # The metadata never changes shape between a full and an
    # incremental read: both keys are always present.
    assert _metadata(table).columns == {
        SALESFORCE_META_SOQL: "SELECT Id FROM Account",
        SALESFORCE_META_LAST_MODIFIED: "",
    }


def test_src_plugin_drops_metadata_when_src_cfg_asks(monkeypatch, work_dir):
    fake = _FakeSF()
    fake.records_by_query = [[{"attributes": {}, "Id": "001"}]]
    _patch_login(monkeypatch, fake)
    src = SalesforceSrc(
        queries=["SELECT Id FROM Account"],
        src_cfg={"tabsdata.src_metadata.drop": True},
    )
    [[table]] = plugin._SalesforceSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    _served(table)
    assert _metadata(table).columns is None


def test_record_lines_hand_out_whole_records_in_the_sizes_asked():
    lines = reader.RecordLines(iter([{"a": 1}, {"a": 2}, {"a": "é"}]), 0)
    data = b""
    while chunk := lines.read(5):
        assert len(chunk) <= 5
        data += chunk
    assert data.decode("utf-8").splitlines() == ['{"a":1}', '{"a":2}', '{"a":"é"}']


# ---------------------------------------------------------------------------
# _SalesforceReportSrcPlugin -- mock the metadata + restful surface
# ---------------------------------------------------------------------------


def _drained(out) -> list[list[LazyFrameInput]]:
    return [list(slot) for slot in out]


def test_report_plugin_raises_salesforce_13_when_lookup_misses(
    monkeypatch,
    work_dir,
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
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    with pytest.raises(SalesforceRuntimeException) as ei:
        _drained(out)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_13


def test_report_plugin_raises_salesforce_15_on_unsupported_format(
    monkeypatch,
    work_dir,
):
    fake = _FakeSF()
    fake.query_returns = {"records": [{"Id": "00O000000000001", "Format": "Matrix"}]}
    _patch_login(monkeypatch, fake)
    src = SalesforceReportSrc(
        reports=["00O000000000001"],
        find_report_by="id",
    )
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    with pytest.raises(SalesforceRuntimeException) as ei:
        _drained(out)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_15


def test_report_plugin_wraps_other_failures_as_salesforce_16(monkeypatch, work_dir):
    fake = _FakeSF()
    fake.query_returns = {"records": [{"Id": "00O000000000001", "Format": "Tabular"}]}
    _patch_login(monkeypatch, fake)
    _patch_report_run(monkeypatch, [["a"]])
    monkeypatch.setattr(
        reader,
        "_launch_report_instance",
        MagicMock(side_effect=RuntimeError("launch failed")),
    )
    src = SalesforceReportSrc(reports=["00O000000000001"], find_report_by="id")
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    with pytest.raises(SalesforceRuntimeException) as ei:
        _drained(out)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_16
    assert "report_index=0" in str(ei.value)


class _Response:
    def __init__(self, rows: list[list[Any]], all_data: bool = True) -> None:
        body = {
            "allData": all_data,
            "factMap": {
                "T!T": {
                    "rows": [
                        {
                            "dataCells": [
                                {"label": "-" if v is None else str(v), "value": v}
                                for v in row
                            ]
                        }
                        for row in rows
                    ]
                }
            },
        }
        self.raw = io.BytesIO(json.dumps(body).encode("utf-8"))
        self.headers: dict = {}
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _patch_report_run(
    monkeypatch,
    rows: list[list[Any]],
    columns: list[tuple[str, str]] | None = None,
) -> list[_Response]:
    """Stub out everything between report-id resolution and the result
    stream so a report read can be driven without an HTTP layer."""
    columns = columns or [("C", "string")]
    names = [name for name, _ in columns]
    monkeypatch.setattr(
        reader,
        "_obtain_report_metadata",
        lambda _sf, _report_id, _report: (
            names,
            {
                "columnName": {name: name for name in names},
                "label": {name: name for name in names},
            },
            [kind for _, kind in columns],
            {
                name: {"columnName": name, "label": name, "dataType": kind}
                for name, kind in columns
            },
        ),
    )
    monkeypatch.setattr(reader, "_launch_report_instance", lambda **_kw: "instance-1")
    responses: list[_Response] = []

    def stream(**_kw):
        responses.append(_Response(rows))
        return responses[-1]

    monkeypatch.setattr(reader, "_await_report_stream", stream)
    return responses


def _report(monkeypatch, rows, columns=None, **options):
    fake = _FakeSF()
    fake.query_returns = {"records": [{"Id": "00O000000000001", "Format": "Tabular"}]}
    logins = _patch_login(monkeypatch, fake)
    responses = _patch_report_run(monkeypatch, rows, columns)
    options.setdefault("reports", ["00O000000000001"])
    options.setdefault("find_report_by", "id")
    return logins, responses, SalesforceReportSrc(**options)


def test_report_plugin_runs_nothing_until_read(monkeypatch, work_dir):
    logins, responses, src = _report(monkeypatch, [["a"]])
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    assert (logins, responses) == ([], [])
    [[table]] = _drained(out)
    assert table.frame.collect()["C"].to_list() == ["a"]
    assert [response.closed for response in responses] == [True]


def test_report_plugin_hands_over_typed_batches(monkeypatch, work_dir):
    monkeypatch.setattr(reader, "_SFR_CHUNK_ROWS", 2)
    _, _, src = _report(monkeypatch, [["a"], ["b"], ["c"]])
    reader_batches = reader._report_batches
    monkeypatch.setattr(
        reader,
        "_report_batches",
        lambda **kw: reader_batches(**{**kw, "chunk_size": 2}),
    )
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    [tables] = _drained(out)
    assert [table.frame.collect()["C"].to_list() for table in tables] == [
        ["a", "b"],
        ["c"],
    ]


def test_report_plugin_closes_the_result_when_reading_stops(monkeypatch, work_dir):
    _, responses, src = _report(monkeypatch, [["a"]])
    [slot] = plugin._SalesforceReportSrcPlugin().read_in(
        _src_ctx(work_dir), _conn(), src
    )
    tables = iter(slot)
    next(tables)
    tables.close()
    assert [response.closed for response in responses] == [True]


def test_report_plugin_reports_resolved_id_and_cutoff_as_metadata(
    monkeypatch,
    work_dir,
):
    _, _, src = _report(
        monkeypatch,
        [["a", None]],
        [("C", "string"), ("LastModifiedDate", "datetime")],
        # Named by name -- the metadata must carry the *resolved* id.
        reports=["Quarterly Pipeline"],
        find_report_by="name",
        last_modified_column="LastModifiedDate",
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    [[table]] = _drained(out)
    columns = _metadata(table).columns
    assert columns[SALESFORCE_META_REPORT_ID] == "00O000000000001"
    assert columns[SALESFORCE_META_LAST_MODIFIED].startswith("2026-01-01")


def test_report_plugin_metadata_cutoff_empty_without_initial_last_modified(
    monkeypatch,
    work_dir,
):
    _, _, src = _report(monkeypatch, [["a"]])
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    [[table]] = _drained(out)
    assert _metadata(table).columns == {
        SALESFORCE_META_REPORT_ID: "00O000000000001",
        SALESFORCE_META_LAST_MODIFIED: "",
    }


def test_report_plugin_drops_metadata_when_src_cfg_asks(monkeypatch, work_dir):
    _, _, src = _report(
        monkeypatch,
        [["a"]],
        src_cfg={"tabsdata.src_metadata.drop": True},
    )
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    [[table]] = _drained(out)
    assert table.metadata is None


COLUMNS = [
    ("s", "string"),
    ("i", "int"),
    ("n", "long"),
    ("d", "double"),
    ("b", "boolean"),
    ("day", "date"),
    ("at", "datetime"),
    ("t", "time"),
    ("p", "picklist"),
]

SCHEMA = pl.Schema({
    "s": pl.String,
    "i": pl.Int64,
    "n": pl.Int64,
    "d": pl.Float64,
    "b": pl.Boolean,
    "day": pl.Datetime("ns", "UTC"),
    "at": pl.Datetime("ns", "UTC"),
    "t": pl.Datetime("ns", "UTC"),
    "p": pl.String,
})


def test_report_values_take_their_column_types(monkeypatch, work_dir):
    rows = [
        [
            "x",
            1,
            12345678901,
            2.5,
            False,
            "2026-01-15",
            "2026-01-15T10:20:30Z",
            "10:20:30.000Z",
            "Open",
        ],
        ["-", None, None, None, True, None, None, None, None],
    ]
    _, _, src = _report(monkeypatch, rows, COLUMNS)
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    [[table]] = _drained(out)
    frame = table.frame.collect()
    assert frame.schema == SCHEMA
    assert frame.row(0) == (
        "x",
        1,
        12345678901,
        2.5,
        False,
        datetime(2026, 1, 15, tzinfo=timezone.utc),
        datetime(2026, 1, 15, 10, 20, 30, tzinfo=timezone.utc),
        datetime(1970, 1, 1, 10, 20, 30, tzinfo=timezone.utc),
        "Open",
    )
    assert frame.row(1) == (None, None, None, None, True, None, None, None, None)


def test_an_empty_report_is_one_empty_table_of_its_types(monkeypatch, work_dir):
    _, _, src = _report(monkeypatch, [], COLUMNS)
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    [[table]] = _drained(out)
    frame = table.frame.collect()
    assert (frame.height, frame.schema) == (0, SCHEMA)


# ---------------------------------------------------------------------------
# SrcDef wiring
# ---------------------------------------------------------------------------


def test_salesforce_src_def_wires_plugin_and_explorer():
    assert plugin.SALESFORCE_SRC.conn is SalesforceSrcConn
    assert plugin.SALESFORCE_SRC.src is SalesforceSrc
    assert plugin.SALESFORCE_SRC.plugin is plugin._SalesforceSrcPlugin
    assert plugin.SALESFORCE_SRC.explorer is SalesforceSOQLConnectionExplorer
    assert plugin.SALESFORCE_SRC.type_ == "salesforce-sosql-in"


def test_salesforce_report_src_def_wires_plugin_and_explorer():
    assert plugin.SALESFORCE_REPORT_SRC.conn is SalesforceSrcConn
    assert plugin.SALESFORCE_REPORT_SRC.src is SalesforceReportSrc
    assert plugin.SALESFORCE_REPORT_SRC.plugin is plugin._SalesforceReportSrcPlugin
    assert plugin.SALESFORCE_REPORT_SRC.explorer is SalesforceReportsConnectionExplorer
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
    assert cfg.drop_metadata is False


def test_resolve_src_cfg_applies_overrides():
    cfg = plugin._resolve_src_cfg({
        "salesforce.logging_level": 20,
        "tabsdata.src_metadata.drop": True,
    })
    assert cfg.logging_level == 20
    assert cfg.drop_metadata is True


def test_resolve_src_cfg_warns_on_unknown_key_via_raw_dict(caplog):
    plugin._resolve_src_cfg({"bogus.key": "value"})
    assert any("Unknown src_cfg key" in r.getMessage() for r in caplog.records)


def _launches(monkeypatch) -> list[dict]:
    launched: list[dict] = []

    def launch(**kwargs):
        launched.append(kwargs)
        return "instance-1"

    monkeypatch.setattr(reader, "_launch_report_instance", launch)
    return launched


_STAMPED = [("C", "string"), ("LastModifiedDate", "datetime")]
_CUTOFF = datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc)


def test_report_last_modified_column_filters_on_the_server(monkeypatch, work_dir):
    _, _, src = _report(
        monkeypatch,
        [["a", None]],
        _STAMPED,
        last_modified_column="LastModifiedDate",
        initial_last_modified=_CUTOFF,
    )
    launched = _launches(monkeypatch)
    _drained(
        plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    )
    [launch] = launched
    assert launch["filters"] == [
        ("LastModifiedDate", "greaterThan", "2026-01-01T12:30:00Z")
    ]
    assert launch["filter_logic"] is None


def test_report_last_modified_filter_is_anded_onto_the_user_logic(
    monkeypatch, work_dir
):
    _, _, src = _report(
        monkeypatch,
        [["a", None]],
        _STAMPED,
        filter=[("C", "equals", "x"), ("C", "equals", "y")],
        filter_logic="1 OR 2",
        last_modified_column="LastModifiedDate",
        initial_last_modified=_CUTOFF,
    )
    launched = _launches(monkeypatch)
    _drained(
        plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    )
    [launch] = launched
    assert launch["filters"][-1][0] == "LastModifiedDate"
    assert launch["filter_logic"] == "(1 OR 2) AND 3"


def test_report_cutoff_is_sent_in_utc():
    from datetime import timedelta

    madrid = timezone(timedelta(hours=1))
    assert (
        reader._report_datetime(datetime(2026, 1, 1, 13, 30, tzinfo=madrid))
        == "2026-01-01T12:30:00Z"
    )
    assert reader._report_datetime(datetime(2026, 1, 1, 12, 30)) == (
        "2026-01-01T12:30:00Z"
    )


def test_report_unknown_column_is_named(monkeypatch, work_dir):
    _, _, src = _report(
        monkeypatch,
        [["a"]],
        last_modified_column="Missing",
        initial_last_modified=_CUTOFF,
    )
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    with pytest.raises(SalesforceRuntimeException) as ei:
        _drained(out)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_19
    assert "Missing" in str(ei.value)


def test_report_truncated_by_the_api_is_refused(monkeypatch, work_dir):
    _, _, src = _report(monkeypatch, [["a"]])
    monkeypatch.setattr(
        reader, "_await_report_stream", lambda **_kw: _Response([["a"]], False)
    )
    out = plugin._SalesforceReportSrcPlugin().read_in(_src_ctx(work_dir), _conn(), src)
    with pytest.raises(SalesforceRuntimeException) as ei:
        _drained(out)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_21


class _PollingSF:
    base_url = "https://example.my.salesforce.com/services/data/v60.0/"
    session_id = "token"

    def __init__(self, status: str) -> None:
        self.status = status

    def restful(self, url):
        return {"attributes": {"status": self.status}}


def test_report_that_never_finishes_times_out():
    with pytest.raises(SalesforceRuntimeException) as ei:
        reader._await_report_stream(
            sf=_PollingSF("Running"),
            report_id="00O000000000001",
            instance_id="i",
            maximum_wait_time=0,
            poll_interval=0,
        )
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_20


class _Fetched:
    def __init__(self, failing: bool) -> None:
        self.failing = failing
        self.closed = False

    def raise_for_status(self):
        if self.failing:
            raise RuntimeError("403")

    def close(self):
        self.closed = True


@pytest.mark.parametrize("failing", [False, True])
def test_report_result_is_fetched_with_a_timeout_and_checked(monkeypatch, failing):
    import requests

    calls: list[dict] = []
    fetched = _Fetched(failing)

    def get(url, **kwargs):
        calls.append(kwargs)
        return fetched

    monkeypatch.setattr(requests, "get", get)
    if failing:
        with pytest.raises(RuntimeError, match="403"):
            reader._await_report_stream(
                sf=_PollingSF("Success"), report_id="r", instance_id="i"
            )
        assert fetched.closed
    else:
        reader._await_report_stream(
            sf=_PollingSF("Success"), report_id="r", instance_id="i"
        )
    assert calls[0]["timeout"] == (
        reader._SFR_CONNECT_SECONDS,
        reader._SFR_READ_SECONDS,
    )
