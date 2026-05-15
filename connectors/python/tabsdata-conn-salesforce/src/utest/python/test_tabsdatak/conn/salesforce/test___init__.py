
from datetime import datetime, timezone

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.salesforce.error import SalesforceValidateException
from tabsdatak.conn.salesforce import (
    SalesforceConn,
    SalesforceReportSrc,
    SalesforceSrc,
    SalesforceTokenCredentials,
)
from tabsdatak.conn.salesforce.error import SalesforceErrorCode


pytestmark = pytest.mark.salesforce


def _credentials():
    return SalesforceTokenCredentials(
        username=Secret.lit("etl@example.com"),
        password=Secret.lit("hunter2"),
        security_token=Secret.lit("salesforce-token"),
    )


def _conn():
    return SalesforceConn(
        credentials=_credentials(),
        instance_url="https://example.my.salesforce.com",
        objects=["Account", "Contact"],
        conn_cfg={"salesforce.timeout": 30},
    )


def test_salesforce_src_full_round_trip():
    src = SalesforceSrc(
        conn=_conn(),
        queries=["SELECT Id, Name FROM Account"],
        include_deleted=True,
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        api_version="60.0",
        entity_whitelist=["Account"],
        src_cfg={"chunkSize": 1024},
    )
    loaded = SalesforceSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT Id, Name FROM Account"]
    assert loaded.include_deleted is True
    assert loaded.initial_last_modified == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert loaded.api_version == "60.0"
    assert loaded.entity_whitelist == ["Account"]
    assert loaded.src_cfg == {"chunkSize": 1024}


def test_salesforce_report_src_full_round_trip():
    src = SalesforceReportSrc(
        conn=_conn(),
        reports=["00O000000000001", "00O000000000002"],
        column_name_strategy="label",
        find_report_by="id",
        filter=[("Type", "equals", "Customer")],
        filter_logic="1",
        last_modified_column="LastModifiedDate",
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        entity_whitelist=["report:00O000000000001"],
        src_cfg={"chunkSize": 2048},
    )
    loaded = SalesforceReportSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.reports == ["00O000000000001", "00O000000000002"]
    assert loaded.column_name_strategy == "label"
    assert loaded.find_report_by == "id"
    assert loaded.filter == [("Type", "equals", "Customer")]
    assert loaded.filter_logic == "1"
    assert loaded.last_modified_column == "LastModifiedDate"
    assert loaded.src_cfg == {"chunkSize": 2048}


def test_salesforce_src_round_trip_with_named_conn():
    src = SalesforceSrc(
        conn=NamedConn(name="prod_salesforce"),
        queries=["SELECT Id FROM Account"],
    )
    loaded = SalesforceSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_salesforce"


def test_salesforce_report_src_round_trip_with_named_conn():
    src = SalesforceReportSrc(
        conn=NamedConn(name="prod_salesforce"),
        reports=["00O000000000001"],
    )
    loaded = SalesforceReportSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_salesforce"


def test_salesforce_conn_validate_rejects_category_with_objects():
    with pytest.raises(SalesforceValidateException) as ei:
        SalesforceConn(
            credentials=_credentials(),
            category="standard",
            objects=["Account"],
        )
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_3
