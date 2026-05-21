
from datetime import datetime, timezone

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.common.error import ConnInitException
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
        entity_whitelist=["Account"],
        conn_cfg={"salesforce.timeout": 30},
    )


def test_salesforce_src_full_round_trip():
    src = SalesforceSrc(
        conn=_conn(),
        queries=["SELECT Id, Name FROM Account"],
        include_deleted=True,
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        api_version="60.0",
        src_cfg={"salesforce.logging_level": 20},
    )
    loaded = SalesforceSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT Id, Name FROM Account"]
    assert loaded.include_deleted is True
    assert loaded.initial_last_modified == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert loaded.api_version == "60.0"
    assert loaded.conn.entity_whitelist == ["Account"]
    assert loaded.src_cfg == {"salesforce.logging_level": 20}


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
        src_cfg={"salesforce.logging_level": 20},
    )
    loaded = SalesforceReportSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.reports == ["00O000000000001", "00O000000000002"]
    assert loaded.column_name_strategy == "label"
    assert loaded.find_report_by == "id"
    assert loaded.filter == [("Type", "equals", "Customer")]
    assert loaded.filter_logic == "1"
    assert loaded.last_modified_column == "LastModifiedDate"
    assert loaded.src_cfg == {"salesforce.logging_level": 20}


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


def test_salesforce_report_src_validate_rejects_lmc_without_initial():
    with pytest.raises(SalesforceValidateException) as ei:
        SalesforceReportSrc(
            conn=_conn(),
            reports=["00O000000000001"],
            last_modified_column="LastModifiedDate",
        )
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_17


def test_salesforce_report_src_validate_rejects_initial_without_lmc():
    from datetime import datetime, timezone

    with pytest.raises(SalesforceValidateException) as ei:
        SalesforceReportSrc(
            conn=_conn(),
            reports=["00O000000000001"],
            initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_17


# ---------------------------------------------------------------------------
# src_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_salesforce_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        SalesforceSrc(
            conn=_conn(),
            queries=["SELECT Id FROM Account"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_3


def test_salesforce_src_accepts_known_src_cfg_keys():
    src = SalesforceSrc(
        conn=_conn(),
        queries=["SELECT Id FROM Account"],
        src_cfg={"salesforce.logging_level": 20},
    )
    loaded = SalesforceSrc.from_json(src.to_json())
    assert loaded == src


def test_salesforce_report_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        SalesforceReportSrc(
            conn=_conn(),
            reports=["00O000000000001"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_4


def test_salesforce_report_src_accepts_known_src_cfg_keys():
    src = SalesforceReportSrc(
        conn=_conn(),
        reports=["00O000000000001"],
        src_cfg={"salesforce.logging_level": 20},
    )
    loaded = SalesforceReportSrc.from_json(src.to_json())
    assert loaded == src
