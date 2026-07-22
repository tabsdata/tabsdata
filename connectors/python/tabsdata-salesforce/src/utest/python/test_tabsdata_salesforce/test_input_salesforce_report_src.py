#
# Copyright 2026 Tabsdata Inc.
#

import copy
import json
import logging
import os

import polars as pl

# noinspection PyPackageRequirements
import pytest

import tabsdata as td
from tabsdata._io.connections.named_connection.kind_registry import kind_for
from tabsdata._tabsserver.function.named_connection_utils import (
    CONNECTIONS_FOLDER,
    TD_URI_REPOSITORY,
)
from tabsdata.exceptions import ParameterValidationError
from test_tabsdata.conftest import (
    MOCK_PROJECT_ID,
    clean_polars_df,
)
from test_tabsdata.fntest import tdfnrun
from test_tabsdata_salesforce.conftest import (
    FAKE_CREDENTIALS,
    TESTING_RESOURCES_FOLDER,
)
from test_tabsdata_salesforce.test_input_salesforcereportsrc_basic.example import (
    input_salesforcereportsrc,
)
from test_tabsdata_salesforce.test_input_salesforcereportsrc_initial_values.example import (  # noqa: E501
    input_salesforcereportsrc_initial_values,
)
from test_tabsdata_salesforce.test_input_salesforcereportsrc_named_conn.example import (
    testing_tabsdata_function as input_salesforcereportsrc_named_conn_generator,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SALESFORCE_NAMED_CONN_NAME = "test_salesforce_named"


def _write_named_conn_repository(tmp_path, name, conn_type, conn_configs, conn_mode):
    """Create a temporary repository with a single named-connection JSON."""
    repo_dir = os.path.join(str(tmp_path), "repository")
    connections_dir = os.path.join(repo_dir, CONNECTIONS_FOLDER, MOCK_PROJECT_ID)
    os.makedirs(connections_dir, exist_ok=True)
    cx_dict = {
        "kind": kind_for(conn_type, conn_mode),
        "apiVersion": "1.0",
        "name": name,
        "conn_configs": conn_configs,
    }
    connection_file = os.path.join(connections_dir, f"{name.lower()}.json")
    with open(connection_file, "w") as f:
        json.dump(cx_dict, f)
    return repo_dir


SFR_TESTING_REPORT = {
    "long_id": "00OgL000004gpqDUAQ",
    "short_id": "00OgL000004gpqD",
    "name": "Foo_for_dev_ci_testing",
}

SFR_INITIAL_VALUES_TESTING_REPORT = "users_dev_ci"


# --- Unit tests ---


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_with_real_conn_properties():
    conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    src = td.SalesforceReportSrc(
        conn=conn,
        report="FAKE_ID",
        column_name_strategy="columnName",
    )
    assert src.conn is conn
    assert src.report == ["FAKE_ID"]
    assert src.column_name_strategy == "columnName"
    assert src.filter is None
    assert src.filter_logic is None
    assert src.last_modified_column is None
    assert src.initial_last_modified is None
    assert src.initial_values == {}
    assert src.api_version is not None  # default filled in by inner source
    # Inner source is built and reflects the conn's credentials.
    assert src._salesforce_report_source is not None
    assert src._salesforce_report_source.credentials is FAKE_CREDENTIALS


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_with_list_report_and_kwargs():
    conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    reports = ["00OgL000004gpqDUAQ", "00OgL000004gpqEUBR"]
    src = td.SalesforceReportSrc(
        conn=conn,
        report=reports,
        column_name_strategy="label",
        find_report_by="id",
        api_version="50.0",
    )
    assert src.report == reports
    assert src.column_name_strategy == "label"
    assert src.find_report_by == "id"
    assert src.api_version == "50.0"


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_conn_setter_rebuilds_inner_source():
    first_conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    src = td.SalesforceReportSrc(
        conn=first_conn,
        report="FAKE_ID",
        column_name_strategy="columnName",
    )
    first_inner = src._salesforce_report_source
    assert first_inner is not None
    assert first_inner.credentials is FAKE_CREDENTIALS

    new_credentials = td.SalesforceTokenCredentials(
        username="other_user",
        password="other_pass",
        security_token="other_token",
    )
    new_conn = td.SalesforceConn(credentials=new_credentials)
    src.conn = new_conn
    assert src.conn is new_conn
    # Reassigning conn must rebuild the inner SalesforceReportSource so
    # downstream calls see the new credentials.
    assert src._salesforce_report_source is not first_inner
    assert src._salesforce_report_source.credentials is new_credentials


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_conn_setter_rejects_bad_type():
    src = td.SalesforceReportSrc(
        conn=td.SalesforceConn(credentials=FAKE_CREDENTIALS),
        report="FAKE_ID",
        column_name_strategy="columnName",
    )
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        src.conn = "not_a_conn"


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_report_wrong_type_raises():
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        td.SalesforceReportSrc(
            conn=td.SalesforceConn(credentials=FAKE_CREDENTIALS),
            report=1,
            column_name_strategy="columnName",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_initial_last_modified_missing_raises():
    with pytest.raises(ValueError):
        td.SalesforceReportSrc(
            conn=td.SalesforceConn(credentials=FAKE_CREDENTIALS),
            report="FAKE REPORT",
            column_name_strategy="columnName",
            last_modified_column="LastModifiedDate",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_last_modified_column_missing_raises():
    with pytest.raises(ValueError):
        td.SalesforceReportSrc(
            conn=td.SalesforceConn(credentials=FAKE_CREDENTIALS),
            report="FAKE REPORT",
            column_name_strategy="columnName",
            initial_last_modified="2024-03-10T11:03:08.000+0000",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_initial_last_modified_no_timezone_raises():
    with pytest.raises(ValueError):
        td.SalesforceReportSrc(
            conn=td.SalesforceConn(credentials=FAKE_CREDENTIALS),
            report="FAKE REPORT",
            initial_last_modified="2098-02-05T11:27:47.000000",
            last_modified_column="LastModifiedDate",
            column_name_strategy="columnName",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_invalid_column_name_strategy_raises():
    with pytest.raises(ParameterValidationError):
        td.SalesforceReportSrc(
            conn=td.SalesforceConn(credentials=FAKE_CREDENTIALS),
            report="FAKE_ID",
            column_name_strategy="not_a_strategy",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_initial_values_getter_and_setter():
    conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    src = td.SalesforceReportSrc(
        conn=conn,
        report="FAKE REPORT",
        column_name_strategy="columnName",
        last_modified_column="fakeColumn",
        initial_last_modified="2024-03-10T11:03:08.000+0000",
    )
    assert src.initial_values == {
        "initial_last_modified": "2024-03-10T11:03:08.000000+0000"
    }
    src.initial_values = {"initial_last_modified": "2025-01-01T00:00:00.000+0000"}
    assert src.initial_values == {
        "initial_last_modified": "2025-01-01T00:00:00.000+0000"
    }


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_filter_and_filter_logic():
    conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    src = td.SalesforceReportSrc(
        conn=conn,
        report="FAKE_ID",
        column_name_strategy="columnName",
        filter=[
            ("CreatedDate", "greaterThan", "2023-01-01T00:00:00.000Z"),
            ("Name", "contains", "foo"),
        ],
        filter_logic="(1 AND 2)",
    )
    assert src.filter == [
        ("CreatedDate", "greaterThan", "2023-01-01T00:00:00.000Z"),
        ("Name", "contains", "foo"),
    ]
    assert src.filter_logic == "(1 AND 2)"


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_filter_logic_without_filter_raises():
    with pytest.raises(ValueError):
        td.SalesforceReportSrc(
            conn=td.SalesforceConn(credentials=FAKE_CREDENTIALS),
            report="FAKE_ID",
            column_name_strategy="columnName",
            filter_logic="(1 AND 2)",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_passthrough_setters_update_inner_source():
    conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    src = td.SalesforceReportSrc(
        conn=conn,
        report="FAKE_ID",
        column_name_strategy="columnName",
    )
    inner = src._salesforce_report_source
    assert inner is not None

    src.report = "NEW_ID"
    assert src.report == ["NEW_ID"]
    assert inner.report == ["NEW_ID"]

    src.column_name_strategy = "label"
    assert src.column_name_strategy == "label"
    assert inner.column_name_strategy == "label"

    src.find_report_by = "name"
    assert src.find_report_by == "name"
    assert inner.find_report_by == "name"

    src.filter = ("CreatedDate", "greaterThan", "2023-01-01T00:00:00.000Z")
    assert src.filter == [("CreatedDate", "greaterThan", "2023-01-01T00:00:00.000Z")]
    assert inner.filter == [("CreatedDate", "greaterThan", "2023-01-01T00:00:00.000Z")]

    src.filter_logic = "(1)"
    assert src.filter_logic == "(1)"
    assert inner.filter_logic == "(1)"

    src.last_modified_column = "LastModifiedDate"
    assert src.last_modified_column == "LastModifiedDate"
    assert inner.last_modified_column == "LastModifiedDate"

    src.api_version = "50.0"
    assert src.api_version == "50.0"
    assert inner.api_version == "50.0"


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcereportsrc_passthrough_setter_validation_propagates():
    src = td.SalesforceReportSrc(
        conn=td.SalesforceConn(credentials=FAKE_CREDENTIALS),
        report="FAKE_ID",
        column_name_strategy="columnName",
    )
    with pytest.raises(ValueError):
        src.column_name_strategy = "not_a_strategy"
    with pytest.raises(ValueError):
        src.find_report_by = "not_a_lookup"
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        src.report = 1


# --- Live chunk() ---


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
def test_salesforcereportsrc_chunk(tmp_path, sf_config):
    conn = td.SalesforceConn(credentials=sf_config["CREDENTIALS"])

    src = td.SalesforceReportSrc(
        conn=conn,
        report="DOES NOT EXIST",
        column_name_strategy="columnName",
    )
    with pytest.raises(Exception):
        src.chunk(str(tmp_path))

    src = td.SalesforceReportSrc(
        conn=conn,
        report=SFR_TESTING_REPORT["long_id"],
        column_name_strategy="columnName",
    )
    [result] = src.chunk(str(tmp_path))
    result = os.path.join(tmp_path, result)
    assert os.path.isfile(result)
    output = pl.read_parquet(result)
    output = clean_polars_df(output)
    assert not output.is_empty()


# --- Full pipeline e2e ---


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_salesforcereportsrc_by_long_id(sf_config, sf_expected_report_basic):
    fn = copy.deepcopy(input_salesforcereportsrc)
    fn.input.conn = td.SalesforceConn(credentials=sf_config["CREDENTIALS"])
    fn.input.report = SFR_TESTING_REPORT["long_id"]
    fnrun = tdfnrun(fn)
    assert fnrun.status == 0
    output = pl.read_parquet(fnrun.output[0].path)
    output = clean_polars_df(output)
    assert output.equals(sf_expected_report_basic)


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_salesforcereportsrc_initial_values_by_label(sf_config):
    fn = copy.deepcopy(input_salesforcereportsrc_initial_values)
    fn.input.conn = td.SalesforceConn(credentials=sf_config["CREDENTIALS"])
    fn.input.report = SFR_INITIAL_VALUES_TESTING_REPORT
    fn.input.last_modified_column = "Last Modified Date"
    fn.input.column_name_strategy = "label"
    fn.input.initial_last_modified = "2024-03-10T11:03:08.000+0000"
    fnrun = tdfnrun(fn, output_initial_values=True)
    assert fnrun.status == 0

    output = pl.read_parquet(fnrun.output[0].path)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_salesforce_report_initial_values",
        "expected_result_by_label.parquet",
    )
    expected_output = pl.read_parquet(expected_output_file)
    expected_output = clean_polars_df(expected_output)
    assert output.shape == expected_output.shape
    assert output.columns == expected_output.columns

    initial_values_file = fnrun.offset
    assert initial_values_file.exists()
    initial_values = pl.read_parquet(initial_values_file)
    expected_initial_values = pl.read_parquet(
        os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_input_salesforce_report_initial_values",
            "expected_initial_values.parquet",
        )
    )
    assert initial_values.shape == expected_initial_values.shape
    assert initial_values.columns == expected_initial_values.columns

    # Second iteration: feed the offset back in; nothing new should land and
    # no new offset file should be written.
    fn2 = copy.deepcopy(input_salesforcereportsrc_initial_values)
    fn2.input.conn = td.SalesforceConn(credentials=sf_config["CREDENTIALS"])
    fn2.input.report = SFR_INITIAL_VALUES_TESTING_REPORT
    fn2.input.last_modified_column = "Last Modified Date"
    fn2.input.column_name_strategy = "label"
    fn2.input.initial_last_modified = "2024-03-10T11:03:08.000+0000"
    fnrun2 = tdfnrun(
        fn2,
        input_initial_values=initial_values_file,
        require_output=False,
    )
    assert fnrun2.status == 0
    assert not fnrun2.output[0].path.exists()
    assert not fnrun2.offset.exists()


# --- NamedConn e2e ---


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_salesforcereportsrc_with_named_conn(sf_config, tmp_path, monkeypatch):
    repo_dir = _write_named_conn_repository(
        tmp_path,
        SALESFORCE_NAMED_CONN_NAME,
        "SalesforceConn",
        {
            "credentials": {
                "username": os.environ.get(sf_config["USERNAME_ENV"]),
                "password": os.environ.get(sf_config["PASSWORD_ENV"]),
                "security_token": os.environ.get(sf_config["SECURITY_TOKEN_ENV"]),
            }
        },
        "src",
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    testing_function = input_salesforcereportsrc_named_conn_generator(
        conn_name=SALESFORCE_NAMED_CONN_NAME,
        report_id=SFR_TESTING_REPORT["long_id"],
        column_name_strategy="columnName",
    )

    fnrun = tdfnrun(function=testing_function)
    assert fnrun.status == 0

    output = pl.read_parquet(fnrun.output[0].path)
    assert not output.is_empty()
