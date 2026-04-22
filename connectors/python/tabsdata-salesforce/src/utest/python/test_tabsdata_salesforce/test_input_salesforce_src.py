#
# Copyright 2026 Tabs Data Inc.
#

import copy
import logging
import os

import polars as pl

# noinspection PyPackageRequirements
import pytest

import tabsdata as td
from tabsdata.exceptions import ParameterValidationError
from test_tabsdata.conftest import (
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.fntest import tdfnrun
from test_tabsdata_salesforce.conftest import (
    FAKE_CREDENTIALS,
    TESTING_RESOURCES_FOLDER,
)
from test_tabsdata_salesforce.test_input_salesforcesrc_basic.example import (
    input_salesforcesrc,
)
from test_tabsdata_salesforce.test_input_salesforcesrc_initial_values.example import (
    input_salesforcesrc_initial_values,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# --- Unit tests ---


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcesrc_with_real_conn_properties():
    conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    src = td.SalesforceSrc(
        conn=conn,
        query="SELECT Name FROM Contact",
    )
    assert src.conn is conn
    assert src.query == ["SELECT Name FROM Contact"]
    assert src.include_deleted is False
    assert src.initial_last_modified is None
    assert src.initial_values == {}
    assert src.api_version is not None  # default filled in by inner source
    # Inner source is built and reflects the conn's credentials.
    assert src._salesforce_source is not None
    assert src._salesforce_source.credentials is FAKE_CREDENTIALS


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcesrc_with_list_query_and_kwargs():
    conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    queries = [
        "SELECT Name FROM Contact",
        "SELECT Id FROM Account",
    ]
    src = td.SalesforceSrc(
        conn=conn,
        query=queries,
        include_deleted=True,
        api_version="50.0",
    )
    assert src.query == queries
    assert src.include_deleted is True
    assert src.api_version == "50.0"


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcesrc_conn_setter_rebuilds_inner_source():
    first_conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    src = td.SalesforceSrc(
        conn=first_conn,
        query="SELECT Name FROM Contact",
    )
    first_inner = src._salesforce_source
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
    # Reassigning conn must rebuild the inner SalesforceSource so downstream
    # calls see the new credentials.
    assert src._salesforce_source is not first_inner
    assert src._salesforce_source.credentials is new_credentials


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcesrc_conn_setter_rejects_bad_type():
    src = td.SalesforceSrc(
        conn=td.SalesforceConn(credentials=FAKE_CREDENTIALS),
        query="SELECT Name FROM Contact",
    )
    with pytest.raises(ParameterValidationError):
        # noinspection PyTypeChecker
        src.conn = "not_a_conn"


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcesrc_initial_last_modified_missing_raises():
    conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    with pytest.raises(ValueError):
        td.SalesforceSrc(
            conn=conn,
            query=(
                f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact"
                f" WHERE {td.SalesforceSource.LAST_MODIFIED_COLUMN} >"
                f" {td.SalesforceSource.LAST_MODIFIED_TOKEN}"
            ),
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcesrc_initial_last_modified_no_timezone_raises():
    conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    with pytest.raises(ValueError):
        td.SalesforceSrc(
            conn=conn,
            query=(
                f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact"
                f" WHERE {td.SalesforceSource.LAST_MODIFIED_COLUMN} >"
                f" {td.SalesforceSource.LAST_MODIFIED_TOKEN}"
            ),
            initial_last_modified="2098-02-05T11:27:47.000000",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_salesforcesrc_initial_values_getter_and_setter():
    conn = td.SalesforceConn(credentials=FAKE_CREDENTIALS)
    src = td.SalesforceSrc(
        conn=conn,
        query=(
            f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact"
            f" WHERE {td.SalesforceSource.LAST_MODIFIED_COLUMN} >"
            f" {td.SalesforceSource.LAST_MODIFIED_TOKEN}"
        ),
        initial_last_modified="2024-03-10T11:03:08.000+0000",
    )
    assert src.initial_values == {
        "initial_last_modified": "2024-03-10T11:03:08.000000+0000"
    }
    src.initial_values = {"initial_last_modified": "2025-01-01T00:00:00.000+0000"}
    assert src.initial_values == {
        "initial_last_modified": "2025-01-01T00:00:00.000+0000"
    }


# --- Live chunk() ---


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
def test_salesforcesrc_chunk(tmp_path, sf_config):
    date1 = "2098-02-05T11:27:47.000000+0000"
    date5 = "1934-04-16T14:10:02.000000+0000"
    conn = td.SalesforceConn(credentials=sf_config["CREDENTIALS"])

    src = td.SalesforceSrc(
        conn=conn,
        query=(
            f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact"
            f" WHERE {td.SalesforceSource.LAST_MODIFIED_COLUMN} > {date1}"
        ),
    )
    [result] = src.chunk(str(tmp_path))
    assert result is None

    src = td.SalesforceSrc(
        conn=conn,
        query=(
            f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact"
            f" WHERE {td.SalesforceSource.LAST_MODIFIED_COLUMN} > {date5}"
        ),
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
def test_input_salesforcesrc(sf_config):
    fn = copy.deepcopy(input_salesforcesrc)
    fn.input.conn = td.SalesforceConn(credentials=sf_config["CREDENTIALS"])
    fnrun = tdfnrun(fn)
    assert fnrun.status == 0
    output = pl.read_parquet(fnrun.output[0].path)
    output = clean_polars_df(output)
    # Reuse the expected output from the non-Src basic test — the query is
    # the same.
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_salesforce_basic",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_salesforcesrc_initial_values(sf_config):
    fn = copy.deepcopy(input_salesforcesrc_initial_values)
    fn.input.conn = td.SalesforceConn(credentials=sf_config["CREDENTIALS"])
    fnrun = tdfnrun(fn, output_initial_values=True)
    assert fnrun.status == 0

    # Reuse expected outputs from the non-Src initial-values test — same
    # query and initial_last_modified.
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_salesforce_initial_values",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)

    output = pl.read_parquet(fnrun.output[0].path)
    output = clean_polars_df(output)
    assert output.equals(expected_output)

    output = pl.read_parquet(fnrun.output[1].path)
    output = clean_polars_df(output)
    assert output.equals(expected_output)

    initial_values_file = fnrun.offset
    assert initial_values_file.exists()
    initial_values = pl.read_parquet(initial_values_file)
    expected_initial_values = pl.read_parquet(
        os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_input_salesforce_initial_values",
            "expected_initial_values.parquet",
        )
    )
    assert initial_values.equals(expected_initial_values)

    # Second iteration: feed the offset back in; nothing new should land and
    # no new offset file should be written.
    fn2 = copy.deepcopy(input_salesforcesrc_initial_values)
    fn2.input.conn = td.SalesforceConn(credentials=sf_config["CREDENTIALS"])
    fnrun2 = tdfnrun(
        fn2,
        input_initial_values=initial_values_file,
        require_output=False,
    )
    assert fnrun2.status == 0
    assert not fnrun2.output[0].path.exists()
    assert not fnrun2.output[1].path.exists()
    assert not fnrun2.offset.exists()
