#
# Copyright 2024 Tabs Data Inc.
#

import copy
from urllib.parse import urlparse

import pytest
from tests_tabsdata.conftest import FORMAT_TYPE_TO_CONFIG

from tabsdata import CSVFormat, ParquetFormat
from tabsdata._credentials import S3AccessKeyCredentials, UserPasswordCredentials
from tabsdata._io.outputs.file_outputs import (
    FRAGMENT_INDEX_PLACEHOLDER,
    AWSGlue,
    S3Destination,
)
from tabsdata._io.plugin import DestinationPlugin
from tabsdata._secret import DirectSecret
from tabsdata.exceptions import (
    ErrorCode,
    FormatConfigurationError,
    OutputConfigurationError,
)

TEST_ACCESS_KEY_ID = "test_access_key_id"
TEST_SECRET_ACCESS_KEY = "test_secret_access_key"
S3_CREDENTIALS = S3AccessKeyCredentials(
    aws_access_key_id=TEST_ACCESS_KEY_ID,
    aws_secret_access_key=TEST_SECRET_ACCESS_KEY,
)


def test_all_correct_implicit_format():
    uri = "s3://path/to/data/data.csv"
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, S3Destination)
    assert isinstance(output, DestinationPlugin)
    assert output.credentials == S3_CREDENTIALS
    assert output.__repr__()


def test_all_correct_uri_list():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, S3Destination)
    assert isinstance(output, DestinationPlugin)
    assert output.credentials == S3_CREDENTIALS
    assert output.__repr__()


def test_uri_list_update_to_string():
    uri = [
        "s3://path/to/data/invoice-headers.csv",
        "s3://path/to/data/invoice-items-*.csv",
    ]
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.uri == uri
    assert output._uri_list == uri
    output = S3Destination("s3://path/to/data/invoice-headers.csv", S3_CREDENTIALS)
    assert output._uri_list == ["s3://path/to/data/invoice-headers.csv"]


def test_parsed_uri_list():
    uri = [
        "s3://path/to/data/invoice-headers.csv",
        "s3://path/to/data/invoice-items-*.csv",
    ]
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.uri == uri
    assert output._uri_list == uri
    assert output._parsed_uri_list == [urlparse(uri[0]), urlparse(uri[1])]
    uri = "s3://path/to/data/invoice-headers.csv"
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.uri == uri
    assert output._uri_list == [uri]
    assert output._parsed_uri_list == [urlparse(uri)]


def test_update_uri():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.uri == uri
    assert output._uri_list == uri
    assert output._parsed_uri_list == [urlparse(uri[0]), urlparse(uri[1])]
    uri2 = ["s3://path/to/data/data.csv", "s3://path/to/data/data3.csv"]
    output.uri = uri2
    assert output.uri == uri2
    assert output._uri_list == uri2
    assert output._parsed_uri_list == [urlparse(uri2[0]), urlparse(uri2[1])]


def test_update_uri_implicit_format():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    uri2 = ["s3://path/to/data/data.parquet", "s3://path/to/data/data3.parquet"]
    output.uri = uri2
    assert output.uri == uri2
    assert isinstance(output.format, ParquetFormat)


def test_update_uri_explicit_format():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    output = S3Destination(uri, S3_CREDENTIALS, format="csv")
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    uri2 = ["s3://path/to/data/data.parquet", "s3://path/to/data/data3.parquet"]
    output.uri = uri2
    assert output.uri == uri2
    assert isinstance(output.format, CSVFormat)


def test_uri_path_mixed_format():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    assert output._format is None
    uri2 = ["s3://path/to/data/data.parquet", "s3://path/to/data/data3.parquet"]
    output.uri = uri2
    assert output.uri == uri2
    assert isinstance(output.format, ParquetFormat)
    assert output._format is None
    output.format = CSVFormat()
    assert output.format == CSVFormat()
    assert output._format == CSVFormat()
    output.uri = uri2
    assert output.uri == uri2
    assert isinstance(output.format, CSVFormat)
    assert output._format == CSVFormat()
    output.format = ParquetFormat()
    assert isinstance(output.format, ParquetFormat)
    assert output._format == ParquetFormat()


def test_update_format():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.format == CSVFormat()
    format = CSVFormat(separator=";", input_has_header=False)
    output.format = format
    assert output.format == format


def test_update_credentials():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.credentials == S3_CREDENTIALS
    credentials = S3AccessKeyCredentials(
        aws_access_key_id="new_access_key_id",
        aws_secret_access_key="new_secret_access_key",
    )
    output.credentials = credentials
    assert output.credentials == credentials


def test_wrong_type_credentials_raises_error():
    uri = ["s3://path/to/data/data.csv", "s3://path/to/data/data2.csv"]
    credentials = UserPasswordCredentials("username", "password")
    with pytest.raises(OutputConfigurationError) as e:
        S3Destination(uri, credentials)
    assert e.value.error_code == ErrorCode.OCE19


def test_all_correct_explicit_format():
    uri = "s3://path/to/data/data"
    format = "csv"
    output = S3Destination(uri, S3_CREDENTIALS, format=format)
    assert output.uri == uri
    assert isinstance(output.format, CSVFormat)
    assert isinstance(output, S3Destination)
    assert isinstance(output, DestinationPlugin)


def test_wrong_scheme_raises_value_error():
    uri = "wrongscheme://path/to/data/data.csv"
    with pytest.raises(OutputConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.OCE12


def test_empty_scheme_raises_value_error():
    uri = "path/to/data/data.csv"
    with pytest.raises(OutputConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.OCE12


def test_list_of_integers_raises_exception():
    uri = [1, 2, "hi"]
    with pytest.raises(OutputConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.OCE17


def test_uri_list():
    uri = [
        "s3://path/to/data/invoice-headers.csv",
        "s3://path/to/data/invoice-items-*.csv",
    ]
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.uri == uri


def test_uri_wrong_type_raises_type_error():
    uri = 42
    with pytest.raises(OutputConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.OCE17


def test_format_from_uri_list():
    uri = [
        "s3://path/to/data/invoice-headers.csv",
        "s3://path/to/data/invoice-items-*.csv",
    ]
    output = S3Destination(uri, S3_CREDENTIALS)
    assert isinstance(output.format, CSVFormat)


def test_no_implicit_format_raises_value_error():
    uri = "s3://path/to/data/data"
    with pytest.raises(FormatConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE6


def test_correct_format_object():
    uri = "s3://path/to/data/data"
    format = CSVFormat(separator=".", input_has_header=False)
    expected_format = copy.deepcopy(FORMAT_TYPE_TO_CONFIG["csv"])
    expected_format["separator"] = "."
    expected_format["input_has_header"] = False

    output = S3Destination(uri, S3_CREDENTIALS, format=format)
    assert output.format._to_dict()[CSVFormat.IDENTIFIER] == expected_format
    assert isinstance(output, S3Destination)
    assert isinstance(output, DestinationPlugin)


def test_incorrect_data_format_raises_value_error():
    uri = "s3://path/to/data/data.wrongformat"
    with pytest.raises(FormatConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_implicit_format_raises_value_error():
    uri = "s3://path/to/data/data.wrong"
    with pytest.raises(FormatConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_explicit_format_raises_value_error():
    uri = "s3://path/to/data/data.csv"
    format = "wrong"
    with pytest.raises(FormatConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_empty_format():
    uri = "s3://path/to/data/data"
    format = ""
    with pytest.raises(FormatConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE4


def test_wrong_type_format_raises_type_error():
    uri = "s3://path/to/data/data.csv"
    format = 42
    with pytest.raises(FormatConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS, format=format)
    assert e.value.error_code == ErrorCode.FOCE5


def test_region():
    uri = "s3://path/to/data/data"
    format = "csv"
    region = "us-west-2"
    output = S3Destination(uri, S3_CREDENTIALS, format=format, region=region)
    assert output.region == region
    assert isinstance(output, S3Destination)
    assert isinstance(output, DestinationPlugin)


def test_region_wrong_type_raises_error():
    uri = "s3://uri/to/data/data.csv"
    region = 42
    with pytest.raises(OutputConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS, region=region)
    assert e.value.error_code == ErrorCode.OCE18


def test_allow_fragments():
    uri = f"s3://path/to/data/data_{FRAGMENT_INDEX_PLACEHOLDER}.csv"
    output = S3Destination(uri, S3_CREDENTIALS)
    assert output.uri == uri
    assert output.allow_fragments


def test_correct_catalog_implicit_format():
    catalog = AWSGlue(
        definition={
            "name": "default",
            "uri": "sqlite:////tmp/uri/pyiceberg_catalog.db",
            "warehouse": "file:///tmp/uri",
        },
        tables=["output1", "output2"],
    )
    uri = "s3://uri/to/data/data.parquet"
    output = S3Destination(uri, S3_CREDENTIALS, catalog=catalog)
    assert output.catalog == catalog


def test_correct_catalog_explicit_format():
    catalog = AWSGlue(
        definition={
            "name": "default",
            "uri": "sqlite:////tmp/uri/pyiceberg_catalog.db",
            "warehouse": "file:///tmp/uri",
        },
        tables=["output1", "output2"],
    )
    uri = "s3://uri/to/data/data"
    output = S3Destination(uri, S3_CREDENTIALS, catalog=catalog, format=ParquetFormat())
    assert output.catalog == catalog


def test_wrong_format_fails():
    catalog = AWSGlue(
        definition={
            "name": "default",
            "uri": "sqlite:////tmp/uri/pyiceberg_catalog.db",
            "warehouse": "file:///tmp/uri",
        },
        tables=["output1", "output2"],
    )
    uri = "s3://uri/to/data/data"
    output = S3Destination(uri, S3_CREDENTIALS, catalog=catalog, format=ParquetFormat())
    assert output.catalog == catalog
    with pytest.raises(OutputConfigurationError) as e:
        output.format = CSVFormat()
    assert e.value.error_code == ErrorCode.OCE37

    with pytest.raises(OutputConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS, catalog=catalog, format=CSVFormat())
    assert e.value.error_code == ErrorCode.OCE37

    output = S3Destination("s3://uri/to/data/data.csv", S3_CREDENTIALS)
    with pytest.raises(OutputConfigurationError) as e:
        output.catalog = catalog
    assert e.value.error_code == ErrorCode.OCE37


def test_catalog_wrong_type_raises_exception():
    uri = "s3://uri/to/data/data.csv"
    catalog = 42
    with pytest.raises(OutputConfigurationError) as e:
        S3Destination(uri, S3_CREDENTIALS, catalog=catalog)
    assert e.value.error_code == ErrorCode.OCE34
