#
# Copyright 2024 Tabs Data Inc.
#

import pytest

import tabsdata as td
from tabsdata._io.outputs.file_outputs import AWSGlue, build_catalog
from tabsdata._secret import DirectSecret, EnvironmentSecret
from tabsdata.exceptions import (
    CredentialsConfigurationError,
    ErrorCode,
    OutputConfigurationError,
)

pytestmark = pytest.mark.catalog


def test_catalog_class():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(definition=definition, tables=tables)
    assert catalog.definition == definition
    assert catalog.tables == tables
    assert catalog._to_dict() == {
        AWSGlue.IDENTIFIER: {
            "allow_incompatible_changes": False,
            "auto_create_at": [None, None],
            "definition": definition,
            "if_table_exists": "append",
            "partitioned_table": False,
            "schema_strategy": "update",
            "tables": tables,
        }
    }
    assert build_catalog(catalog._to_dict()) == catalog


def test_catalog_class_if_table_exists():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(definition=definition, tables=tables, if_table_exists="replace")
    assert catalog.definition == definition
    assert catalog.tables == tables
    assert catalog._to_dict() == {
        AWSGlue.IDENTIFIER: {
            "allow_incompatible_changes": False,
            "auto_create_at": [None, None],
            "definition": definition,
            "if_table_exists": "replace",
            "partitioned_table": False,
            "schema_strategy": "update",
            "tables": tables,
        }
    }
    assert build_catalog(catalog._to_dict()) == catalog


def test_catalog_class_allow_incompatible_changes():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(
        definition=definition, tables=tables, allow_incompatible_changes=True
    )
    assert catalog.definition == definition
    assert catalog.tables == tables
    assert catalog._to_dict() == {
        AWSGlue.IDENTIFIER: {
            "allow_incompatible_changes": True,
            "auto_create_at": [None, None],
            "definition": definition,
            "if_table_exists": "append",
            "partitioned_table": False,
            "schema_strategy": "update",
            "tables": tables,
        }
    }
    assert build_catalog(catalog._to_dict()) == catalog


def test_catalog_class_auto_create_at():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(
        definition=definition, tables=tables, auto_create_at=["destination_1", None]
    )
    assert catalog.definition == definition
    assert catalog.tables == tables
    assert catalog._to_dict() == {
        AWSGlue.IDENTIFIER: {
            "allow_incompatible_changes": False,
            "auto_create_at": ["destination_1", None],
            "definition": definition,
            "if_table_exists": "append",
            "partitioned_table": False,
            "schema_strategy": "update",
            "tables": tables,
        }
    }
    assert build_catalog(catalog._to_dict()) == catalog


def test_catalog_class_partitioned_table():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(
        definition=definition,
        tables=tables,
        partitioned_table=True,
    )
    assert catalog.definition == definition
    assert catalog.tables == tables
    assert catalog._to_dict() == {
        AWSGlue.IDENTIFIER: {
            "allow_incompatible_changes": False,
            "auto_create_at": [None, None],
            "definition": definition,
            "if_table_exists": "append",
            "partitioned_table": True,
            "schema_strategy": "update",
            "tables": tables,
        }
    }
    assert build_catalog(catalog._to_dict()) == catalog


def test_catalog_class_schema_strategy():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(
        definition=definition,
        tables=tables,
        schema_strategy="strict",
    )
    assert catalog.definition == definition
    assert catalog.tables == tables
    assert catalog._to_dict() == {
        AWSGlue.IDENTIFIER: {
            "allow_incompatible_changes": False,
            "auto_create_at": [None, None],
            "definition": definition,
            "if_table_exists": "append",
            "partitioned_table": False,
            "schema_strategy": "strict",
            "tables": tables,
        }
    }
    assert build_catalog(catalog._to_dict()) == catalog


def test_wrong_if_table_exists():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(definition=definition, tables=tables, if_table_exists="wrong")
    assert e.value.error_code == ErrorCode.OCE33


def test_catalog_class_with_secrets():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "secret1": DirectSecret("hello"),
        "secret2": EnvironmentSecret("hello"),
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(definition=definition, tables=tables)
    assert catalog.definition == definition
    assert catalog.tables == tables
    assert catalog._to_dict() == {
        AWSGlue.IDENTIFIER: {
            "allow_incompatible_changes": False,
            "auto_create_at": [None, None],
            "definition": definition,
            "if_table_exists": "append",
            "partitioned_table": False,
            "schema_strategy": "update",
            "tables": tables,
        }
    }
    assert build_catalog(catalog._to_dict()) == catalog


def test_catalog_class_with_secrets_definition():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "secret1": DirectSecret("hello")._to_dict(),
        "secret2": EnvironmentSecret("hello")._to_dict(),
        "secret_list": [
            DirectSecret("hello")._to_dict(),
            EnvironmentSecret("hello")._to_dict(),
        ],
    }
    expected_definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "secret1": DirectSecret("hello"),
        "secret2": EnvironmentSecret("hello"),
        "secret_list": [DirectSecret("hello"), EnvironmentSecret("hello")],
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(definition=definition, tables=tables)
    assert catalog.definition == expected_definition
    assert catalog.tables == tables
    assert catalog._to_dict() == {
        AWSGlue.IDENTIFIER: {
            "allow_incompatible_changes": False,
            "auto_create_at": [None, None],
            "definition": expected_definition,
            "if_table_exists": "append",
            "partitioned_table": False,
            "schema_strategy": "update",
            "tables": tables,
        }
    }
    assert build_catalog(catalog._to_dict()) == catalog


def test_catalog_wrong_definition_type():
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(definition="wrong", tables=["output1", "output2"])
    assert e.value.error_code == ErrorCode.OCE30


def test_catalog_wrong_table_type():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(definition=definition, tables=42)
    assert e.value.error_code == ErrorCode.OCE32


def test_catalog_wrong_table_list_type():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(definition=definition, tables=["hi", 42])
    assert e.value.error_code == ErrorCode.OCE31


def test_build_catalog():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(definition=definition, tables=tables)
    assert build_catalog(catalog._to_dict()) == catalog
    assert build_catalog(catalog) == catalog


def test_build_catalog_wrong_type():
    with pytest.raises(OutputConfigurationError) as e:
        build_catalog("wrong")
    assert e.value.error_code == ErrorCode.OCE34


def test_build_catalog_wrong_dictionary_key():
    with pytest.raises(OutputConfigurationError) as e:
        build_catalog({"wrong": "key"})
    assert e.value.error_code == ErrorCode.OCE35


def test_build_catalog_wrong_dictionary_multiple_keys():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(definition=definition, tables=tables)
    with pytest.raises(OutputConfigurationError) as e:
        build_catalog(
            {AWSGlue.IDENTIFIER: catalog._to_dict()[AWSGlue.IDENTIFIER], "wrong": "key"}
        )
    assert e.value.error_code == ErrorCode.OCE35


def test_build_catalog_wrong_dictionary_value_type():
    with pytest.raises(OutputConfigurationError) as e:
        build_catalog({AWSGlue.IDENTIFIER: "wrong"})
    assert e.value.error_code == ErrorCode.OCE36


def test_catalog_partitioned_and_replace():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(
            definition=definition,
            tables=tables,
            if_table_exists="replace",
            partitioned_table=True,
        )
    assert e.value.error_code == ErrorCode.OCE39
    catalog = AWSGlue(definition=definition, tables=tables, if_table_exists="replace")
    with pytest.raises(OutputConfigurationError) as e:
        catalog.partitioned_table = True
    assert e.value.error_code == ErrorCode.OCE39
    catalog = AWSGlue(definition=definition, tables=tables, partitioned_table=True)
    with pytest.raises(OutputConfigurationError) as e:
        catalog.if_table_exists = "replace"
    assert e.value.error_code == ErrorCode.OCE39


def test_catalog_wrong_auto_create_at():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(
        definition=definition,
        tables=tables,
    )
    with pytest.raises(OutputConfigurationError) as e:
        catalog.auto_create_at = "wrong_length"
    assert e.value.error_code == ErrorCode.OCE42
    with pytest.raises(OutputConfigurationError) as e:
        catalog.auto_create_at = ["wrong_length", None, None]
    assert e.value.error_code == ErrorCode.OCE42
    with pytest.raises(OutputConfigurationError) as e:
        catalog.tables = ["wrong_length"]
    assert e.value.error_code == ErrorCode.OCE42
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(definition=definition, tables=tables, auto_create_at="wrong_length")
    assert e.value.error_code == ErrorCode.OCE42
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(
            definition=definition,
            tables=tables,
            auto_create_at=["wrong_length", None, None],
        )
    assert e.value.error_code == ErrorCode.OCE42
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(definition=definition, tables=tables, auto_create_at=42)
    assert e.value.error_code == ErrorCode.OCE44
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(definition=definition, tables=tables, auto_create_at=[42])
    assert e.value.error_code == ErrorCode.OCE43


def test_catalog_class_s3_credentials():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(
        definition=definition,
        tables=tables,
        s3_credentials=td.S3AccessKeyCredentials(
            td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"), "access_token"
        ),
    )
    definition["client.access-key-id"] = td.EnvironmentSecret(
        "TRANSPORTER_AWS_ACCESS_KEY_ID"
    )
    definition["client.secret-access-key"] = DirectSecret("access_token")
    assert catalog.definition == definition
    assert catalog.tables == tables
    assert catalog._to_dict() == {
        AWSGlue.IDENTIFIER: {
            "allow_incompatible_changes": False,
            "auto_create_at": [None, None],
            "definition": definition,
            "if_table_exists": "append",
            "partitioned_table": False,
            "schema_strategy": "update",
            "tables": tables,
        }
    }
    assert build_catalog(catalog._to_dict()) == catalog


def test_catalog_class_duplicate_access_key_id():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.access-key-id": "fake_id",
    }
    tables = ["output1", "output2"]
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(
            definition=definition,
            tables=tables,
            s3_credentials=td.S3AccessKeyCredentials(
                td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"), "access_token"
            ),
        )
    assert e.value.error_code == ErrorCode.OCE45

    definition = {}
    catalog = AWSGlue(
        definition=definition,
        tables=tables,
        s3_credentials=td.S3AccessKeyCredentials(
            td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"), "access_token"
        ),
    )
    with pytest.raises(OutputConfigurationError) as e:
        catalog.definition = {
            "name": "default",
            "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
            "warehouse": "file:///tmp/path",
            "client.access-key-id": "fake_id",
        }
    assert e.value.error_code == ErrorCode.OCE45

    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.access-key-id": "fake_id",
    }
    catalog = AWSGlue(definition=definition, tables=tables)
    with pytest.raises(OutputConfigurationError) as e:
        catalog.s3_credentials = td.S3AccessKeyCredentials(
            td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"), "access_token"
        )
    assert e.value.error_code == ErrorCode.OCE45

    definition = {}
    catalog = AWSGlue(
        definition=definition,
        tables=tables,
        s3_credentials=td.S3AccessKeyCredentials(
            td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"), "access_token"
        ),
    )
    catalog.s3_credentials = td.S3AccessKeyCredentials("new_id", "new_token")
    assert catalog.s3_credentials == td.S3AccessKeyCredentials("new_id", "new_token")

    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.access-key-id": "fake_id",
    }
    catalog = AWSGlue(definition=definition, tables=tables)
    catalog.definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.access-key-id": "new_fake_id",
    }
    assert catalog.definition == {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.access-key-id": "new_fake_id",
    }


def test_catalog_class_duplicate_secret_access_key():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.secret-access-key": "fake_id",
    }
    tables = ["output1", "output2"]
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(
            definition=definition,
            tables=tables,
            s3_credentials=td.S3AccessKeyCredentials(
                td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"), "access_token"
            ),
        )
    assert e.value.error_code == ErrorCode.OCE45

    definition = {}
    catalog = AWSGlue(
        definition=definition,
        tables=tables,
        s3_credentials=td.S3AccessKeyCredentials(
            td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"), "access_token"
        ),
    )
    with pytest.raises(OutputConfigurationError) as e:
        catalog.definition = {
            "name": "default",
            "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
            "warehouse": "file:///tmp/path",
            "client.secret-access-key": "fake_id",
        }
    assert e.value.error_code == ErrorCode.OCE45

    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.secret-access-key": "fake_id",
    }
    catalog = AWSGlue(definition=definition, tables=tables)
    with pytest.raises(OutputConfigurationError) as e:
        catalog.s3_credentials = td.S3AccessKeyCredentials(
            td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"), "access_token"
        )
    assert e.value.error_code == ErrorCode.OCE45

    definition = {}
    catalog = AWSGlue(
        definition=definition,
        tables=tables,
        s3_credentials=td.S3AccessKeyCredentials(
            td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"), "access_token"
        ),
    )
    catalog.s3_credentials = td.S3AccessKeyCredentials("new_id", "new_token")
    assert catalog.s3_credentials == td.S3AccessKeyCredentials("new_id", "new_token")

    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.secret-access-key": "fake_id",
    }
    catalog = AWSGlue(definition=definition, tables=tables)
    catalog.definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.secret-access-key": "new_fake_id",
    }
    assert catalog.definition == {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.secret-access-key": "new_fake_id",
    }


def test_s3_credentials_wrong_type():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    with pytest.raises(CredentialsConfigurationError) as e:
        AWSGlue(definition=definition, tables=tables, s3_credentials="wrong")
    assert e.value.error_code == ErrorCode.CCE3

    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(
            definition=definition,
            tables=tables,
            s3_credentials=td.UserPasswordCredentials("hi", "hello"),
        )
    assert e.value.error_code == ErrorCode.OCE47


def test_catalog_class_s3_region():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    catalog = AWSGlue(definition=definition, tables=tables, s3_region="us-east-1")
    definition["client.region"] = "us-east-1"
    assert catalog.definition == definition
    assert catalog.tables == tables
    assert catalog._to_dict() == {
        AWSGlue.IDENTIFIER: {
            "allow_incompatible_changes": False,
            "auto_create_at": [None, None],
            "definition": definition,
            "if_table_exists": "append",
            "partitioned_table": False,
            "schema_strategy": "update",
            "tables": tables,
        }
    }
    assert build_catalog(catalog._to_dict()) == catalog


def test_catalog_class_duplicate_region():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.region": "us-east-1",
    }
    tables = ["output1", "output2"]
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(
            definition=definition,
            tables=tables,
            s3_region="eu-west-1",
        )
    assert e.value.error_code == ErrorCode.OCE46

    definition = {}
    catalog = AWSGlue(
        definition=definition,
        tables=tables,
        s3_region="eu-west-1",
    )
    with pytest.raises(OutputConfigurationError) as e:
        catalog.definition = {
            "name": "default",
            "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
            "warehouse": "file:///tmp/path",
            "client.region": "us-east-1",
        }
    assert e.value.error_code == ErrorCode.OCE46

    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.region": "us-east-1",
    }
    catalog = AWSGlue(definition=definition, tables=tables)
    with pytest.raises(OutputConfigurationError) as e:
        catalog.s3_region = "eu-west-1"
    assert e.value.error_code == ErrorCode.OCE46

    definition = {}
    catalog = AWSGlue(definition=definition, tables=tables, s3_region="us-east-1")
    catalog.s3_region = "eu-west-1"
    assert catalog.s3_region == "eu-west-1"

    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.region": "us-east-1",
    }
    catalog = AWSGlue(definition=definition, tables=tables)
    catalog.definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.region": "eu-west-1",
    }
    assert catalog.definition == {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "client.region": "eu-west-1",
    }


def test_catalog_class_s3_region_wrong_type():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
    }
    tables = ["output1", "output2"]
    with pytest.raises(OutputConfigurationError) as e:
        AWSGlue(definition=definition, tables=tables, s3_region=42)
    assert e.value.error_code == ErrorCode.OCE48
