#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata.exceptions import ErrorCode, OutputConfigurationError
from tabsdata.io.output import AWSGlue, build_catalog
from tabsdata.secret import DirectSecret, EnvironmentSecret

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
    assert catalog.to_dict() == {
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
    assert build_catalog(catalog.to_dict()) == catalog


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
    assert catalog.to_dict() == {
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
    assert build_catalog(catalog.to_dict()) == catalog


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
    assert catalog.to_dict() == {
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
    assert build_catalog(catalog.to_dict()) == catalog


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
    assert catalog.to_dict() == {
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
    assert build_catalog(catalog.to_dict()) == catalog


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
    assert catalog.to_dict() == {
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
    assert build_catalog(catalog.to_dict()) == catalog


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
    assert catalog.to_dict() == {
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
    assert build_catalog(catalog.to_dict()) == catalog


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
    assert catalog.to_dict() == {
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
    assert build_catalog(catalog.to_dict()) == catalog


def test_catalog_class_with_secrets_definition():
    definition = {
        "name": "default",
        "uri": "sqlite:////tmp/path/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/path",
        "secret1": DirectSecret("hello").to_dict(),
        "secret2": EnvironmentSecret("hello").to_dict(),
        "secret_list": [
            DirectSecret("hello").to_dict(),
            EnvironmentSecret("hello").to_dict(),
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
    assert catalog.to_dict() == {
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
    assert build_catalog(catalog.to_dict()) == catalog


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
    assert build_catalog(catalog.to_dict()) == catalog
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
            {AWSGlue.IDENTIFIER: catalog.to_dict()[AWSGlue.IDENTIFIER], "wrong": "key"}
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
