#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os
import uuid

import numpy as np
import pandas as pd
import polars as pl
import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.transforms import YearTransform

import tabsdata as td
from test_tabsdata.fntest import tdfnrun
from tabsdata._secret import _recursively_evaluate_secret
from test_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.test_output_new_s3_catalog.example import (
    testing_tabsdata_function as output_new_s3_catalog,
)
from test_tabsdata.test_output_new_s3_catalog_append.example import (
    testing_tabsdata_function as output_new_s3_catalog_append,
)
from test_tabsdata.test_output_new_s3_catalog_partition.example import (
    NUMBER_OF_PARTITIONS,
    testing_tabsdata_function as output_new_s3_catalog_partition,
)
from test_tabsdata.test_output_new_s3_catalog_region_creds.example import (
    testing_tabsdata_function as output_new_s3_catalog_region_creds,
)
from test_tabsdata.test_output_new_s3_catalog_replace.example import (
    testing_tabsdata_function as output_new_s3_catalog_replace,
)
from test_tabsdata.test_output_new_s3_catalog_schema_strategy.example import (
    testing_tabsdata_function as output_new_s3_catalog_schema_strategy,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


pytestmark = pytest.mark.catalog


def create_bucket(bucket_name: str, region: str, client):
    create_bucket_configurations = (
        {"LocationConstraint": region}  # Change region as needed
        if region != "us-east-1"
        else {}
    )
    client.create_bucket(
        Bucket=bucket_name, CreateBucketConfiguration=create_bucket_configurations
    )


def delete_bucket(bucket_name: str, client):
    try:
        # Delete all objects in the bucket before deleting the bucket itself
        response = client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
            client.delete_objects(
                Bucket=bucket_name, Delete={"Objects": objects_to_delete}
            )

        client.delete_bucket(Bucket=bucket_name)
    except Exception as e:
        logger.error(f"Error deleting bucket {bucket_name}: {e}")


@pytest.mark.requires_internet
@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_s3_catalog(tmp_path, s3_client, s3_config):
    bucket_name = s3_config["BUCKET_NAME"]
    file_name_0 = (
        f"testing_output/test_output_s3_catalog_{uuid.uuid4()}_0.parquet"
    )
    file_name_1 = (
        f"testing_output/test_output_s3_catalog_{uuid.uuid4()}_1.parquet"
    )

    namespace = f"testing_namespace_{uuid.uuid4()}"
    table_0 = f"{namespace}.s3_catalog_0"
    table_1 = f"{namespace}.s3_catalog_1"

    s3_catalog = td.AWSGlue(
        definition=s3_config["CATALOG_DEFINITION"],
        tables=[
            table_0,
            table_1,
        ],
    )

    catalog_definition = _recursively_evaluate_secret(s3_catalog.definition)
    catalog = load_catalog(**catalog_definition)
    catalog.create_namespace(namespace)

    catalog_bucket_name = (
        f'{s3_config["CATALOG_BUCKET_NAME_PREFIX"]}-{uuid.uuid4().hex[:16]}'
    )
    create_bucket(catalog_bucket_name, s3_config["REGION"], s3_client)
    s3_catalog.auto_create_at = [
        f"s3://{catalog_bucket_name}/{namespace}/s3_catalog_0",
        f"s3://{catalog_bucket_name}/{namespace}/s3_catalog_1",
    ]

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3_catalog", "mock_table.parquet"
    )

    fnrun = tdfnrun(
        output_new_s3_catalog(
            bucket_uri=s3_config["URI"],
            path=[file_name_0, file_name_1],
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            catalog=s3_catalog,
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name_0, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3_catalog",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name_1, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        assert output.equals(expected_output)

        # Verify the catalog has the proper data
        table = catalog.load_table(table_0)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        assert output.equals(expected_output)

        table = catalog.load_table(table_1)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        assert output.equals(expected_output)

    finally:
        operations = [
            lambda: catalog.drop_table(table_0),
            lambda: catalog.drop_table(table_1),
            lambda: catalog.drop_namespace(namespace),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_0),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_1),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass

        delete_bucket(catalog_bucket_name, s3_client)


@pytest.mark.requires_internet
@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_s3_catalog_replace(tmp_path, s3_client, s3_config):
    namespace = f"testing_namespace_replace_{uuid.uuid4()}"
    table_name = f"{namespace}.s3_catalog_replace"
    bucket_name = s3_config["BUCKET_NAME"]

    s3_catalog = td.AWSGlue(
        definition=s3_config["CATALOG_DEFINITION"],
        tables=table_name,
        if_table_exists="replace",
    )

    catalog_definition = _recursively_evaluate_secret(s3_catalog.definition)
    catalog = load_catalog(**catalog_definition)
    catalog.create_namespace(namespace)

    catalog_bucket_name = (
        f'{s3_config["CATALOG_BUCKET_NAME_PREFIX"]}-{uuid.uuid4().hex[:16]}'
    )
    create_bucket(catalog_bucket_name, s3_config["REGION"], s3_client)
    s3_catalog.auto_create_at = (
        f"s3://{catalog_bucket_name}/{namespace}/s3_catalog_replace"
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_s3_catalog_replace",
        "mock_table.parquet",
    )

    files_to_delete = []
    try:
        for i in range(2):
            file_name = (
                f"testing_output/test_output_new_s3_catalog_replace_"
                f"{uuid.uuid4()}_{i}.parquet"
            )
            files_to_delete.append(file_name)

            fnrun = tdfnrun(
                output_new_s3_catalog_replace(
                    bucket_uri=s3_config["URI"],
                    path=file_name,
                    credentials=s3_config["CREDENTIALS"],
                    region=s3_config["REGION"],
                    catalog=s3_catalog,
                ),
                input_tables=mock_parquet_table,
            )
            assert fnrun.status == 0

            copy_destination = os.path.join(tmp_path, f"output_{i}.parquet")
            s3_client.download_file(bucket_name, file_name, copy_destination)
            output = pl.read_parquet(copy_destination)
            output = clean_polars_df(output)
            expected_output_file = os.path.join(
                TESTING_RESOURCES_FOLDER,
                "test_output_new_s3_catalog_replace",
                "expected_result.json",
            )
            expected_output = read_json_and_clean(expected_output_file)
            assert output.equals(expected_output)

        # Verify the catalog has the proper data (replaced, not appended)
        table = catalog.load_table(table_name)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3_catalog",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert len(output) == len(expected_output)

    finally:
        operations = [
            lambda: catalog.drop_table(table_name),
            lambda: catalog.drop_namespace(namespace),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass

        for file in files_to_delete:
            s3_client.delete_object(Bucket=bucket_name, Key=file)

        delete_bucket(catalog_bucket_name, s3_client)


@pytest.mark.requires_internet
@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_s3_catalog_append(tmp_path, s3_client, s3_config):
    namespace = f"testing_namespace_append_{uuid.uuid4()}"
    table_name = f"{namespace}.s3_catalog_append"
    bucket_name = s3_config["BUCKET_NAME"]

    s3_catalog = td.AWSGlue(
        definition=s3_config["CATALOG_DEFINITION"],
        tables=table_name,
        if_table_exists="append",
    )

    catalog_definition = _recursively_evaluate_secret(s3_catalog.definition)
    catalog = load_catalog(**catalog_definition)
    catalog.create_namespace(namespace)

    catalog_bucket_name = (
        f'{s3_config["CATALOG_BUCKET_NAME_PREFIX"]}-{uuid.uuid4().hex[:16]}'
    )
    create_bucket(catalog_bucket_name, s3_config["REGION"], s3_client)
    s3_catalog.auto_create_at = (
        f"s3://{catalog_bucket_name}/{namespace}/s3_catalog_append"
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_s3_catalog_append",
        "mock_table.parquet",
    )

    files_to_delete = []
    try:
        for i in range(2):
            file_name = (
                f"testing_output/test_output_new_s3_catalog_append_"
                f"{uuid.uuid4()}_{i}.parquet"
            )
            files_to_delete.append(file_name)

            fnrun = tdfnrun(
                output_new_s3_catalog_append(
                    bucket_uri=s3_config["URI"],
                    path=file_name,
                    credentials=s3_config["CREDENTIALS"],
                    region=s3_config["REGION"],
                    catalog=s3_catalog,
                ),
                input_tables=mock_parquet_table,
            )
            assert fnrun.status == 0

            copy_destination = os.path.join(tmp_path, f"output_{i}.parquet")
            s3_client.download_file(bucket_name, file_name, copy_destination)
            output = pl.read_parquet(copy_destination)
            output = clean_polars_df(output)
            expected_output_file = os.path.join(
                TESTING_RESOURCES_FOLDER,
                "test_output_new_s3_catalog_append",
                "expected_result.json",
            )
            expected_output = read_json_and_clean(expected_output_file)
            assert output.equals(expected_output)

        # Verify the catalog has the proper data (appended twice)
        table = catalog.load_table(table_name)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3_catalog_append",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert len(output) == 2 * len(expected_output)

    finally:
        operations = [
            lambda: catalog.drop_table(table_name),
            lambda: catalog.drop_namespace(namespace),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass

        for file in files_to_delete:
            s3_client.delete_object(Bucket=bucket_name, Key=file)

        delete_bucket(catalog_bucket_name, s3_client)


@pytest.mark.requires_internet
@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_s3_catalog_no_auto_create_at_fails(s3_client, s3_config):
    bucket_name = s3_config["BUCKET_NAME"]
    file_name_0 = (
        f"testing_output/test_output_s3_catalog_no_autocreate_"
        f"{uuid.uuid4()}_0.parquet"
    )
    file_name_1 = (
        f"testing_output/test_output_s3_catalog_no_autocreate_"
        f"{uuid.uuid4()}_1.parquet"
    )

    namespace = f"testing_namespace_no_auto_create_at_fails_{uuid.uuid4()}"
    table_0 = f"{namespace}.s3_catalog_no_autocreate_0"
    table_1 = f"{namespace}.s3_catalog_no_autocreate_1"

    s3_catalog = td.AWSGlue(
        definition=s3_config["CATALOG_DEFINITION"],
        tables=[table_0, table_1],
        auto_create_at=None,
    )

    catalog_definition = _recursively_evaluate_secret(s3_catalog.definition)
    catalog = load_catalog(**catalog_definition)
    catalog.create_namespace(namespace)

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3_catalog", "mock_table.parquet"
    )

    try:
        fnrun = tdfnrun(
            output_new_s3_catalog(
                bucket_uri=s3_config["URI"],
                path=[file_name_0, file_name_1],
                credentials=s3_config["CREDENTIALS"],
                region=s3_config["REGION"],
                catalog=s3_catalog,
            ),
            input_tables=mock_parquet_table,
            require_success=False,
        )
        assert fnrun.status != 0
    finally:
        operations = [
            lambda: catalog.drop_table(table_0),
            lambda: catalog.drop_table(table_1),
            lambda: catalog.drop_namespace(namespace),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_0),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_1),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass


@pytest.mark.requires_internet
@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_s3_catalog_schema_update(tmp_path, s3_client, s3_config):
    namespace = f"testing_namespace_schema_update_{uuid.uuid4()}"
    table_name = f"{namespace}.s3_catalog_schema_update"
    bucket_name = s3_config["BUCKET_NAME"]

    s3_catalog = td.AWSGlue(
        definition=s3_config["CATALOG_DEFINITION"],
        tables=table_name,
        schema_strategy="update",
    )

    catalog_definition = _recursively_evaluate_secret(s3_catalog.definition)
    catalog = load_catalog(**catalog_definition)
    catalog.create_namespace(namespace)

    catalog_bucket_name = (
        f'{s3_config["CATALOG_BUCKET_NAME_PREFIX"]}-{uuid.uuid4().hex[:16]}'
    )
    create_bucket(catalog_bucket_name, s3_config["REGION"], s3_client)
    table_location = f"s3://{catalog_bucket_name}/{namespace}/s3_catalog_schema_update"
    s3_catalog.auto_create_at = table_location

    # Pre-create a table with a different schema to test schema evolution
    example_df = pl.DataFrame({"first_column": [1, 2, 3], "second_column": [4, 5, 6]})
    pyarrow_table = example_df.to_arrow()
    catalog.create_table(
        identifier=table_name,
        location=table_location,
        schema=pyarrow_table.schema,
    )

    file_name = (
        f"testing_output/test_output_s3_catalog_schema_update_"
        f"{uuid.uuid4()}.parquet"
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_s3_catalog_schema_strategy",
        "mock_table.parquet",
    )

    try:
        fnrun = tdfnrun(
            output_new_s3_catalog_schema_strategy(
                bucket_uri=s3_config["URI"],
                path=file_name,
                credentials=s3_config["CREDENTIALS"],
                region=s3_config["REGION"],
                catalog=s3_catalog,
            ),
            input_tables=mock_parquet_table,
        )
        assert fnrun.status == 0

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3_catalog_schema_strategy",
            "expected_result_bucket.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        # Verify the catalog has the proper data
        table = catalog.load_table(table_name)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3_catalog_schema_strategy",
            "expected_result_catalog.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        operations = [
            lambda: catalog.drop_table(table_name),
            lambda: catalog.drop_namespace(namespace),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass

        delete_bucket(catalog_bucket_name, s3_client)


@pytest.mark.requires_internet
@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_s3_catalog_schema_strict(s3_client, s3_config):
    namespace = f"testing_namespace_schema_strict_{uuid.uuid4()}"
    table_name = f"{namespace}.s3_catalog_schema_strict"
    bucket_name = s3_config["BUCKET_NAME"]

    s3_catalog = td.AWSGlue(
        definition=s3_config["CATALOG_DEFINITION"],
        tables=table_name,
        schema_strategy="strict",
    )

    catalog_definition = _recursively_evaluate_secret(s3_catalog.definition)
    catalog = load_catalog(**catalog_definition)
    catalog.create_namespace(namespace)

    catalog_bucket_name = (
        f'{s3_config["CATALOG_BUCKET_NAME_PREFIX"]}-{uuid.uuid4().hex[:16]}'
    )
    create_bucket(catalog_bucket_name, s3_config["REGION"], s3_client)
    table_location = f"s3://{catalog_bucket_name}/{namespace}/s3_catalog_schema_strict"
    s3_catalog.auto_create_at = table_location

    # Pre-create a table with a different schema — strict mode should reject the write
    example_df = pl.DataFrame({"first_column": [1, 2, 3], "second_column": [4, 5, 6]})
    pyarrow_table = example_df.to_arrow()
    catalog.create_table(
        identifier=table_name,
        location=table_location,
        schema=pyarrow_table.schema,
    )

    file_name = (
        f"testing_output/test_output_s3_catalog_schema_strict_"
        f"{uuid.uuid4()}.parquet"
    )

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_s3_catalog_schema_strategy",
        "mock_table.parquet",
    )

    try:
        fnrun = tdfnrun(
            output_new_s3_catalog_schema_strategy(
                bucket_uri=s3_config["URI"],
                path=file_name,
                credentials=s3_config["CREDENTIALS"],
                region=s3_config["REGION"],
                catalog=s3_catalog,
            ),
            input_tables=mock_parquet_table,
            require_success=False,
        )
        assert fnrun.status != 0
    finally:
        operations = [
            lambda: catalog.drop_table(table_name),
            lambda: catalog.drop_namespace(namespace),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass

        delete_bucket(catalog_bucket_name, s3_client)


@pytest.mark.requires_internet
@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_s3_catalog_partition(tmp_path, s3_client, s3_config):
    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_s3_catalog_partition_"
        f"{uuid.uuid4()}_$FRAGMENT_IDX.parquet"
    )

    namespace = f"testing_namespace_partition_{uuid.uuid4()}"
    table_name = f"{namespace}.s3_catalog_partition"

    s3_catalog = td.AWSGlue(
        definition=s3_config["CATALOG_DEFINITION"],
        tables=table_name,
    )

    catalog_definition = _recursively_evaluate_secret(s3_catalog.definition)
    catalog = load_catalog(**catalog_definition)
    catalog.create_namespace(namespace)

    catalog_bucket_name = (
        f'{s3_config["CATALOG_BUCKET_NAME_PREFIX"]}-{uuid.uuid4().hex[:16]}'
    )
    create_bucket(catalog_bucket_name, s3_config["REGION"], s3_client)
    table_location = f"s3://{catalog_bucket_name}/{namespace}/s3_catalog_partition"
    s3_catalog.auto_create_at = table_location

    # Pre-create a partitioned table
    date_range = pd.date_range(start="1900-01-01", end=pd.Timestamp.now(), freq="D")
    random_dates = np.random.choice(date_range, size=10)
    random_numbers = np.random.rand(10)
    example_df = pl.DataFrame(
        {"timestamp": random_dates, "random_number": random_numbers}
    )
    example_df = example_df.with_columns(
        example_df["timestamp"].cast(pl.Datetime("us"))
    )
    pyarrow_table = example_df.to_arrow()
    iceberg_table = catalog.create_table(
        identifier=table_name,
        location=table_location,
        schema=pyarrow_table.schema,
    )
    with iceberg_table.update_spec() as update:
        update.add_field("timestamp", YearTransform())

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_s3_catalog_partition",
        "mock_table.parquet",
    )

    try:
        fnrun = tdfnrun(
            output_new_s3_catalog_partition(
                bucket_uri=s3_config["URI"],
                path=file_name,
                credentials=s3_config["CREDENTIALS"],
                region=s3_config["REGION"],
                catalog=s3_catalog,
            ),
            input_tables=mock_parquet_table,
        )
        assert fnrun.status == 0

        for i in range(NUMBER_OF_PARTITIONS):
            copy_destination = os.path.join(tmp_path, f"output_{i}.parquet")
            s3_client.download_file(
                bucket_name,
                file_name.replace("$FRAGMENT_IDX", str(i)),
                copy_destination,
            )
            output = pl.read_parquet(copy_destination)
            assert len(output) == 1

        # Verify the catalog has the proper data
        table = catalog.load_table(table_name)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        assert len(output) == NUMBER_OF_PARTITIONS

    finally:
        operations = [
            lambda: catalog.drop_table(table_name),
            lambda: catalog.drop_namespace(namespace),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass

        for i in range(NUMBER_OF_PARTITIONS):
            try:
                s3_client.delete_object(
                    Bucket=bucket_name, Key=file_name.replace("$FRAGMENT_IDX", str(i))
                )
            except:
                pass

        delete_bucket(catalog_bucket_name, s3_client)


@pytest.mark.requires_internet
@pytest.mark.s3
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_s3_catalog_region_creds(tmp_path, s3_client, s3_config):
    bucket_name = s3_config["BUCKET_NAME"]
    file_name_0 = (
        f"testing_output/test_output_s3_catalog_region_creds_"
        f"{uuid.uuid4()}_0.parquet"
    )
    file_name_1 = (
        f"testing_output/test_output_s3_catalog_region_creds_"
        f"{uuid.uuid4()}_1.parquet"
    )

    namespace = f"testing_namespace_region_creds_{uuid.uuid4()}"
    table_0 = f"{namespace}.s3_catalog_region_creds_0"
    table_1 = f"{namespace}.s3_catalog_region_creds_1"

    credentials = td.S3AccessKeyCredentials(
        td.EnvironmentSecret(s3_config["ACCESS_KEY_ENV"]),
        td.EnvironmentSecret(s3_config["SECRET_KEY_ENV"]),
    )

    s3_catalog = td.AWSGlue(
        definition={"name": "default", "type": "glue"},
        s3_region=s3_config["REGION"],
        s3_credentials=credentials,
        tables=[table_0, table_1],
    )

    catalog_definition = _recursively_evaluate_secret(s3_catalog.definition)
    catalog = load_catalog(**catalog_definition)
    catalog.create_namespace(namespace)

    catalog_bucket_name = (
        f'{s3_config["CATALOG_BUCKET_NAME_PREFIX"]}-{uuid.uuid4().hex[:16]}'
    )
    create_bucket(catalog_bucket_name, s3_config["REGION"], s3_client)
    s3_catalog.auto_create_at = [
        f"s3://{catalog_bucket_name}/{namespace}/s3_catalog_region_creds_0",
        f"s3://{catalog_bucket_name}/{namespace}/s3_catalog_region_creds_1",
    ]
    s3_catalog.s3_credentials = s3_config["CREDENTIALS"]

    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_new_s3_catalog_region_creds",
        "mock_table.parquet",
    )

    try:
        fnrun = tdfnrun(
            output_new_s3_catalog_region_creds(
                bucket_uri=s3_config["URI"],
                path=[file_name_0, file_name_1],
                credentials=s3_config["CREDENTIALS"],
                region=s3_config["REGION"],
                catalog=s3_catalog,
            ),
            input_tables=mock_parquet_table,
        )
        assert fnrun.status == 0

        copy_destination = os.path.join(tmp_path, "output_0.parquet")
        s3_client.download_file(bucket_name, file_name_0, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3_catalog_region_creds",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        copy_destination = os.path.join(tmp_path, "output_1.parquet")
        s3_client.download_file(bucket_name, file_name_1, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        assert output.equals(expected_output)

        # Verify the catalog has the proper data
        table = catalog.load_table(table_0)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        assert output.equals(expected_output)

        table = catalog.load_table(table_1)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        assert output.equals(expected_output)

    finally:
        operations = [
            lambda: catalog.drop_table(table_0),
            lambda: catalog.drop_table(table_1),
            lambda: catalog.drop_namespace(namespace),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_0),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_1),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass

        delete_bucket(catalog_bucket_name, s3_client)
