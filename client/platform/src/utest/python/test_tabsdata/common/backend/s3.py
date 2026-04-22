#
# Copyright 2026 Tabs Data Inc.
#

"""AWS S3 fixtures and utilities."""

import os

import boto3
import pytest

import tabsdata as td

TESTING_AWS_ACCESS_KEY_ID = "TRANSPORTER_AWS_ACCESS_KEY_ID"
TESTING_AWS_SECRET_ACCESS_KEY = "TRANSPORTER_AWS_SECRET_ACCESS_KEY"


@pytest.fixture(scope="session")
def s3_config():

    config = {
        "ACCESS_KEY_ENV": "S30__S3_ACCESS_KEY",
        "SECRET_KEY_ENV": "S30__S3_SECRET_KEY",
        "URI_ENV": "S30__S3_URI",
        "REGION_ENV": "S30__S3_REGION",
    }
    access_key_id = os.environ.get(config["ACCESS_KEY_ENV"])
    secret_key = os.environ.get(config["SECRET_KEY_ENV"])
    if not access_key_id or not secret_key:
        raise Exception(
            f"The environment variables {config['ACCESS_KEY_ENV']} and/or "
            f"{config['SECRET_KEY_ENV']} are not set. Unable to run tests "
            "using the 's3_config' fixture."
        )
    credentials = td.S3AccessKeyCredentials(
        td.EnvironmentSecret(config["ACCESS_KEY_ENV"]),
        td.EnvironmentSecret(config["SECRET_KEY_ENV"]),
    )
    config["ACCESS_KEY"] = access_key_id
    config["SECRET_KEY"] = secret_key
    config["CREDENTIALS"] = credentials
    region = os.environ.get(config["REGION_ENV"])
    if not region:
        raise Exception(
            f"The environment variable {config['REGION_ENV']} is not set. Unable to "
            "run tests using the 's3_config' fixture."
        )
    config["REGION"] = region

    uri = os.environ.get(config["URI_ENV"])
    if not uri:
        raise Exception(
            f"The environment variable {config['URI_ENV']} is not set. Unable to run "
            "tests using the 's3_config' fixture."
        )
    config["URI"] = uri
    bucket_name = uri.replace("s3://", "").split("/", 1)[0]
    config["BUCKET_NAME"] = bucket_name

    catalog_definition = {
        "name": "default",
        "type": "glue",
        "client.access-key-id": config["ACCESS_KEY"],
        "client.secret-access-key": config["SECRET_KEY"],
        "client.region": config["REGION"],
    }
    config["CATALOG_DEFINITION"] = catalog_definition

    config["CATALOG_BUCKET_NAME_PREFIX"] = (
        f'tabsdata-{config["REGION"]}-catalog-metadata'
    )

    yield config


@pytest.fixture(scope="session")
def s3_client(s3_config):
    # noinspection PyPackageRequirements

    session = boto3.Session(
        aws_access_key_id=os.environ.get(s3_config["ACCESS_KEY_ENV"]),
        aws_secret_access_key=os.environ.get(s3_config["SECRET_KEY_ENV"]),
        region_name=s3_config["REGION"],
    )
    client = session.client("s3")

    # noinspection PyBroadException
    try:
        bucket_name = s3_config["BUCKET_NAME"]

        create_bucket_configurations = (
            {"LocationConstraint": s3_config["REGION"]}  # Change region as needed
            if s3_config["REGION"] != "us-east-1"
            else {}
        )

        client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration=create_bucket_configurations
        )
    except:
        pass

    yield client
