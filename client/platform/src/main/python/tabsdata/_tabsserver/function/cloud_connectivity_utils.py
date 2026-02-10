#
# Copyright 2024 Tabs Data Inc.
#

import logging
import os

from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    AzureCredentials,
    GCPCredentials,
    GCPServiceAccountKeyCredentials,
    S3AccessKeyCredentials,
    S3Credentials,
)

logger = logging.getLogger(__name__)

SERVER_SIDE_AWS_ACCESS_KEY_ID = "LOCATION_AWS_ACCESS_KEY_ID"
SERVER_SIDE_AWS_REGION = "LOCATION_AWS_REGION"
SERVER_SIDE_AWS_SECRET_ACCESS_KEY = "LOCATION_AWS_SECRET_ACCESS_KEY"
SERVER_SIDE_AZURE_ACCOUNT_NAME = "LOCATION_ACCOUNT_NAME"
SERVER_SIDE_AZURE_ACCOUNT_KEY = "LOCATION_ACCOUNT_KEY"
SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON = "LOCATION_GCS_SERVICE_ACCOUNT_JSON"


def obtain_and_set_azure_credentials(credentials: AzureCredentials):
    """Given the AzureCredentials object, obtain the secret values and set them as
    required for the server-side file-importer binary to use them."""
    logger.info("Setting Azure credentials.")
    if isinstance(credentials, AzureAccountKeyCredentials):
        os.environ[SERVER_SIDE_AZURE_ACCOUNT_NAME] = (
            credentials.account_name.secret_value
        )
        os.environ[SERVER_SIDE_AZURE_ACCOUNT_KEY] = credentials.account_key.secret_value
        logger.info("Azure credentials set successfully.")
    else:
        logger.error(
            f"Invalid credentials type for Azure: {type(credentials)}. No "
            "data imported."
        )
        raise TypeError(
            f"Invalid credentials type for Azure: {type(credentials)}. No data"
            " imported."
        )


def obtain_and_set_gcp_credentials(credentials: GCPCredentials):
    logger.info("Setting GCP credentials.")
    if isinstance(credentials, GCPServiceAccountKeyCredentials):
        os.environ[SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON] = (
            credentials.service_account_key.secret_value
        )
        logger.info("GCP credentials set successfully.")
    else:
        logger.error(
            f"Invalid credentials type for GCP: {type(credentials)}. No data imported."
        )
        raise TypeError(
            f"Invalid credentials type for GCP: {type(credentials)}. No data imported."
        )


def obtain_and_set_s3_credentials(credentials: S3Credentials):
    """Given the S3Credentials object, obtain the secret values and set them as
    required for the server-side file-importer binary to use them."""
    logger.info("Setting AWS S3 credentials.")
    if isinstance(credentials, S3AccessKeyCredentials):
        os.environ[SERVER_SIDE_AWS_ACCESS_KEY_ID] = (
            credentials.aws_access_key_id.secret_value
        )
        os.environ[SERVER_SIDE_AWS_SECRET_ACCESS_KEY] = (
            credentials.aws_secret_access_key.secret_value
        )
        logger.info("AWS S3 credentials set successfully.")
    else:
        logger.error(
            f"Invalid credentials type for AWS S3: {type(credentials)}. No "
            "data imported."
        )
        raise TypeError(
            f"Invalid credentials type for AWS S3: {type(credentials)}. No data"
            " imported."
        )


def set_s3_region(region: str | None):
    """Given the region, set it as required for the server-side file-importer binary
    to use it."""
    logger.info("Setting AWS region.")
    if region:
        os.environ[SERVER_SIDE_AWS_REGION] = region
        logger.info(f"Set AWS region to: {region}")
    else:
        logger.warning("No AWS region provided. Using default AWS region.")
