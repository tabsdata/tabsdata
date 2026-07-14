#
# Copyright 2026 Tabs Data Inc.
#

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3",
#     "azure-storage-blob",
#     "google-cloud-storage",
#     "google-cloud-bigquery",
#     "snowflake-connector-python",
#     "simple-salesforce",
#     "databricks-sdk",
#     "openai",
#     "requests",
#     "rich",
# ]
# ///

import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Callable

from rich import box
from rich.console import Console
from rich.table import Table

RENDER_WIDTH = 1024


@dataclass
class Group:
    name: str
    required: list[str]
    check: Callable[[dict[str, str]], None]


def env(*names: str) -> dict[str, str]:
    return {name: os.environ.get(name, "") for name in names}


def summarize(detail: str, limit: int = 80) -> str:
    concise = detail.splitlines()[0] if detail else ""
    if len(concise) > limit:
        concise = concise[: limit - 1].rstrip() + "…"
    return concise


def check_aws(access_key: str, secret_key: str, region: str, uri: str) -> None:
    import boto3

    bucket = uri.replace("s3://", "", 1).split("/", 1)[0]
    client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region or "us-east-1",
    )
    client.head_bucket(Bucket=bucket)


def check_azure(account_name: str, account_key: str, container: str) -> None:
    from azure.storage.blob import BlobServiceClient

    service = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    )
    container_client = service.get_container_client(container)
    container_client.get_container_properties()


def check_gcs_hmac(access_key: str, secret_key: str, uri: str) -> None:
    import boto3

    bucket = uri.replace("gs://", "", 1).replace("gcs://", "", 1).split("/", 1)[0]
    client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url="https://storage.googleapis.com",
        region_name="auto",
    )
    client.list_objects_v2(Bucket=bucket, MaxKeys=1)


def check_gcs_service_account(service_account_key: str, bucket: str) -> None:
    from google.cloud import storage
    from google.oauth2 import service_account

    credentials = service_account.Credentials.from_service_account_info(
        json.loads(service_account_key)
    )
    client = storage.Client(credentials=credentials, project=credentials.project_id)
    next(client.list_blobs(bucket, max_results=1).pages)


def check_bigquery(service_account_key: str, project: str) -> None:
    from google.cloud import bigquery
    from google.oauth2 import service_account

    credentials = service_account.Credentials.from_service_account_info(
        json.loads(service_account_key)
    )
    client = bigquery.Client(credentials=credentials, project=project)
    list(client.query("SELECT 1").result(timeout=30))


def check_databricks(host: str, token: str) -> None:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(host=host, token=token)
    client.current_user.me()


def check_snowflake(account: str, user: str, pat: str) -> None:
    import requests

    response = requests.post(
        f"https://{account}.snowflakecomputing.com/api/v2/statements",
        headers={
            "Authorization": f"Bearer {pat}",
            "Content-Type": "application/json",
            "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
        },
        json={"statement": "SELECT 1", "timeout": 5},
        timeout=30,
    )
    response.raise_for_status()


def check_salesforce(
    username: str, password: str, security_token: str, instance_url: str
) -> None:
    from simple_salesforce import Salesforce

    domain = "test" if "test.salesforce" in instance_url else "login"
    Salesforce(
        username=username,
        password=password,
        security_token=security_token,
        domain=domain,
    ).query("SELECT Id FROM Organization LIMIT 1")


def check_openai(api_key: str) -> None:
    from openai import OpenAI

    OpenAI(api_key=api_key).models.list()


def check_oss_index(username: str, api_key: str) -> None:
    import requests

    response = requests.post(
        "https://ossindex.sonatype.org/api/v3/component-report",
        params={"api_key": api_key},
        auth=(username, api_key),
        json={"coordinates": ["pkg:cargo/claxon@0.3.0"]},
        timeout=30,
    )
    response.raise_for_status()


GROUPS: list[Group] = [
    Group(
        "aws-s3 (S30)",
        ["S30__S3_ACCESS_KEY", "S30__S3_SECRET_KEY", "S30__S3_REGION", "S30__S3_URI"],
        lambda e: check_aws(
            e["S30__S3_ACCESS_KEY"],
            e["S30__S3_SECRET_KEY"],
            e["S30__S3_REGION"],
            e["S30__S3_URI"],
        ),
    ),
    Group(
        "aws-s3 (S31)",
        ["S31__S3_ACCESS_KEY", "S31__S3_SECRET_KEY", "S31__S3_REGION", "S31__S3_URI"],
        lambda e: check_aws(
            e["S31__S3_ACCESS_KEY"],
            e["S31__S3_SECRET_KEY"],
            e["S31__S3_REGION"],
            e["S31__S3_URI"],
        ),
    ),
    Group(
        "aws-s3 (S3)",
        ["S3__ACCESS_KEY", "S3__SECRET_KEY", "S3__REGION", "S3__BUCKET"],
        lambda e: check_aws(
            e["S3__ACCESS_KEY"],
            e["S3__SECRET_KEY"],
            e["S3__REGION"],
            f"s3://{e['S3__BUCKET']}",
        ),
    ),
    Group(
        "aws-s3 (COPY_AWS)",
        [
            "COPY_AWS_AWS_ACCESS_KEY_ID",
            "COPY_AWS_AWS_SECRET_ACCESS_KEY",
            "COPY_AWS_AWS_REGION",
            "COPY_AWS_BASE_URL",
        ],
        lambda e: check_aws(
            e["COPY_AWS_AWS_ACCESS_KEY_ID"],
            e["COPY_AWS_AWS_SECRET_ACCESS_KEY"],
            e["COPY_AWS_AWS_REGION"],
            e["COPY_AWS_BASE_URL"],
        ),
    ),
    Group(
        "aws-s3 (IMPORT_AWS)",
        [
            "IMPORT_AWS_ACCESS_KEY",
            "IMPORT_AWS_SECRET_KEY",
            "IMPORT_AWS_REGION",
            "IMPORT_AWS_BASE_URL",
        ],
        lambda e: check_aws(
            e["IMPORT_AWS_ACCESS_KEY"],
            e["IMPORT_AWS_SECRET_KEY"],
            e["IMPORT_AWS_REGION"],
            e["IMPORT_AWS_BASE_URL"],
        ),
    ),
    Group(
        "aws-s3 (TRANSPORTER_AWS)",
        [
            "TRANSPORTER_AWS_ACCESS_KEY_ID",
            "TRANSPORTER_AWS_SECRET_ACCESS_KEY",
            "TRANSPORTER_AWS_REGION",
            "TRANSPORTER_AWS_URI",
        ],
        lambda e: check_aws(
            e["TRANSPORTER_AWS_ACCESS_KEY_ID"],
            e["TRANSPORTER_AWS_SECRET_ACCESS_KEY"],
            e["TRANSPORTER_AWS_REGION"],
            e["TRANSPORTER_AWS_URI"],
        ),
    ),
    Group(
        "azure-blob (AZ0)",
        ["AZ0__AZ_ACCOUNT_NAME", "AZ0__AZ_ACCOUNT_KEY", "AZ0__AZ_URI"],
        lambda e: check_azure(
            e["AZ0__AZ_ACCOUNT_NAME"],
            e["AZ0__AZ_ACCOUNT_KEY"],
            e["AZ0__AZ_URI"].split("/")[-1] or "tabsdataci",
        ),
    ),
    Group(
        "azure-blob (AZ)",
        ["AZ__ACCOUNT_NAME", "AZ__ACCOUNT_KEY", "AZ__CONTAINER"],
        lambda e: check_azure(
            e["AZ__ACCOUNT_NAME"], e["AZ__ACCOUNT_KEY"], e["AZ__CONTAINER"]
        ),
    ),
    Group(
        "azure-blob (COPY_AZURE)",
        ["COPY_AZURE_ACCOUNT_NAME", "COPY_AZURE_ACCOUNT_KEY", "COPY_AZURE_BASE_URL"],
        lambda e: check_azure(
            e["COPY_AZURE_ACCOUNT_NAME"],
            e["COPY_AZURE_ACCOUNT_KEY"],
            e["COPY_AZURE_BASE_URL"].rstrip("/").split("/")[-1],
        ),
    ),
    Group(
        "azure-blob (IMPORT_AZURE)",
        [
            "IMPORT_AZURE_ACCOUNT_NAME",
            "IMPORT_AZURE_ACCOUNT_KEY",
            "IMPORT_AZURE_BASE_URL",
        ],
        lambda e: check_azure(
            e["IMPORT_AZURE_ACCOUNT_NAME"],
            e["IMPORT_AZURE_ACCOUNT_KEY"],
            e["IMPORT_AZURE_BASE_URL"].rstrip("/").split("/")[-1],
        ),
    ),
    Group(
        "gcs-hmac (GCP0)",
        [
            "GCP0__GCP_STORAGE_ACCESS_KEY",
            "GCP0__GCP_STORAGE_SECRET_KEY",
            "GCP0__GCP_STORAGE_URI",
        ],
        lambda e: check_gcs_hmac(
            e["GCP0__GCP_STORAGE_ACCESS_KEY"],
            e["GCP0__GCP_STORAGE_SECRET_KEY"],
            e["GCP0__GCP_STORAGE_URI"],
        ),
    ),
    Group(
        "gcs-hmac (GCS)",
        ["GCS__HMAC_ACCESS_KEY", "GCS__HMAC_SECRET_KEY", "GCS__BUCKET"],
        lambda e: check_gcs_hmac(
            e["GCS__HMAC_ACCESS_KEY"],
            e["GCS__HMAC_SECRET_KEY"],
            f"gs://{e['GCS__BUCKET']}",
        ),
    ),
    Group(
        "gcs-sa (GCS)",
        ["GCS__SERVICE_ACCOUNT_KEY", "GCS__BUCKET"],
        lambda e: check_gcs_service_account(
            e["GCS__SERVICE_ACCOUNT_KEY"], e["GCS__BUCKET"]
        ),
    ),
    Group(
        "bigquery (GCP0)",
        ["GCP0__GCP_SERVICE_ACCOUNT_KEY", "GCP0__BIGQUERY_PROJECT"],
        lambda e: check_bigquery(
            e["GCP0__GCP_SERVICE_ACCOUNT_KEY"], e["GCP0__BIGQUERY_PROJECT"]
        ),
    ),
    Group(
        "bigquery (BIGQUERY)",
        ["BIGQUERY__SERVICE_ACCOUNT_KEY", "BIGQUERY__PROJECT"],
        lambda e: check_bigquery(
            e["BIGQUERY__SERVICE_ACCOUNT_KEY"], e["BIGQUERY__PROJECT"]
        ),
    ),
    Group(
        "databricks (DBX0)",
        ["DBX0__HOST", "DBX0__TOKEN"],
        lambda e: check_databricks(e["DBX0__HOST"], e["DBX0__TOKEN"]),
    ),
    Group(
        "databricks (DATABRICKS)",
        ["DATABRICKS__HOST", "DATABRICKS__TOKEN"],
        lambda e: check_databricks(e["DATABRICKS__HOST"], e["DATABRICKS__TOKEN"]),
    ),
    Group(
        "snowflake (SNW)",
        ["SNW__ACCOUNT", "SNW__USER", "SNW__PAT"],
        lambda e: check_snowflake(e["SNW__ACCOUNT"], e["SNW__USER"], e["SNW__PAT"]),
    ),
    Group(
        "snowflake (SNOWFLAKE)",
        ["SNOWFLAKE__ACCOUNT", "SNOWFLAKE__USERNAME", "SNOWFLAKE__PAT"],
        lambda e: check_snowflake(
            e["SNOWFLAKE__ACCOUNT"], e["SNOWFLAKE__USERNAME"], e["SNOWFLAKE__PAT"]
        ),
    ),
    Group(
        "salesforce (SF0)",
        [
            "SF0__USERNAME",
            "SF0__PASSWORD",
            "SF0__SECURITY_TOKEN",
            "SF0__INSTANCE_URL",
        ],
        lambda e: check_salesforce(
            e["SF0__USERNAME"],
            e["SF0__PASSWORD"],
            e["SF0__SECURITY_TOKEN"],
            e["SF0__INSTANCE_URL"],
        ),
    ),
    Group(
        "salesforce (SALESFORCE)",
        [
            "SALESFORCE__USERNAME",
            "SALESFORCE__PASSWORD",
            "SALESFORCE__TOKEN",
            "SALESFORCE__URL",
        ],
        lambda e: check_salesforce(
            e["SALESFORCE__USERNAME"],
            e["SALESFORCE__PASSWORD"],
            e["SALESFORCE__TOKEN"],
            e["SALESFORCE__URL"],
        ),
    ),
    Group(
        "openai (OPENAI_API_KEY)",
        ["OPENAI_API_KEY"],
        lambda e: check_openai(e["OPENAI_API_KEY"]),
    ),
    Group(
        "openai (OPENAI__API_KEY)",
        ["OPENAI__API_KEY"],
        lambda e: check_openai(e["OPENAI__API_KEY"]),
    ),
    Group(
        "oss-index (OSS_INDEX)",
        ["OSS_INDEX_USERNAME", "OSS_INDEX_API_KEY"],
        lambda e: check_oss_index(e["OSS_INDEX_USERNAME"], e["OSS_INDEX_API_KEY"]),
    ),
]


def main() -> int:
    results: list[tuple[str, str, str, float]] = []
    failed = False

    console = Console(width=RENDER_WIDTH)
    total = len(GROUPS)

    for index, group in enumerate(GROUPS, start=1):
        console.print(f"[cyan]▶[/cyan] [{index}/{total}] Checking {group.name} ...")
        started = time.perf_counter()
        values = env(*group.required)
        missing = [name for name, value in values.items() if not value]
        if missing:
            elapsed = time.perf_counter() - started
            results.append((
                group.name,
                "FAIL",
                f"missing: {', '.join(missing)}",
                elapsed,
            ))
            failed = True
            console.print(
                f"[red]✗[/red] {group.name} failed in {elapsed:.2f}s (missing secrets)"
            )
            continue
        try:
            group.check(values)
            elapsed = time.perf_counter() - started
            results.append((group.name, "OK", "", elapsed))
            console.print(f"[green]✓[/green] {group.name} valid in {elapsed:.2f}s")
        except Exception as error:
            elapsed = time.perf_counter() - started
            detail = str(error).splitlines()[0] if str(error) else type(error).__name__
            results.append((group.name, "FAIL", detail, elapsed))
            failed = True
            console.print(f"[red]✗[/red] {group.name} failed in {elapsed:.2f}s")

    ok_count = sum(1 for _, status, _, _ in results if status == "OK")

    table = Table(
        title=f"Credential validity check ({ok_count}/{len(results)} valid)",
        title_justify="left",
        title_style="bold magenta",
        box=box.SQUARE,
    )
    table.add_column("Result", no_wrap=True)
    table.add_column("Credential group", no_wrap=True)
    table.add_column("Time", no_wrap=True, justify="right")
    table.add_column("Detail", no_wrap=True)
    for name, status, detail, elapsed in results:
        if status == "OK":
            result = "[green]✅ OK[/green]"
        else:
            result = "[red]❌ FAIL[/red]"
        table.add_row(result, name, f"{elapsed:.2f}s", summarize(detail))
    console.print(table)

    failures = [
        (name, detail) for name, status, detail, _ in results if status == "FAIL"
    ]
    if failures:
        console.print()
        console.print("[bold red]Failure details[/bold red]")
        for name, detail in failures:
            console.print(f"[red]❌ {name}[/red]")
            console.print(f"   {detail}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
