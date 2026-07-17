#
# Copyright 2026 Tabsdata Inc.
#

"""Minimal Druid native-batch ingestion for tdx model seeding.

Druid datasources materialise only on ingestion. To seed a model into Druid
(the way SQL systems seed via DDL + INSERT), the tdx machinery stages the
generated parquet to an object store the cluster can read and submits one
``index_parallel`` task per datasource to the Overlord, polling each to
completion.

This is deliberately self-contained (stdlib ``urllib`` + basic auth, matching
the rest of the Druid system module) and does not import the Druid connector:
the connector is the code under test, so the test infrastructure must not
depend on it.
"""

from __future__ import annotations

import base64
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

from tabsdatax.itest.tdksystems.errors import DatagenError

# world (and any current model) carries no event-time column; Druid requires a
# timestamp, so ingest a synthetic constant one, mirroring the connector.
DUMMY_TIME_COLUMN = "__td_dummy_time"
DUMMY_TIME_VALUE = "2026-01-01T00:00:00.000Z"


@dataclass(frozen=True)
class S3Staging:
    """S3 location + credentials the Druid cluster uses to read the staged
    parquet. `region` is threaded into the input source so the cluster's S3
    client signs for the bucket's region (else it defaults to us-east-1 and a
    regional bucket rejects the request with AuthorizationHeaderMalformed)."""

    access_key: str
    secret_key: str
    region: str = ""


def _auth_header(user: str, password: str) -> dict[str, str]:
    if not user:
        return {}
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def _post_json(url: str, payload: dict, user: str, password: str) -> dict:
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json", **_auth_header(user, password)}
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")  # noqa: S310
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
            return json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise DatagenError(
            f"Druid POST {url} failed: HTTP {exc.code} {exc.reason}: {detail}"
        ) from exc


def _get_json(url: str, user: str, password: str) -> dict:
    req = urllib.request.Request(url, headers=_auth_header(user, password))  # noqa: S310
    with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
        return json.loads(resp.read().decode() or "{}")


def _s3_input_source(s3_uris: list[str], staging: S3Staging) -> dict:
    source: dict = {
        "type": "s3",
        "uris": s3_uris,
        "properties": {
            "accessKeyId": staging.access_key,
            "secretAccessKey": staging.secret_key,
        },
    }
    if staging.region:
        # Point the cluster's S3 client at the bucket's regional endpoint so it
        # signs for that region (the default us-east-1 signing is rejected by a
        # regional bucket with AuthorizationHeaderMalformed).
        source["endpointConfig"] = {
            "url": f"s3.{staging.region}.amazonaws.com",
            "signingRegion": staging.region,
        }
    return source


def _index_spec(datasource: str, s3_uris: list[str], staging: S3Staging) -> dict:
    return {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": datasource,
                "timestampSpec": {
                    "column": DUMMY_TIME_COLUMN,
                    "format": "iso",
                    "missingValue": DUMMY_TIME_VALUE,
                },
                "dimensionsSpec": {"dimensions": [], "useSchemaDiscovery": True},
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "ALL",
                    "queryGranularity": "none",
                    "rollup": False,
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": _s3_input_source(s3_uris, staging),
                "inputFormat": {"type": "parquet"},
                "appendToExisting": False,
            },
            "tuningConfig": {
                "type": "index_parallel",
                "partitionsSpec": {"type": "dynamic"},
            },
        },
    }


def ingest_parquet(
    *,
    overlord_url: str,
    datasource: str,
    s3_uris: list[str],
    staging: S3Staging,
    user: str,
    password: str,
    timeout: int = 300,
) -> None:
    """Submit an ``index_parallel`` task for `datasource` and poll to success."""
    submit = f"{overlord_url}/druid/indexer/v1/task"
    resp = _post_json(submit, _index_spec(datasource, s3_uris, staging), user, password)
    task_id = resp.get("task")
    if not task_id:
        raise DatagenError(f"Druid ingestion for {datasource!r} returned no task id")

    status_url = f"{overlord_url}/druid/indexer/v1/task/{task_id}/status"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = _get_json(status_url, user, password).get("status", {})
        state = status.get("status")
        if state == "SUCCESS":
            return
        if state == "FAILED":
            raise DatagenError(
                f"Druid ingestion task {task_id} for {datasource!r} FAILED: "
                f"{status.get('errorMsg', '<no error message>')}"
            )
        time.sleep(3)
    raise DatagenError(
        f"Druid ingestion task {task_id} for {datasource!r} did not finish "
        f"within {timeout}s"
    )
