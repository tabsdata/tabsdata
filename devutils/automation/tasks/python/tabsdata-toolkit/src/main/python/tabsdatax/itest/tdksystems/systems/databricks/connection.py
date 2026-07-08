"""Databricks connection — schema ops via the SQL Statement Execution REST
API (stdlib `urllib`, no DB-API driver). Built from a resolved
`{OPTION: value}` dict — no argparse / descriptor coupling."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from ...errors import DatagenError


def _host(url_or_host: str) -> str:
    """Accept either a bare hostname (`xyz.cloud.databricks.com`) or a full
    URL (`https://xyz.cloud.databricks.com`). Returns the host without
    scheme or trailing slash."""
    s = url_or_host.strip()
    for scheme in ("https://", "http://"):
        if s.startswith(scheme):
            s = s[len(scheme) :]
            break
    return s.rstrip("/")


def _api(
    host: str, token: str, method: str, path: str, body: dict | None = None
) -> dict:
    url = f"https://{host}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        raise DatagenError(
            f"databricks {method} {path} HTTP {exc.code}: {detail.strip()}"
        )
    except urllib.error.URLError as exc:
        raise DatagenError(f"databricks {method} {path} unreachable: {exc}")
    if not payload:
        return {}
    return json.loads(payload)


def exec_sql(
    host: str,
    token: str,
    warehouse_id: str,
    catalog: str,
    statement: str,
    schema: str | None = None,
) -> list[list[str]]:
    """Run a SQL statement on the warehouse and return rows (as strings).
    Polls until the statement reaches a terminal state. When `schema` is
    given, unqualified table names resolve under `<catalog>.<schema>`."""
    body = {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "catalog": catalog,
        "wait_timeout": "30s",  # max value the API accepts inline
    }
    if schema:
        body["schema"] = schema
    resp = _api(host, token, "POST", "/api/2.0/sql/statements", body)
    sid = resp.get("statement_id")
    state = (resp.get("status") or {}).get("state", "")
    import time

    while state in ("PENDING", "RUNNING"):
        time.sleep(1)
        resp = _api(host, token, "GET", f"/api/2.0/sql/statements/{sid}")
        state = (resp.get("status") or {}).get("state", "")
    if state != "SUCCEEDED":
        err = (resp.get("status") or {}).get("error") or {}
        raise DatagenError(
            f"databricks SQL {statement!r} ended in state {state!r}: "
            f"{err.get('message', '(no error message)')}"
        )
    rows = (resp.get("result") or {}).get("data_array") or []
    return rows


def resolve(values: dict[str, str]) -> tuple[str, str, str, str]:
    """Resolve `(host, token, warehouse_id, catalog)` from a values dict,
    erroring if the required options are unset."""
    host = _host(values.get("HOST", "") or "")
    token = values.get("TOKEN", "") or ""
    if not host or not token:
        raise DatagenError("HOST and TOKEN must be set")
    wh_id = values.get("WAREHOUSE_ID", "") or ""
    catalog = values.get("CATALOG", "") or ""
    if not wh_id or not catalog:
        raise DatagenError("WAREHOUSE_ID and CATALOG must be set")
    return host, token, wh_id, catalog


def ping(host: str, token: str, warehouse_id: str) -> None:
    """Verify the warehouse exists + token has access. Cheaper than running
    a SQL statement: GETs a single warehouse by id."""
    _api(host, token, "GET", f"/api/2.0/sql/warehouses/{warehouse_id}")
