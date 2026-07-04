"""Snowflake connection (snowflake-connector-python). Built from a resolved
`{OPTION: value}` dict — no argparse / descriptor coupling."""
from __future__ import annotations

from contextlib import contextmanager

from ...errors import DatagenError


def connect(values: dict[str, str]):
    try:
        import snowflake.connector  # type: ignore
    except ImportError as exc:
        raise DatagenError(
            "snowflake-connector-python is not installed "
            "(pip install snowflake-connector-python)") from exc
    return snowflake.connector.connect(
        account=values.get("ACCOUNT", ""),
        user=values.get("USERNAME", ""),
        password=values.get("PAT", ""),          # PAT passed as password
        warehouse=values.get("WAREHOUSE", ""),
        database=values.get("DATABASE", ""),
        client_session_keep_alive=False,
    )


@contextmanager
def cursor(values: dict[str, str]):
    conn = connect(values)
    try:
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()
    finally:
        conn.close()
