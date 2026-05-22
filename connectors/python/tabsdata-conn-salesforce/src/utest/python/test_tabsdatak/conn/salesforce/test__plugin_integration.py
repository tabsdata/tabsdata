"""End-to-end Salesforce integration tests against a real Salesforce
org. Skipped unless ALL of these are set:

  - ``SF0__USERNAME``        -- Salesforce login (e.g. ``etl@example.com``)
  - ``SF0__PASSWORD``        -- Salesforce password
  - ``SF0__SECURITY_TOKEN``  -- Per-user security token
  - ``SF0__INSTANCE_URL``    -- Optional; required only when the user
                                is associated with more than one
                                instance (e.g. sandbox vs prod)
  - ``SF0__OBJECT``          -- The SObject to SELECT from (e.g.
                                ``Account``); must already exist in
                                the org

The tests issue read-only SOQL queries -- no destructive operations
are performed against the live org. There is no Salesforce equivalent
of the ``fresh_<db>`` per-test container fixture used by the SQL
connectors; production Salesforce orgs are shared / persistent, so
isolation is the caller's responsibility (use a sandbox org).
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.salesforce import (
    SalesforceSrcConn,
    SalesforceSrc,
    SalesforceTokenCredentials,
)

# noinspection PyProtectedMember
import tabsdatak.conn.salesforce._plugin as plugin
from tabsdatak.spi import SrcPluginCtx


pytestmark = pytest.mark.salesforce


_USERNAME = os.environ.get("SF0__USERNAME")
_PASSWORD = os.environ.get("SF0__PASSWORD")
_SECURITY_TOKEN = os.environ.get("SF0__SECURITY_TOKEN")
_INSTANCE_URL = os.environ.get("SF0__INSTANCE_URL")
_OBJECT = os.environ.get("SF0__OBJECT", "Account")


_skip_reason = (
    "Salesforce integration tests require SF0__USERNAME + SF0__PASSWORD"
    " + SF0__SECURITY_TOKEN (and optionally SF0__INSTANCE_URL +"
    " SF0__OBJECT)"
)


def _gates_ok() -> bool:
    return all([_USERNAME, _PASSWORD, _SECURITY_TOKEN])


pytestmark = [pytestmark, pytest.mark.skipif(not _gates_ok(), reason=_skip_reason)]


# ---------------------------------------------------------------------------
# Test context shim -- concrete subclass of the SPI's abstract SrcContext
# so the integration tests exercise the real SrcContext protocol rather
# than a duck-typed surrogate.
# ---------------------------------------------------------------------------


class _SrcCtx(SrcPluginCtx):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "salesforce-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "salesforce-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def store_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass

@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    out = tmp_path / "work"
    out.mkdir()
    return out


@pytest.fixture
def conn() -> SalesforceSrcConn:
    return SalesforceSrcConn(
        credentials=SalesforceTokenCredentials(
            username=Secret.lit(_USERNAME),
            password=Secret.lit(_PASSWORD),
            security_token=Secret.lit(_SECURITY_TOKEN),
        ),
        instance_url=Secret.lit(_INSTANCE_URL) if _INSTANCE_URL else None,
    )


# ---------------------------------------------------------------------------
# SOQL src -- happy path against the live org
# ---------------------------------------------------------------------------


def test_src_plugin_read_in_returns_parquet_files(conn, work_dir):
    src = SalesforceSrc(
        conn=conn,
        queries=[f"SELECT Id FROM {_OBJECT} LIMIT 1"],
    )
    out = plugin._SalesforceSrcPlugin().read_in(_SrcCtx(work_dir), src)
    assert len(out) == 1
    assert len(out[0]) == 1
    df = pl.read_parquet(out[0][0])
    # We can't assert row counts (the org's data is out of our
    # control) -- but the schema must include the Id column.
    assert "Id" in df.columns


def test_src_plugin_includes_deleted_flag_runs(conn, work_dir):
    src = SalesforceSrc(
        conn=conn,
        queries=[f"SELECT Id FROM {_OBJECT} LIMIT 1"],
        include_deleted=True,
    )
    out = plugin._SalesforceSrcPlugin().read_in(_SrcCtx(work_dir), src)
    assert len(out) == 1
    assert out[0][0].exists()
