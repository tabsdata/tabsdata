#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.azureblob import AzureBlobSrcConn
from tabsdatak.conn.azureblob import _explorer
from tabsdatak.conn.common.types import AzureAccountKey

pytestmark = pytest.mark.azureblob


def test_metadata_reads_blob_schemas_through_the_filesystem(tmp_path, monkeypatch):
    pq.write_table(pa.table({"sku": ["a"], "qty": [1]}), tmp_path / "stock.parquet")
    blobs = [SimpleNamespace(name="stock.parquet", size=100, last_modified=datetime(2026, 9, 1, tzinfo=timezone.utc))]
    monkeypatch.setattr(
        _explorer, "_container_client", lambda conn: SimpleNamespace(list_blobs=lambda name_starts_with=None: blobs)
    )
    opened = []

    def open_input_file(path):
        opened.append(path)
        return open(tmp_path / path.split("/", 1)[1], "rb")

    monkeypatch.setattr(_explorer, "_filesystem", lambda conn: SimpleNamespace(open_input_file=open_input_file))
    conn = AzureBlobSrcConn(
        container="my-container",
        credentials=AzureAccountKey(account_name=Secret("acctname"), account_key=Secret("acctkey")),
    )

    [entity] = _explorer.AzureBlobConnectionExplorer().metadata(conn)

    assert opened == ["my-container/stock.parquet"]
    assert [(c.name, c.type) for c in entity.columns] == [("sku", "string"), ("qty", "int64")]
