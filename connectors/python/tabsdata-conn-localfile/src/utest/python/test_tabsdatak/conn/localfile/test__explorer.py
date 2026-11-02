#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.localfile import LocalFileSrcConn
from tabsdatak.conn.localfile._explorer import LocalFileExplorer

pytestmark = pytest.mark.localfile


def test_metadata_reads_each_file_schema(tmp_path):
    (tmp_path / "invoices.csv").write_text("invoice_id,customer_email,amount_due\n1,a@x.com,120.5\n")
    (tmp_path / "events").mkdir()
    (tmp_path / "events" / "e.ndjson").write_text('{"id": 1, "kind": "open"}\n')
    (tmp_path / "readme.md").write_text("# notes\n")
    conn = LocalFileSrcConn(base_path=str(tmp_path))
    explorer = LocalFileExplorer()

    entities = {e.file.pattern: e for e in explorer.metadata(conn)}

    assert entities["invoices.csv"].field_names() == ["invoice_id", "customer_email", "amount_due"]
    assert entities["e.ndjson"].field_names() == ["id", "kind"]
    assert entities["readme.md"].field_names() == []
    assert entities["invoices.csv"].to_dict()["columns"][2] == {"name": "amount_due", "type": "float"}
