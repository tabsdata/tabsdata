#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import sys

from tabsdatak.common._tabular import Column, Kind, TableView, show

COLUMN_KINDS = {"status": Kind.STATUS}


def view(payload: dict) -> TableView:
    return TableView(
        title=payload.get("title") or "",
        columns=[
            Column(
                column.get("name") or "",
                COLUMN_KINDS.get(column.get("color_scheme"), Kind.TEXT),
            )
            for column in payload.get("columns", [])
        ],
        rows=[list(row) for row in payload.get("rows", [])],
        max_width=0,
    )


def main() -> int:
    payload = json.load(sys.stdin)
    entries = payload if isinstance(payload, list) else [payload]
    show([view(entry) for entry in entries])
    return 0


if __name__ == "__main__":
    sys.exit(main())
