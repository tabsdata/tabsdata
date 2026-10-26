#
# Copyright 2026 Tabsdata Inc.
#

"""Every row of a server listing, read page by page: a page holds at most the
server's default length, so counting one page undercounts a long list."""

from __future__ import annotations

from tabsdatak.sdk.tabsdata_server import TabsdataServer


def listed(server: TabsdataServer, path: str) -> list[dict]:
    items, params = [], {}
    while True:
        response = server.connection.get(path, params=params)
        response.raise_for_status()
        page = response.json()["data"]
        items += page.get("data") or []
        if page.get("next") is None or page.get("next_pagination_id") is None:
            return items
        params = {"next": page["next"], "pagination_id": page["next_pagination_id"]}
