#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from itertools import islice
from unittest.mock import MagicMock

# noinspection PyProtectedMember
from tabsdatak.sdk import tabsdata_server as ts

_PAGES = {
    (None, None): (["a", "b", "c"], ("c", "n1")),
    ("c", "n1"): (["d", "e", "f"], ("f", "n2")),
    ("f", "n2"): (["g"], (None, None)),
}


def _fetch(requests: list):
    def fetch(pagination_id, next_step):
        requests.append((pagination_id, next_step))
        rows, (next_id, next_step) = _PAGES[(pagination_id, next_step)]
        response = MagicMock()
        response.json.return_value = {
            "data": {
                "data": [{"name": row} for row in rows],
                "next_pagination_id": next_id,
                "next": next_step,
            }
        }
        return response

    return fetch


def _names(listing) -> list[str]:
    return [row["name"] for row in listing]


def test_listing_walks_every_page_lazily():
    requests = []
    listing = ts.Listing(_fetch(requests), dict)
    assert requests == []
    assert _names(listing) == list("abcdefg")
    assert requests == [(None, None), ("c", "n1"), ("f", "n2")]


def test_position_is_the_cursor_of_the_last_item():
    listing = ts.Listing(_fetch([]), dict)
    list(islice(listing, 5))
    assert listing.position == ts.ListCursor("c", "n1", 1)


def test_cursor_resumes_at_its_item():
    requests = []
    listing = ts.Listing(_fetch(requests), dict, ts.ListCursor("c", "n1", 1))
    assert _names(listing) == list("efg")
    assert requests[0] == ("c", "n1")


def test_an_empty_listing_stops():
    response = MagicMock()
    response.json.return_value = {"data": None}
    assert list(ts.Listing(lambda *_: response, dict)) == []


def test_generators_accept_a_cursor():
    connection = MagicMock()
    requests = []
    fetch = _fetch(requests)
    connection.users_list.side_effect = lambda pagination_id, next_step, **_: fetch(
        pagination_id, next_step
    )
    server = ts.TabsdataServer.__new__(ts.TabsdataServer)
    server.connection = connection
    users = server.list_users_generator(cursor=ts.ListCursor("f", "n2", 0))
    assert [user.name for user in users] == ["g"]
    assert requests == [("f", "n2")]
