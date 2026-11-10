#
#  Copyright 2026 Tabsdata Inc.
#

"""``--max-result`` behaviour for the ``tdk`` list commands.

Every standalone list command caps its listing at ``--max-result`` rows
(default 100) and says so when the server holds more. The cap is enforced by
consuming the SDK's ``*_generator`` twin, so the command must stop pulling
pages once it has enough -- these tests assert both the visible contract (rows
shown, notice emitted) and the invisible one (the generator is not drained).
"""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.cli_utils import DEFAULT_MAX_RESULT, ENVELOPE_BYTES, json_size
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


class _User:
    def __init__(self, index: int):
        self.name = f"user-{index:04d}"
        self.full_name = f"User {index}"
        self.email = f"user-{index}@x.io"
        self.enabled = True
        self._data = {"name": self.name, "email": self.email}


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "proj-a"
    return client


def _counting_users(total: int) -> tuple[MagicMock, list[int]]:
    """A client whose user listing yields ``total`` rows, counting each one
    pulled so a test can prove the command stopped early."""
    client = _client()
    pulled = []

    def _generator(*_args, **_kwargs):
        for index in range(total):
            pulled.append(index)
            yield _User(index)

    client.list_users_generator.side_effect = _generator
    return client, pulled


def _invoke(argv: list[str], *, client: MagicMock, tmp_path, env=None):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv, env={"COLUMNS": "200", **(env or {})})


MORE_RESULTS = "More results available"


def test_default_cap_applies_and_announces_more(tmp_path):
    client, pulled = _counting_users(250)
    result = _invoke(["user", "list"], client=client, tmp_path=tmp_path)

    assert result.exit_code == 0, result.output
    assert f"Number of users: {DEFAULT_MAX_RESULT}" in result.output
    assert MORE_RESULTS in result.output
    # One row beyond the cap is pulled to detect the overflow, and no more:
    # the remaining pages are never fetched.
    assert len(pulled) == DEFAULT_MAX_RESULT + 1


def test_explicit_cap_applies(tmp_path):
    client, pulled = _counting_users(250)
    result = _invoke(
        ["user", "list", "--max-result", "5"], client=client, tmp_path=tmp_path
    )

    assert result.exit_code == 0, result.output
    assert "Number of users: 5" in result.output
    assert "user-0004" in result.output
    assert "user-0005" not in result.output
    assert MORE_RESULTS in result.output
    assert len(pulled) == 6


def test_no_notice_when_listing_fits_exactly(tmp_path):
    client, pulled = _counting_users(5)
    result = _invoke(
        ["user", "list", "--max-result", "5"], client=client, tmp_path=tmp_path
    )

    assert result.exit_code == 0, result.output
    assert "Number of users: 5" in result.output
    assert MORE_RESULTS not in result.output
    assert len(pulled) == 5


def test_no_notice_when_listing_is_short(tmp_path):
    client, _ = _counting_users(2)
    result = _invoke(
        ["user", "list", "--max-result", "5"], client=client, tmp_path=tmp_path
    )

    assert result.exit_code == 0, result.output
    assert "Number of users: 2" in result.output
    assert MORE_RESULTS not in result.output


def test_zero_means_unlimited(tmp_path):
    client, pulled = _counting_users(150)
    result = _invoke(
        ["user", "list", "--max-result", "0"], client=client, tmp_path=tmp_path
    )

    assert result.exit_code == 0, result.output
    assert "Number of users: 150" in result.output
    assert MORE_RESULTS not in result.output
    assert len(pulled) == 150


def test_negative_cap_rejected(tmp_path):
    client, _ = _counting_users(10)
    result = _invoke(
        ["user", "list", "--max-result", "-1"], client=client, tmp_path=tmp_path
    )

    assert result.exit_code != 0


def test_json_stays_parseable_and_notice_goes_to_stderr(tmp_path):
    client, _ = _counting_users(250)
    result = _invoke(
        ["user", "list", "-o", "json", "--max-result", "3"],
        client=client,
        tmp_path=tmp_path,
    )

    assert result.exit_code == 0, result.output
    assert [row["name"] for row in json.loads(result.stdout)] == [
        "user-0000",
        "user-0001",
        "user-0002",
    ]
    assert MORE_RESULTS not in result.stdout
    assert MORE_RESULTS in result.stderr


def test_jsonb_listing_is_the_envelope(tmp_path):
    client, _ = _counting_users(250)
    result = _invoke(
        ["user", "list", "-o", "jsonb", "--max-result", "3"],
        client=client,
        tmp_path=tmp_path,
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert [row["name"] for row in payload["items"]] == [
        "user-0000",
        "user-0001",
        "user-0002",
    ]
    assert payload["truncated"] is True
    assert payload["next"] is None
    assert payload["hint"].startswith(MORE_RESULTS)
    assert MORE_RESULTS in result.stderr


@pytest.mark.parametrize(
    "argv",
    [
        ["user", "list"],
        ["group", "list"],
        ["project", "list"],
        ["collection", "list"],
        ["fn", "list", "-c", "coll-a"],
        ["fn", "patches", "-n", "fn-a", "-c", "coll-a"],
        ["table", "list", "-c", "coll-a"],
        ["table", "versions", "-c", "coll-a", "--name", "tbl-a"],
        ["plan", "list"],
        ["trx", "list"],
        ["fnrun", "list"],
        ["worker", "list"],
    ],
)
def test_every_list_command_offers_the_option(argv, tmp_path):
    client = _client()
    result = _invoke([*argv, "--help"], client=client, tmp_path=tmp_path)

    assert result.exit_code == 0, result.output
    assert "--max-result" in result.output


_PAGE = 4


def _paged_server(total: int) -> tuple[MagicMock, list[dict]]:
    """A real SDK server over a fake connection that serves ``total`` users in
    pages of four, keyed by the pagination id of the page before."""
    # noinspection PyProtectedMember
    from tabsdatak.sdk import tabsdata_server as ts

    requests = []
    names = [f"user-{index:04d}" for index in range(total)]

    def _users_list(pagination_id=None, next_step=None, **kwargs):
        requests.append({"pagination_id": pagination_id, **kwargs})
        start = 0 if pagination_id is None else names.index(pagination_id) + 1
        page = names[start : start + _PAGE]
        more = start + _PAGE < total
        response = MagicMock()
        response.json.return_value = {
            "data": {
                "data": [{"name": name} for name in page],
                "next_pagination_id": page[-1] if more else None,
                "next": "n" if more else None,
            }
        }
        return response

    connection = MagicMock()
    connection.project = "proj-a"
    connection.users_list.side_effect = _users_list
    server = ts.TabsdataServer.__new__(ts.TabsdataServer)
    server.connection = connection
    return server, requests


def _names(result) -> list[str]:
    return [row["name"] for row in json.loads(result.stdout)["items"]]


def test_next_cursor_continues_the_listing(tmp_path):
    server, requests = _paged_server(10)
    argv = ["user", "list", "-o", "jsonb", "--max-result", "6"]
    first = _invoke(argv, client=server, tmp_path=tmp_path)
    assert first.exit_code == 0, first.output
    assert _names(first) == [f"user-{index:04d}" for index in range(6)]
    cursor = json.loads(first.stdout)["next"]
    assert cursor
    assert f"--cursor {cursor}" in first.stderr

    requests.clear()
    second = _invoke([*argv, "--cursor", cursor], client=server, tmp_path=tmp_path)
    assert second.exit_code == 0, second.output
    assert _names(second) == [f"user-{index:04d}" for index in range(6, 10)]
    assert json.loads(second.stdout)["next"] is None
    assert requests[0]["pagination_id"] == "user-0003"


@pytest.mark.parametrize(
    "changed",
    [
        ["--filter", "name:lk:user*"],
        ["--order-by", "name-"],
    ],
)
def test_a_cursor_only_continues_its_own_listing(changed, tmp_path):
    server, requests = _paged_server(10)
    argv = ["user", "list", "-o", "jsonb", "--max-result", "6"]
    first = _invoke(argv, client=server, tmp_path=tmp_path)
    cursor = json.loads(first.stdout)["next"]
    requests.clear()
    second = _invoke(
        [*argv, *changed, "--cursor", cursor], client=server, tmp_path=tmp_path
    )
    assert second.exit_code == 2, second.output
    assert "different listing" in second.output
    assert requests == []


class _Paged:
    def __init__(self, rows):
        self._rows = iter(rows)
        self.position = MagicMock(pagination_id="p", next_step="n", skip=0)

    def __iter__(self):
        return self._rows


def _function(name: str) -> MagicMock:
    function = MagicMock()
    function.name = name
    function._data = {"name": name}
    return function


def test_a_cursor_is_bound_to_the_command_options(tmp_path):
    client = _client()
    client.list_functions_generator.side_effect = lambda *_, **__: _Paged([
        _function("f1"),
        _function("f2"),
    ])
    argv = ["fn", "list", "-o", "jsonb", "--max-result", "1"]
    first = _invoke([*argv, "-c", "c1"], client=client, tmp_path=tmp_path)
    assert first.exit_code == 0, first.output
    cursor = json.loads(first.stdout)["next"]
    same = _invoke(
        [*argv, "-c", "c1", "--cursor", cursor], client=client, tmp_path=tmp_path
    )
    assert same.exit_code == 0, same.output
    other = _invoke(
        [*argv, "-c", "c2", "--cursor", cursor], client=client, tmp_path=tmp_path
    )
    assert other.exit_code == 2, other.output


def _cursor(document: dict) -> str:
    raw = json.dumps(document).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


@pytest.mark.parametrize(
    "document",
    [
        {"p": 1, "n": None, "s": 0, "k": "x"},
        {"p": None, "n": ["n"], "s": 0, "k": "x"},
        {"p": None, "n": None, "s": True, "k": "x"},
        {"p": None, "n": None, "s": "1", "k": "x"},
        {"p": None, "n": None, "s": 0},
    ],
)
def test_a_cursor_with_the_wrong_shape_is_a_usage_error(document, tmp_path):
    client = _client()
    argv = ["user", "list", "--cursor", _cursor(document)]
    result = _invoke(argv, client=client, tmp_path=tmp_path)
    assert result.exit_code == 2, result.output
    assert client.list_users_generator.call_count == 0


def test_filters_and_order_pass_through(tmp_path):
    server, requests = _paged_server(2)
    argv = [
        "user",
        "list",
        "-o",
        "json",
        "--filter",
        "name:lk:user*",
        "--filter",
        "enabled:eq:true",
        "--order-by",
        "name-",
    ]
    result = _invoke(argv, client=server, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert requests[0]["request_filter"] == ["name:lk:user*", "enabled:eq:true"]
    assert requests[0]["order_by"] == "name-"


def test_filters_combine_with_the_command_filters(tmp_path):
    client = _client()
    client.list_executions_generator.return_value = iter([])
    argv = ["plan", "list", "--fn", "f1", "--filter", "name:lk:nightly*"]
    result = _invoke([*argv, "-o", "json"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    kwargs = client.list_executions_generator.call_args.kwargs
    assert kwargs["filter"] == ["function:eq:f1", "name:lk:nightly*"]
    assert kwargs["order_by"] == "triggered_on-"


def test_tag_filters_reach_the_server(tmp_path):
    client = _client()
    client.list_executions_generator.return_value = iter([])
    filters = ["function._parent:is:not_null", "collection.team:eq:finance"]
    argv = ["plan", "list", *[a for f in filters for a in ("--filter", f)]]
    result = _invoke([*argv, "-o", "json"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert client.list_executions_generator.call_args.kwargs["filter"] == filters


def test_order_by_overrides_the_command_order(tmp_path):
    client = _client()
    client.list_executions_generator.return_value = iter([])
    argv = ["plan", "list", "--order-by", "ended_on+", "-o", "json"]
    result = _invoke(argv, client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert client.list_executions_generator.call_args.kwargs["order_by"] == (
        "ended_on+"
    )


@pytest.mark.parametrize(
    "option",
    [
        ["--filter", "name"],
        ["--filter", "name:like:x"],
        ["--order-by", "name*"],
        ["--cursor", "not-a-cursor"],
    ],
)
def test_malformed_list_options_are_usage_errors(option, tmp_path):
    client = _client()
    result = _invoke(["user", "list", *option], client=client, tmp_path=tmp_path)
    assert result.exit_code == 2, result.output
    assert client.list_users_generator.call_count == 0


def test_machine_max_result_caps_an_unlimited_listing(tmp_path, machine_settings):
    machine_settings.update(max_result=5)
    client, pulled = _counting_users(20)
    result = _invoke(
        ["user", "list", "--max-result", "0", "--machine"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert len(payload["items"]) == 5
    assert payload["truncated"] is True
    assert len(pulled) == 6


def test_machine_max_result_zero_lifts_the_cap(tmp_path, machine_settings):
    machine_settings.update(max_result=0)
    client, pulled = _counting_users(1500)
    result = _invoke(
        ["user", "list", "--max-result", "0", "--machine"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert len(json.loads(result.stdout)["items"]) == 1500


def test_a_truncated_machine_listing_hints_at_its_cap_and_cursor(
    tmp_path, machine_settings
):
    machine_settings.update(max_result=8)
    server, _ = _paged_server(10)
    result = _invoke(
        ["user", "list", "--max-result", "3", "--machine"],
        client=server,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["truncated"] is True
    assert "'--max-result 8' for up to 8" in payload["hint"]
    assert f"'--cursor {payload['next']}'" in payload["hint"]
    assert "--max-result 0" not in payload["hint"]


def test_a_row_count_cut_suggests_only_with_a_higher_max_result(
    tmp_path, machine_settings
):
    machine_settings.update(max_result=8)
    server, _ = _paged_server(10)
    result = _invoke(
        ["user", "list", "--max-result", "3", "--machine"],
        client=server,
        tmp_path=tmp_path,
    )
    hint = json.loads(result.stdout)["hint"]
    assert "and keep only the fields you need with '--fields <FIELD,...>'" in hint


def test_a_machine_listing_at_its_cap_hints_only_the_cursor(tmp_path, machine_settings):
    machine_settings.update(max_result=8)
    server, _ = _paged_server(10)
    result = _invoke(
        ["user", "list", "--max-result", "0", "--machine"],
        client=server,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert len(payload["items"]) == 8
    assert "--max-result" not in payload["hint"]
    assert f"'--cursor {payload['next']}'" in payload["hint"]


def test_a_complete_listing_has_no_hint(tmp_path, machine_settings):
    machine_settings.update(max_result=8)
    server, _ = _paged_server(5)
    result = _invoke(["user", "list", "--machine"], client=server, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "hint" not in json.loads(result.stdout)


def _pages(argv: list[str], server, tmp_path) -> list[dict]:
    pages, cursor = [], None
    while True:
        more = ["--cursor", cursor] if cursor else []
        result = _invoke([*argv, *more], client=server, tmp_path=tmp_path)
        assert result.exit_code == 0, result.output
        pages.append(json.loads(result.stdout))
        cursor = pages[-1]["next"]
        if cursor is None:
            return pages


def test_a_machine_listing_stops_at_its_byte_budget_and_resumes_there(
    tmp_path, machine_settings
):
    budget = ENVELOPE_BYTES + 70
    machine_settings.update(max_result=8, max_bytes=budget)
    server, _ = _paged_server(10)
    pages = _pages(["user", "list", "--machine"], server, tmp_path)
    assert all(json_size(page) <= budget for page in pages)
    names = [row["name"] for page in pages for row in page["items"]]
    assert names == [f"user-{index:04d}" for index in range(10)]
    assert [len(page["items"]) for page in pages] == [3, 3, 3, 1]
    assert "all that fit in one result" in pages[0]["hint"]
    assert f"'--cursor {pages[0]['next']}'" in pages[0]["hint"]
    assert "keep only the fields you need with '--fields <FIELD,...>'" in pages[0]["hint"]
    assert "showing the first 3" in pages[0]["hint"]
    assert all("showing the next 3" in page["hint"] for page in pages[1:3])


def test_a_byte_cut_with_only_asks_for_fewer_fields(tmp_path, machine_settings):
    machine_settings.update(max_result=8, max_bytes=ENVELOPE_BYTES + 70)
    server, _ = _paged_server(10)
    result = _invoke(
        ["user", "list", "--fields", "name", "--machine"],
        client=server,
        tmp_path=tmp_path,
    )
    hint = json.loads(result.stdout)["hint"]
    assert "name fewer fields in '--fields'" in hint
    assert "<FIELD,...>" not in hint


def test_a_row_larger_than_the_byte_budget_is_still_listed(tmp_path, machine_settings):
    machine_settings.update(max_result=8, max_bytes=ENVELOPE_BYTES + 5)
    server, _ = _paged_server(3)
    pages = _pages(["user", "list", "--machine"], server, tmp_path)
    assert [len(page["items"]) for page in pages] == [1, 1, 1]


def test_only_keeps_the_named_fields(tmp_path):
    client, _ = _counting_users(3)
    result = _invoke(
        ["user", "list", "-o", "jsonb", "--fields", "email"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["items"] == [
        {"email": f"user-{index}@x.io"} for index in range(3)
    ]


def test_an_unknown_field_names_the_fields_the_rows_have(tmp_path, machine_settings):
    client, _ = _counting_users(3)
    result = _invoke(
        ["user", "list", "--fields", "name,nickname", "--machine"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 2, result.output
    error = json.loads(result.stdout)["error"]
    assert error["type"] == "usage"
    assert error["message"].endswith("do not have: nickname. They have: name, email.")


def test_only_does_not_apply_to_the_table_view(tmp_path):
    client, _ = _counting_users(3)
    result = _invoke(
        ["user", "list", "--fields", "name"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 2, result.output
    assert "'--fields' applies to json, jsonb and yaml output." in result.output


def _timed_users() -> MagicMock:
    client = _client()
    user = _User(0)
    user._data = {"name": user.name, "defined_on": 1750074554472, "count_on": "x"}
    client.list_users_generator.side_effect = lambda *_a, **_k: iter([user])
    return client


def test_a_machine_run_shows_times_as_iso(tmp_path, machine_settings):
    result = _invoke(
        ["user", "list", "--machine"], client=_timed_users(), tmp_path=tmp_path
    )
    (row,) = json.loads(result.stdout)["items"]
    assert row == {
        "name": "user-0000",
        "defined_on": "2025-06-16T11:49:14.472Z",
        "count_on": "x",
    }


def test_a_persons_jsonb_gives_times_as_iso(tmp_path):
    result = _invoke(
        ["user", "list", "-o", "jsonb"], client=_timed_users(), tmp_path=tmp_path
    )
    defined_on = json.loads(result.stdout)["items"][0]["defined_on"]
    assert defined_on == "2025-06-16T11:49:14.472Z"


def test_json_keeps_times_as_numbers(tmp_path):
    result = _invoke(
        ["user", "list", "-o", "json"], client=_timed_users(), tmp_path=tmp_path
    )
    assert json.loads(result.stdout)[0]["defined_on"] == 1750074554472


def test_an_unknown_field_is_an_invalid_request_with_a_hint(tmp_path):
    from tabsdatak.sdk._apiserver import APIServerError

    client = _client()
    client.list_users_generator.side_effect = APIServerError(
        {"code": "ListError::3", "error_description": "Undefined filter: colour"},
        status=400,
    )
    argv = ["user", "list", "--filter", "colour:eq:red", "--machine"]
    result = _invoke(argv, client=client, tmp_path=tmp_path)
    assert result.exit_code == 1
    error = json.loads(result.stdout)["error"]
    assert error["type"] == "invalid_request"
    assert "--filter" in error["hint"]


def test_a_listing_error_without_list_options_has_no_list_hint(tmp_path):
    from tabsdatak.sdk._apiserver import APIServerError

    client = _client()
    client.list_users_generator.side_effect = APIServerError(
        {"code": "ListError::3", "error_description": "Undefined filter: colour"},
        status=400,
    )
    result = _invoke(["user", "list", "--machine"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 1
    error = json.loads(result.stdout)["error"]
    assert error["type"] == "invalid_request"
    assert error.get("hint") is None


@pytest.mark.parametrize(
    "argv",
    [
        ["user", "list"],
        ["group", "list"],
        ["project", "list"],
        ["collection", "list"],
        ["fn", "list", "-c", "coll-a"],
        ["fn", "patches", "-n", "fn-a", "-c", "coll-a"],
        ["table", "list", "-c", "coll-a"],
        ["table", "versions", "-c", "coll-a", "--name", "tbl-a"],
        ["plan", "list"],
        ["trx", "list"],
        ["fnrun", "list"],
        ["worker", "list"],
        ["cron", "list"],
        ["cron", "plans"],
        ["stream", "list"],
        ["stream", "plans"],
    ],
)
def test_every_paged_list_command_offers_the_list_options(argv, tmp_path):
    result = _invoke([*argv, "--help"], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    for option in ("--filter", "--order-by", "--cursor"):
        assert option in result.output
