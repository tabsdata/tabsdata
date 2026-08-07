#
#  Copyright 2026 Tabsdata Inc.
#

"""``tdk cron`` / ``tdk stream``: every command in both groups reads the trigger
worker listing, so what separates them is the ``worker_type`` filter they send --
``CT`` for cron triggers, ``ST`` for stream publishers.

``list`` narrows the listing by it (the rendered output contract lives in the
render harness); ``enable`` / ``disable`` require it to be non-empty before
touching the function, which is the precondition pinned here.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "proj-a"
    client.list_workers_generator.side_effect = lambda *a, **k: iter([])
    # Neither enabled nor disabled, so enable/disable never short-circuits as a
    # no-op here (that path is covered in test_enable_disable.py).
    client.get_function.return_value = MagicMock(enabled=None)
    return client


def _invoke(argv, *, client, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv)


def _sent_filter(client) -> list[str]:
    return client.list_workers_generator.call_args.kwargs["filter"]


def _with_trigger(client) -> MagicMock:
    """Wire the listing so the function looks like it has the trigger."""
    client.list_workers_generator.side_effect = lambda *a, **k: iter([MagicMock()])
    return client


@pytest.mark.parametrize(
    "group,worker_type",
    [("cron", "worker_type:eq:CT"), ("stream", "worker_type:eq:ST")],
)
def test_list_filters_by_its_worker_type(group, worker_type, tmp_path):
    client = _client()
    result = _invoke([group, "list"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert _sent_filter(client) == [worker_type]


@pytest.mark.parametrize("group", ["cron", "stream"])
def test_list_narrows_by_collection_and_function(group, tmp_path):
    client = _client()
    result = _invoke(
        [group, "list", "--coll", "coll-a", "--name", "fn-a"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    sent = _sent_filter(client)
    assert "function:eq:fn-a" in sent
    assert "collection:eq:coll-a" in sent


@pytest.mark.parametrize("group", ["cron", "stream"])
def test_list_defaults_to_the_pinned_collection(group, tmp_path):
    # The pin narrows the listing as it does elsewhere; unlike `fn list` it is
    # never prompted for, since the listing is fine spanning the project.
    (tmp_path / "options.json").write_text('{"pins": {"collection": "coll-p"}}')
    client = _client()
    result = _invoke([group, "list"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "collection:eq:coll-p" in _sent_filter(client)


@pytest.mark.parametrize("group", ["cron", "stream"])
def test_list_spans_projects_with_all_projects(group, tmp_path):
    # The listing project-narrows on the connection's active project, so
    # --all-projects clears it (as on `collection list`) instead of filtering.
    client = _client()
    result = _invoke(
        [group, "list", "--all-projects"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert client.connection.project is None


@pytest.mark.parametrize("group", ["cron", "stream"])
def test_list_surfaces_sdk_failure_as_a_clean_error(group, tmp_path):
    client = _client()
    client.list_workers_generator.side_effect = RuntimeError("boom-xyz")
    result = _invoke([group, "list"], client=client, tmp_path=tmp_path)
    assert result.exit_code != 0
    assert "boom-xyz" in result.output


# --- enable / disable: `tdk fn enable|disable` gated on the trigger ----------


@pytest.mark.parametrize("group,worker_type", [("cron", "CT"), ("stream", "ST")])
@pytest.mark.parametrize("action,enabled", [("enable", True), ("disable", False)])
def test_enable_disable_checks_the_trigger_then_calls_the_sdk(
    group, worker_type, action, enabled, tmp_path
):
    client = _with_trigger(_client())
    result = _invoke(
        [group, action, "-n", "f1", "-c", "coll1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert "Function updated successfully" in result.output
    # The precondition is read for this function, by this trigger type...
    sent = _sent_filter(client)
    assert f"worker_type:eq:{worker_type}" in sent
    assert "function:eq:f1" in sent
    assert "collection:eq:coll1" in sent
    # ...and only then is the function toggled, exactly as `tdk fn` would.
    client.enable_function.assert_called_once_with(
        collection_name="coll1", function_name="f1", enabled=enabled
    )


@pytest.mark.parametrize(
    "group,missing",
    [
        ("cron", "No cron trigger found"),
        ("stream", "No stream-publisher trigger found"),
    ],
)
@pytest.mark.parametrize("action", ["enable", "disable"])
def test_enable_disable_refuses_a_function_without_the_trigger(
    group, missing, action, tmp_path
):
    # No trigger worker: the function is left alone and the error names the
    # command that does work on any function.
    client = _client()
    result = _invoke(
        [group, action, "-n", "f1", "-c", "coll1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code != 0
    assert missing in result.output
    assert f"tdk fn {action}" in result.output
    client.enable_function.assert_not_called()


@pytest.mark.parametrize("group", ["cron", "stream"])
def test_enable_surfaces_sdk_failure_as_a_clean_error(group, tmp_path):
    client = _with_trigger(_client())
    client.enable_function.side_effect = RuntimeError("boom-xyz")
    result = _invoke(
        [group, "enable", "-n", "f1", "-c", "coll1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code != 0
    assert "boom-xyz" in result.output
