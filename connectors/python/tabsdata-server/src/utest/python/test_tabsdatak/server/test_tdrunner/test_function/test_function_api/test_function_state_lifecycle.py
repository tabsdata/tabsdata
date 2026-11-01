#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import functools
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.conn.localfile import LocalFileDestConn, LocalFileSrcConn
from tabsdatak.conn.localfile._plugin import LocalFileDestPlugin
from tabsdatak.server._tdrunner.function._handlers.state import State
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from test_tabsdatak.server.conftest import TESTING_RESOURCES_FOLDER

from .driver import (
    PUBLISHER,
    SUBSCRIBER,
    TRANSFORMER,
    Driver,
    Function,
    Run,
    StateSlots,
    copy_project,
    forget,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

RESOURCES = Path(TESTING_RESOURCES_FOLDER) / "run_function"
STATE_TABLE = "td.fn_state__F-1"
RUNS = 3
ROWS = pl.DataFrame({"id": [1, 2]})


def run_three_times(
    tmp_path: Path,
    monkeypatch,
    apiserver,
    run: Callable[[Driver, StateSlots], Run],
) -> list[dict]:
    url, posts = apiserver
    states = tmp_path / "states"
    states.mkdir()
    previous = None
    seen = []
    for number in range(1, RUNS + 1):
        target = states / f"state-{number}.t"
        slots = StateSlots(table=STATE_TABLE, previous=previous, target=target)
        driver = Driver(tmp_path / f"run-{number}", monkeypatch, url, posts)
        state = run(driver, slots).response["output"][-1]
        assert state["table"] == STATE_TABLE
        assert state["info"]["row_count"] == 1
        seen.append(State.load(TableFile.local(target)).values)
        previous = target
    return seen


@pytest.fixture
def function(tmp_path, monkeypatch, request):
    kind, project, package, module, name = request.param
    copy_project(RESOURCES, tmp_path, monkeypatch, project)
    yield Function(kind, tmp_path / project, package, module, name)
    forget(package)


@pytest.mark.parametrize(
    "function",
    [(PUBLISHER, "ctx_project", "my_pkg", "publisher", "my_publisher")],
    indirect=True,
)
def test_a_publisher_reads_the_state_its_last_run_left(
    tmp_path, monkeypatch, apiserver, no_http_retry, function
):
    def publish(driver: Driver, state: StateSlots) -> Run:
        return driver.run(
            function,
            outputs=["my_table"],
            source_files={"rows.parquet": ROWS},
            connection=lambda base: LocalFileSrcConn(base_path=str(base)).to_dict(),
            state=state,
        )

    seen = run_three_times(tmp_path, monkeypatch, apiserver, publish)
    assert [values["my-key"] for values in seen] == [
        "from-user-fn",
        "again-after-from-user-fn",
        "again-after-again-after-from-user-fn",
    ]


@pytest.mark.parametrize(
    "function",
    [(TRANSFORMER, "tx_ctx_project", "tx_ctx_pkg", "transformer", "my_transformer")],
    indirect=True,
)
def test_a_transformer_reads_the_state_its_last_run_left(
    tmp_path, monkeypatch, apiserver, no_http_retry, function
):
    def transform(driver: Driver, state: StateSlots) -> Run:
        return driver.run(
            function, outputs=["t_out"], inputs={"t_in": ROWS}, state=state
        )

    seen = run_three_times(tmp_path, monkeypatch, apiserver, transform)
    assert seen == [
        {"smoke.last-seen": "tx-after-None"},
        {"smoke.last-seen": "tx-after-tx-after-None"},
        {"smoke.last-seen": "tx-after-tx-after-tx-after-None"},
    ]


@pytest.mark.parametrize(
    "function",
    [(SUBSCRIBER, "sub_ctx_project", "sub_ctx_pkg", "subscriber", "my_subscriber")],
    indirect=True,
)
def test_a_subscriber_reads_the_state_its_last_run_left(
    tmp_path, monkeypatch, apiserver, no_http_retry, function
):
    write_out = LocalFileDestPlugin.write_out

    @functools.wraps(write_out)
    def stateful_write_out(self, ctx, conn, dest, tables):
        ctx.set_attr("dest.last-seen", "dest-after-" + str(ctx.get_attr("dest.last-seen")))
        return write_out(self, ctx, conn, dest, tables)

    monkeypatch.setattr(LocalFileDestPlugin, "write_out", stateful_write_out)

    def deliver(driver: Driver, state: StateSlots) -> Run:
        return driver.run(
            function,
            outputs=[],
            inputs={"t_in": ROWS},
            connection=lambda base: LocalFileDestConn(base_path=str(base)).to_dict(),
            state=state,
        )

    seen = run_three_times(tmp_path, monkeypatch, apiserver, deliver)
    assert seen == [
        {
            "smoke.last-seen": "sub-after-None",
            "dest.last-seen": "dest-after-None",
        },
        {
            "smoke.last-seen": "sub-after-sub-after-None",
            "dest.last-seen": "dest-after-dest-after-None",
        },
        {
            "smoke.last-seen": "sub-after-sub-after-sub-after-None",
            "dest.last-seen": "dest-after-dest-after-dest-after-None",
        },
    ]
