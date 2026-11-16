#
# Copyright 2026 Tabsdata Inc.
#

"""The function state's lifecycle against a live apiserver and Kubernetes.

A publisher, a transformer downstream of it and a subscriber downstream of the
transformer each count their runs in their own state and write the count into
the data they produce. Each runs three times: a first run starts without
state, and each later run must read the state the server committed for the run
before it. The enterprise edition runs the other two in the publisher's
executions; the open-source edition runs only the function triggered, so each
is triggered in turn.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    table_frame,
    wait_for_completion,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from tabsdatax.itest.fixture.monikers import moniker

from tabsdatak.conn.dev import DataGenSrcConn
from tabsdatak.conn.localfile import LocalFileDestConn
from tabsdatak.extensions.variant.edition import ENTERPRISE
from tabsdatak.sdk.status_utils.groups import Entity, Health, health
from testi_tabsdatak.conn.common.function_project import function_module

pytestmark = pytest.mark.sdk

FUNCTIONS = Path(__file__).parent / "functions"
stateful = function_module(FUNCTIONS, "stateful")

RUNS = 3


def runs(frame: pl.DataFrame | None) -> list[int]:
    assert frame is not None, "The table holds no data"
    return frame[stateful.RUNS_COLUMN].unique().to_list()


@tabsdata_instance(consumer="tdtest")
@fs_domain("fs")
def test_every_kind_of_function_reads_the_state_its_last_run_left(
    instance_tdtest: Instance,
    domain_fs: FsDomain,
    collection_factory: CollectionFactory,
    request: pytest.FixtureRequest,
) -> None:
    folder = (domain_fs.base_path / moniker(request)).absolute()
    folder.mkdir(parents=True)
    source = collection_factory("sources", DataGenSrcConn(), stateful.SOURCE_COLLECTION)
    factory = collection_factory("default", name=stateful.FACTORY_COLLECTION)
    destination = collection_factory(
        "destinations", LocalFileDestConn(base_path=str(folder))
    )
    publisher = source.register_function(
        info=stateful.publish, project_root=str(FUNCTIONS)
    )
    transformer = factory.register_function(
        info=stateful.transform, project_root=str(FUNCTIONS)
    )
    subscriber = destination.register_function(
        info=stateful.deliver, project_root=str(FUNCTIONS)
    )
    triggered = [publisher] if ENTERPRISE else [publisher, transformer, subscriber]

    for run in range(1, RUNS + 1):
        for function in triggered:
            status = wait_for_completion(function.trigger())
            assert health(Entity.EXECUTION, status) is Health.OK, (
                f"run {run} of {function.name} finished with non-success status "
                f"{status!r}"
            )
        assert runs(table_frame(source, stateful.PUBLISHED_TABLE)) == [run]
        assert runs(table_frame(factory, stateful.TRANSFORMED_TABLE)) == [run]
        assert runs(pl.read_parquet(folder / stateful.DELIVERED_FILE)) == [run]
