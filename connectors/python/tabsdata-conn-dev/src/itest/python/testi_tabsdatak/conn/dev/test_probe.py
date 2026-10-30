#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.harness import CollectionFactory, run_function
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from tabsdatax.itest.fixture.monikers import moniker
from testi_tabsdatak.conn.common.function_project import function_module

from tabsdatak.conn.dev import DataGenSrcConn, ProbeDestConn

pytestmark = pytest.mark.conn_dev

FUNCTIONS = Path(__file__).parent / "functions"
probe = function_module(FUNCTIONS, "probe")


@tabsdata_instance(consumer="tdtest")
@fs_domain("fs")
def test_a_bus_destination_receives_the_user_columns_of_each_output(
    instance_tdtest: Instance,
    domain_fs: FsDomain,
    collection_factory: CollectionFactory,
    request: pytest.FixtureRequest,
) -> None:
    folder = (domain_fs.base_path / moniker(request)).absolute()
    folder.mkdir(parents=True)
    source = collection_factory(
        "sources",
        DataGenSrcConn(),
        probe.SOURCE_COLLECTION,
    )
    run_function(source, probe.publish, FUNCTIONS)
    destination = collection_factory(
        "destinations",
        ProbeDestConn(folder=str(folder)),
    )
    run_function(destination, probe.probe, FUNCTIONS)
    reports = list(folder.glob("*.json"))
    assert len(reports) == 1, f"expected one report in {folder}, found {reports}"
    assert json.loads(reports[0].read_text()) == {
        "bus": "DestBusOfLazyFrame",
        "channels": [
            {
                "name": "out_0",
                "position": 0,
                "rows": probe.GENERATED_FRAME.height,
                "columns": probe.GENERATED_FRAME.columns,
            },
        ],
    }
