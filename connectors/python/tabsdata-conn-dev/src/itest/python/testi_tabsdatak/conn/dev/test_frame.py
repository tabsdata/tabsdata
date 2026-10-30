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

from tabsdatak.conn.dev import DevFrameDestConn, DevFrameSrcConn
from testi_tabsdatak.conn.common.function_project import function_module

pytestmark = pytest.mark.conn_dev

FUNCTIONS = Path(__file__).parent / "functions"
frame = function_module(FUNCTIONS, "frame")


@tabsdata_instance(consumer="tdtest")
@fs_domain("fs")
def test_frames_flow_from_a_frame_source_to_a_frame_destination(
    instance_tdtest: Instance,
    domain_fs: FsDomain,
    collection_factory: CollectionFactory,
    request: pytest.FixtureRequest,
) -> None:
    folder = (domain_fs.base_path / moniker(request)).absolute()
    folder.mkdir(parents=True)
    source = collection_factory(
        "sources",
        DevFrameSrcConn(),
        frame.SOURCE_COLLECTION,
    )
    run_function(source, frame.publish, FUNCTIONS)
    destination = collection_factory(
        "destinations",
        DevFrameDestConn(folder=str(folder)),
    )
    run_function(destination, frame.report, FUNCTIONS)
    reports = list(folder.glob("*.json"))
    assert len(reports) == 1, f"expected one report in {folder}, found {reports}"
    assert json.loads(reports[0].read_text()) == {
        "tables": [
            {
                "type": "LazyFrame",
                "rows": frame.GENERATED_FRAME.height,
                "columns": frame.GENERATED_FRAME.columns,
            },
        ],
    }
