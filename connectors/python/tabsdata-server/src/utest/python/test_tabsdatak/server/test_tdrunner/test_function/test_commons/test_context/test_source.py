#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io

import pytest
from tabsdatax.itest.fixture.backends.azurebloblocal import AzureBlobLocalDomain
from tabsdatax.itest.fixture.backends.gcslocal import GcsLocalDomain
from tabsdatax.itest.fixture.backends.s3local import S3LocalDomain
from tabsdatax.itest.fixture.emulators import EMULATORS, emulated

from tabsdatak.server._tdrunner.function._commons.context.source import (
    PLUGIN_FOLDER,
    SourceContext,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Info,
)
from test_tabsdatak.server.test_tdrunner.test_function.conftest import (
    emulated_run,
    info_payload,
    run_location,
    stored,
)

pytestmark = [pytest.mark.unit]


@emulated
@pytest.mark.parametrize("emulator", EMULATORS)
def test_the_scratch_is_a_plugin_folder_in_the_runs_scratch(
    domain_azurebloblocal: AzureBlobLocalDomain,
    domain_gcslocal: GcsLocalDomain,
    domain_s3local: S3LocalDomain,
    tmp_path,
    monkeypatch,
    emulator,
):
    domain = locals()[f"domain_{emulator}"]
    run = emulated_run(domain, monkeypatch)
    scratch = run_location(run, "s")
    info = Info.model_validate(
        info_payload(function_landing=run_location(run, "r"), function_scratch=scratch)
    )
    context = SourceContext.from_info(tmp_path / "work", info)
    context._scratch.write("a.bin", io.BytesIO(b"staged"))
    try:
        assert context._scratch.read("a.bin").read() == b"staged"
        assert stored(scratch, domain) == [f"{PLUGIN_FOLDER}/a.bin"]
    finally:
        context._scratch.clear()
    assert stored(scratch, domain) == []
