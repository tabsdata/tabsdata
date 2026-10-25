#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tabsdatak.common._transporter.invoker.transporter1 import (
    TransporterBinaryNotFoundError,
)
from tabsdatak.server._tdrunner.function._commons.document.request import AnyRequest
from tabsdatak.server._tdrunner.function._legacies.localize_fn_bundle import fetch
from tabsdatak.server._tdrunner.function._legacies.localize_fn_bundle.error import (
    TransporterFailedError,
)
from tabsdatak.server._tdrunner.function.commands.get_bundle.error import (
    GetBundleErrorCode,
)
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


@pytest.fixture
def arguments(tmp_path: Path) -> dict:
    request_path = tmp_path / "request.yaml"
    request_path.write_text(yaml.safe_dump({"Function": {"info": info_payload()}}))
    storage_config = tmp_path / "storage.yaml"
    storage_config.write_text(
        "storage:\n  mounts:\n    - id: ROOT\n      options: {}\n"
    )
    return {
        "request": AnyRequest.load(
            request_path,
            GetBundleErrorCode.GB_1,
        ),
        "bundle_archive": tmp_path / "bundle.tar.zst",
        "transporter_request": tmp_path / "shuttle" / "bundle-fetch.yaml",
        "transporter_response": tmp_path / "shuttle" / "bundle-fetch-response.yaml",
        "storage_config": storage_config,
    }


def test_fetch_writes_the_transporter_request_and_runs_it(
    monkeypatch: pytest.MonkeyPatch,
    arguments: dict,
):
    calls = []

    def transporter(request: Path, response: Path) -> int:
        calls.append((request, response))
        arguments["bundle_archive"].write_bytes(b"archive")
        return 0

    monkeypatch.setattr(fetch, "run_transporter", transporter)
    fetch.fetch_via_transporter(**arguments)
    assert calls == [
        (arguments["transporter_request"], arguments["transporter_response"]),
    ]
    assert arguments["transporter_request"].exists()
    assert arguments["bundle_archive"].read_bytes() == b"archive"


def test_fetch_without_transporter_binary_fails(
    monkeypatch: pytest.MonkeyPatch,
    arguments: dict,
):
    def transporter(request: Path, response: Path) -> int:
        raise TransporterBinaryNotFoundError("no binary")

    monkeypatch.setattr(fetch, "run_transporter", transporter)
    with pytest.raises(TransporterFailedError, match="no binary"):
        fetch.fetch_via_transporter(**arguments)


def test_fetch_with_failing_transporter_fails(
    monkeypatch: pytest.MonkeyPatch,
    arguments: dict,
):
    monkeypatch.setattr(fetch, "run_transporter", lambda request, response: 7)
    with pytest.raises(TransporterFailedError, match="exited 7"):
        fetch.fetch_via_transporter(**arguments)
