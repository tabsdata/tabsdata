#
# Copyright 2026 Tabs Data Inc.
#

"""End-to-end test of the subscriber command chain.

Walks every command in the subscriber chain except `prepare-py-env`,
asserting that each command's output composes correctly into the next
command's input. Uses the real `td_transporter` binary for the
transporter steps and the real `tabsdata-conn-localfile` destination
plugin; the only mock is a local HTTP server standing in for the
apiserver endpoint that `report-completion` POSTs to.

The chain executed:

  1.  localize-fn-bundle      — pulls + extracts the bundle.
  2.  prep-localize-state     — emits transporter YAML (pulls input
                                tables from cloud) + inputs + state
                                manifests.
  3.  run-transporter         — copies pre-staged input parquets
                                into in_dir.
                                [NO run_source_plugin, NO stamp_inputs]
  4.  run-function            — invokes the user subscriber; writes one
                                parquet per non-None output slot.
  5.  run-destination-plugin  — hands the parquets to the LocalFileDest
                                plugin, which ships them to the dest dir.
                                [NO prep_upload_tables, NO upload-tables
                                run_transporter]
  6.  prep-upload-state       — empty pairs (no system_output).
  7.  run-transporter         — no-op.
  8.  report-completion       — POSTs the response.yaml to a local
                                HTTP server.

Exercises multi-input cardinality (2 input slots → 2 destination
slots — different output cardinality on the dest plugin side).
Requires `td_transporter` on PATH.
"""

from __future__ import annotations

import http.server
import importlib
import socket
import sys
import threading
from pathlib import Path

import polars as pl
import pytest
import yaml

from tabsdatak.common._archive.bundle import build_bundle

pytestmark = [
    pytest.mark.unit,
    pytest.mark.tabsdatak,
    pytest.mark.skip(reason="Pending revision of all commands"),
]


# --- user code ------------------------------------------------------------

# 2-input / 2-output subscriber. Two source tables get shipped to two
# destination paths via LocalFileDest. Mixed slot modes
# (SINGLE / ARRAY) on the input side exercise the per-parameter mode
# inference.
_USER_SUBSCRIBER_SRC = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, subscriber
from tabsdatak.conn.localfile import LocalFileDestConn, LocalFileDest

_CONN = LocalFileDestConn(base_path={dest_base_path!r})
_DEST = LocalFileDest(
    paths=["out_a.parquet", "out_b.parquet"],
)


@subscriber(conn=_CONN, destination=_DEST, input_tables=["c1/t_a", "c1/t_b"])
def my_subscriber(
    a: TableFrameSpec, b: TableFramesSpec
) -> tuple[TableFrameSpec, TableFrameSpec]:
    # Both inputs ship verbatim. ARRAY-mode b just picks its first
    # element to make this test exercise the SINGLE/ARRAY mix on the
    # input side while keeping the output side simple.
    import polars as pl

    if b and b[0] is not None:
        b0 = b[0]
    else:
        b0 = None
    return (a, b0)
"""


# --- fixtures -------------------------------------------------------------


@pytest.fixture
def user_project(tmp_path, monkeypatch) -> Path:
    """Materialise a user project on disk and make it importable.

    The destination's `base_path` is captured at decoration time, so
    we bake the test-scoped dest dir into the source template before
    writing it.
    """
    dest_out = tmp_path / "dest_out"
    dest_out.mkdir()
    project = tmp_path / "project"
    pkg = project / "sub_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "subscriber.py").write_text(
        _USER_SUBSCRIBER_SRC.format(dest_base_path=str(dest_out))
    )
    monkeypatch.syspath_prepend(str(project))
    importlib.invalidate_caches()
    yield project
    for name in list(sys.modules):
        if name == "sub_pkg" or name.startswith("sub_pkg."):
            del sys.modules[name]


@pytest.fixture
def bundle_path(tmp_path, user_project) -> Path:
    from sub_pkg.subscriber import my_subscriber  # noqa: E402

    path = tmp_path / "bundle.tar.zst"
    build_bundle(my_subscriber, user_project, path)
    return path


@pytest.fixture
def staged_inputs(tmp_path) -> tuple[Path, Path]:
    """Pre-stage the two input parquets at file:// URIs the request
    will point at."""
    staged = tmp_path / "staged"
    staged.mkdir()
    a = staged / "t_a.parquet"
    b = staged / "t_b.parquet"
    pl.DataFrame({"id": [1, 2], "name": ["a1", "a2"]}).write_parquet(a)
    pl.DataFrame({"id": [10, 11, 12], "name": ["b1", "b2", "b3"]}).write_parquet(b)
    return a, b


@pytest.fixture
def dest_out(tmp_path) -> Path:
    """The dir LocalFileDest is configured to ship outputs to.
    Mirrors the path baked into the user project's @subscriber
    decoration (created by `user_project` fixture)."""
    return tmp_path / "dest_out"


@pytest.fixture
def mock_apiserver():
    """Tiny HTTP server in a thread. Yields (base_url, requests_log)."""
    requests_log: list[dict] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a, **kw):
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            requests_log.append({
                "path": self.path,
                "headers": dict(self.headers),
                "body": body,
            })
            self.send_response(200)
            self.end_headers()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", requests_log
    finally:
        server.shutdown()
        server.server_close()


# --- request.yaml ---------------------------------------------------------


def _write_request(path: Path, *, bundle_uri: str, a_uri: str, b_uri: str) -> Path:
    """request.yaml for a 2-input subscriber. `output` is empty —
    subscribers don't push to the catalog, the destination plugin
    handles the egress."""
    body = {
        "info": {
            "collection": "smoke",
            "collection_id": 1,
            "execution_id": 1,
            "transaction_id": 1,
            "function_run_id": 1,
            "function_bundle": {"uri": bundle_uri},
        },
        "input": [
            {"name": "t_a", "location": {"uri": a_uri}},
            {"name": "t_b", "location": {"uri": b_uri}},
        ],
        "output": [],
        "system_input": [],
        "system_output": [],
    }
    path.write_text(yaml.safe_dump(body))
    return path


# --- command invocation helper -------------------------------------------


def _invoke(
    monkeypatch, main_fn, argv: list[str], *, responder_url: str
) -> int:
    monkeypatch.setattr(
        sys,
        "argv",
        argv
        + [
            "--responder-url",
            responder_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    return main_fn()


# --- end-to-end test ------------------------------------------------------


def test_subscriber_full_chain(
    tmp_path,
    bundle_path,
    staged_inputs,
    dest_out,
    mock_apiserver,
    http_ok_url,
    monkeypatch,
):
    """Walk every step of the subscriber chain and verify the
    artifacts each command produces — including the parquets the
    destination plugin ships to its dest dir."""
    base_url, requests_log = mock_apiserver
    a_staged, b_staged = staged_inputs

    extracted = tmp_path / "extracted"
    in_dir = tmp_path / "in"
    state_dir = tmp_path / "state"
    fn_out_dir = tmp_path / "fn_out"
    in_dir.mkdir()
    state_dir.mkdir()

    inputs_manifest = tmp_path / "inputs_manifest.yaml"
    state_manifest = tmp_path / "state_manifest.yaml"
    outputs_manifest = tmp_path / "outputs_manifest.yaml"
    localize_state_req = tmp_path / "localize_state_req.yaml"
    upload_state_req = tmp_path / "upload_state_req.yaml"
    response_yaml = tmp_path / "response.yaml"

    request_yaml = _write_request(
        tmp_path / "request.yaml",
        bundle_uri=bundle_path.as_uri(),
        a_uri=a_staged.as_uri(),
        b_uri=b_staged.as_uri(),
    )

    # --- 1. localize-fn-bundle ------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.localize_fn_bundle import (
        main as localize_main,
    )

    rc = _invoke(
        monkeypatch,
        localize_main,
        [
            "localize-fn-bundle",
            "--request",
            str(request_yaml),
            "--bundle-dir",
            str(extracted),
            "--scratch-dir",
            str(tmp_path / "localize-scratch"),
            "--liveness",
            str(tmp_path / "liveness-localize"),
        ],
        responder_url=http_ok_url,
    )
    assert rc == 0
    assert (extracted / "function_def.json").is_file()

    # --- 2. prep-localize-state ----------------------------------------
    from tabsdatak.server._tdrunner.function.commands.prep_localize_state import (
        main as prep_loc_main,
    )

    rc = _invoke(
        monkeypatch,
        prep_loc_main,
        [
            "prep-localize-state",
            "--request",
            str(request_yaml),
            "--in-dir",
            str(in_dir),
            "--state-dir",
            str(state_dir),
            "--out",
            str(localize_state_req),
            "--inputs-manifest",
            str(inputs_manifest),
            "--state-manifest",
            str(state_manifest),
            "--liveness",
            str(tmp_path / "liveness-prep-loc-state"),
        ],
        responder_url=http_ok_url,
    )
    assert rc == 0

    # --- 3. run-transporter (localize) ---------------------------------
    from tabsdatak.server._tdrunner.function.commands.run_transporter import (
        main as run_transporter_main,
    )

    rc = _invoke(
        monkeypatch,
        run_transporter_main,
        [
            "run-transporter",
            "--request",
            str(localize_state_req),
            "--report",
            str(tmp_path / "transporter-report-loc.yaml"),
            "--liveness",
            str(tmp_path / "liveness-rt-loc"),
        ],
        responder_url=http_ok_url,
    )
    assert rc == 0
    assert (in_dir / "t_a.parquet").is_file()
    assert (in_dir / "t_b.parquet").is_file()

    # --- 4. run-function -----------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.run_function import (
        main as rf_main,
    )

    rc = _invoke(
        monkeypatch,
        rf_main,
        [
            "run-function",
            "--request",
            str(request_yaml),
            "--bundle-dir",
            str(extracted),
            "--inputs-manifest",
            str(inputs_manifest),
            "--out-dir",
            str(fn_out_dir),
            "--outputs-manifest",
            str(outputs_manifest),
            "--state-dir",
            str(state_dir),
            "--liveness",
            str(tmp_path / "liveness-fn"),
        ],
        responder_url=http_ok_url,
    )
    assert rc == 0

    # Two output parquets (one per subscriber output slot, named with
    # synthetic out_<i> labels — see `_output_table_names` in
    # run_function for the rationale).
    fn_parquets = sorted(fn_out_dir.rglob("*.parquet"))
    assert [p.name for p in fn_parquets] == ["out_0.parquet", "out_1.parquet"]
    outputs = yaml.safe_load(outputs_manifest.read_text())
    assert [e["name"] for e in outputs["outputs"]] == ["out_0", "out_1"]
    assert all(e["file"] is not None for e in outputs["outputs"])

    # --- 5. run-destination-plugin ------------------------------------
    from tabsdatak.server._tdrunner.function.commands.run_destination_plugin import (
        main as rdp_main,
    )

    rc = _invoke(
        monkeypatch,
        rdp_main,
        [
            "run-destination-plugin",
            "--request",
            str(request_yaml),
            "--bundle-dir",
            str(extracted),
            "--outputs-manifest",
            str(outputs_manifest),
            "--liveness",
            str(tmp_path / "liveness-rdp"),
        ],
        responder_url=http_ok_url,
    )
    assert rc == 0

    # LocalFileDest shipped both outputs into the dest dir, using the
    # destination's declared paths.
    shipped_a = dest_out / "out_a.parquet"
    shipped_b = dest_out / "out_b.parquet"
    assert shipped_a.is_file()
    assert shipped_b.is_file()

    # Content: a → 2 rows from t_a (1, 2); b → 3 rows from t_b
    # ([1] of ARRAY mode = the only file, which has 3 rows).
    df_a = pl.read_parquet(shipped_a)
    df_b = pl.read_parquet(shipped_b)
    assert df_a["id"].to_list() == [1, 2]
    assert df_b["id"].to_list() == [10, 11, 12]

    # --- 6. prep-upload-state ------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.prep_upload_state import (
        main as prep_up_state_main,
    )

    rc = _invoke(
        monkeypatch,
        prep_up_state_main,
        [
            "prep-upload-state",
            "--request",
            str(request_yaml),
            "--state-dir",
            str(state_dir),
            "--out",
            str(upload_state_req),
            "--liveness",
            str(tmp_path / "liveness-prep-up-state"),
        ],
        responder_url=http_ok_url,
    )
    assert rc == 0
    assert "source_target_pairs: []" in upload_state_req.read_text()

    # --- 7. run-transporter (upload-state, empty) ---------------------
    rc = _invoke(
        monkeypatch,
        run_transporter_main,
        [
            "run-transporter",
            "--request",
            str(upload_state_req),
            "--report",
            str(tmp_path / "transporter-report-up-state.yaml"),
            "--liveness",
            str(tmp_path / "liveness-rt-up-state"),
        ],
        responder_url=http_ok_url,
    )
    assert rc == 0

    # --- 8. report-completion ------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.report_completion import (
        main as report_main,
    )

    rc = _invoke(
        monkeypatch,
        report_main,
        [
            "report-completion",
            "--url",
            f"val:{base_url}/api/v1/done",
            "--authorization",
            "val:Bearer mock-bearer-token",
            "--request",
            str(request_yaml),
            "--outputs-manifest",
            str(outputs_manifest),
            "--state-dir",
            str(state_dir),
            "--out",
            str(response_yaml),
            "--liveness",
            str(tmp_path / "liveness-rc"),
        ],
        responder_url=http_ok_url,
    )
    assert rc == 0

    # For subscribers, request.output is empty → response.yaml's output
    # list is empty too. The destination plugin already reported its
    # own egress; the framework's response is just "function ran ok".
    response = yaml.safe_load(response_yaml.read_text())
    assert response == {"output": []}

    # Mock apiserver received the POST.
    assert len(requests_log) == 1
    posted = requests_log[0]
    assert posted["path"] == "/api/v1/done"
    assert posted["headers"]["Authorization"] == "Bearer mock-bearer-token"
