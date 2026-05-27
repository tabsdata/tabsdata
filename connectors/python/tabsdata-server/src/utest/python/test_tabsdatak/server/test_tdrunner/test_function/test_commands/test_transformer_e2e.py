#
# Copyright 2026 Tabs Data Inc.
#

"""End-to-end test of the transformer command chain.

Walks every command in the transformer chain except `prepare-py-env`,
asserting that each command's output composes correctly into the next
command's input. Uses the real `td_transporter` binary for the three
transporter steps; the only mock is a local HTTP server standing in
for the apiserver endpoint that `report-completion` POSTs to.

The chain executed:

  1.  localize-fn-bundle   — pulls + extracts the bundle.
  2.  prep-localize-state  — emits transporter YAML (pulls input
                             tables from cloud) + inputs manifest.
  3.  run-transporter      — copies pre-staged input parquets to in_dir.
                             [NO run_source_plugin, NO stamp_inputs]
  4.  run-function         — invokes the user transformer; writes one
                             parquet per non-None output slot.
  5.  prep-upload-tables   — builds the upload transporter request,
                             skipping any `file: null` slot.
  6.  run-transporter      — uploads the modified table to its
                             apiserver-allocated file:// location.
  7.  prep-upload-state    — empty pairs (no system_output).
  8.  run-transporter      — no-op.
  9.  report-completion    — POSTs the response.yaml to a local HTTP
                             server.

Exercises multi-input cardinality (2 input slots) to guard against the
regression where input_count was hardcoded for publishers.

Mirrors `test_publisher_e2e.py`. Requires `td_transporter` on PATH.
"""

from __future__ import annotations

import http.server
import importlib
import socket
import sys
import threading
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
import yaml

from tabsdatak._bundle import build_bundle

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- user code ------------------------------------------------------------


_USER_TRANSFORMER_SRC = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, transformer


@transformer(
    input_tables=["c1/t_a", "c1/t_b"],
    output_tables=["merged", "untouched"],
)
def my_transformer(
    a: TableFrameSpec, b: TableFrameSpec
) -> tuple[TableFrameSpec, TableFrameSpec]:
    # Vertically concatenate the two input tables, returning the result
    # as `merged`. Return None for `untouched` to also exercise the
    # `file: null` outputs-manifest path.
    import polars as pl

    if a is None and b is None:
        return (None, None)
    frames = [f for f in (a, b) if f is not None]
    return (pl.concat(frames, how="vertical_relaxed"), None)
"""


# --- fixtures -------------------------------------------------------------


@pytest.fixture
def user_project(tmp_path, monkeypatch) -> Path:
    """Materialise a user project on disk and make it importable."""
    project = tmp_path / "project"
    pkg = project / "tx_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "transformer.py").write_text(_USER_TRANSFORMER_SRC)
    monkeypatch.syspath_prepend(str(project))
    importlib.invalidate_caches()
    yield project
    for name in list(sys.modules):
        if name == "tx_pkg" or name.startswith("tx_pkg."):
            del sys.modules[name]


@pytest.fixture
def bundle_path(tmp_path, user_project) -> Path:
    """Build the bundle tarball. Not extracted — `localize-fn-bundle`
    extracts it during the test."""
    from tx_pkg.transformer import my_transformer  # noqa: E402  (lazy)

    path = tmp_path / "bundle.tar.zst"
    build_bundle(my_transformer, user_project, path)
    return path


@pytest.fixture
def staged_inputs(tmp_path) -> tuple[Path, Path]:
    """Pre-stage the two input parquets at file:// URIs the request
    will point at. Simulates the catalog-resident tables produced by
    an upstream publisher run."""
    staged = tmp_path / "staged"
    staged.mkdir()
    a = staged / "t_a.parquet"
    b = staged / "t_b.parquet"
    pl.DataFrame({"id": [1, 2], "name": ["a1", "a2"]}).write_parquet(a)
    pl.DataFrame({"id": [10, 11], "name": ["b1", "b2"]}).write_parquet(b)
    return a, b


# --- different-cardinality transformer (3 in → 1 out) --------------------

# A second transformer used by the different-cardinality e2e test.
# 3 input_tables (mixed SINGLE / ARRAY / SINGLE modes) and 1 output.
# Tests that input cardinality (3) ≠ output cardinality (1) round-trips
# correctly through the whole chain.
_USER_TRANSFORMER_DIFFERENT_CARDINALITY_SRC = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, transformer


@transformer(
    input_tables=["c1/t_a", "c1/t_b", "c1/t_c"],
    output_tables=["merged"],
)
def my_transformer(
    a: TableFrameSpec, b: TableFramesSpec, c: TableFrameSpec
) -> tuple[TableFrameSpec]:
    import polars as pl

    frames = []
    if a is not None:
        frames.append(a)
    if b:
        frames.extend(f for f in b if f is not None)
    if c is not None:
        frames.append(c)
    if not frames:
        return (None,)
    return (pl.concat(frames, how="vertical_relaxed"),)
"""


@pytest.fixture
def diff_card_user_project(tmp_path, monkeypatch) -> Path:
    project = tmp_path / "diff_project"
    pkg = project / "diff_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "transformer.py").write_text(_USER_TRANSFORMER_DIFFERENT_CARDINALITY_SRC)
    monkeypatch.syspath_prepend(str(project))
    importlib.invalidate_caches()
    yield project
    for name in list(sys.modules):
        if name == "diff_pkg" or name.startswith("diff_pkg."):
            del sys.modules[name]


@pytest.fixture
def diff_card_bundle_path(tmp_path, diff_card_user_project) -> Path:
    from diff_pkg.transformer import my_transformer  # noqa: E402

    path = tmp_path / "diff_bundle.tar.zst"
    build_bundle(my_transformer, diff_card_user_project, path)
    return path


@pytest.fixture
def diff_card_staged_inputs(tmp_path) -> tuple[Path, Path, Path]:
    """Three staged parquets for the 3-input transformer."""
    staged = tmp_path / "diff_staged"
    staged.mkdir()
    a = staged / "t_a.parquet"
    b = staged / "t_b.parquet"
    c = staged / "t_c.parquet"
    pl.DataFrame({"id": [1], "name": ["a"]}).write_parquet(a)
    pl.DataFrame({"id": [2, 3], "name": ["b1", "b2"]}).write_parquet(b)
    pl.DataFrame({"id": [4], "name": ["c"]}).write_parquet(c)
    return a, b, c


@pytest.fixture
def mock_apiserver():
    """Tiny HTTP server in a thread. Yields (base_url, requests_log)."""
    requests_log: list[dict] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a, **kw):  # silence stderr in tests
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


def _write_request(
    path: Path,
    *,
    bundle_uri: str,
    a_uri: str,
    b_uri: str,
    uploaded_dir: Path,
) -> Path:
    """Write a request.yaml for a 2-input / 2-output transformer."""
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
        "output": [
            {
                "name": "merged",
                "location": {"uri": (uploaded_dir / "merged.parquet").as_uri()},
            },
            {
                "name": "untouched",
                "location": {
                    "uri": (uploaded_dir / "untouched.parquet").as_uri(),
                },
            },
        ],
        "system_input": [],
        "system_output": [],
    }
    path.write_text(yaml.safe_dump(body))
    return path


# --- command invocation helper -------------------------------------------


def _invoke(monkeypatch, main_fn, argv: list[str]) -> int:
    """Set sys.argv and call a command's main(). Restore argv on exit."""
    monkeypatch.setattr(sys, "argv", argv)
    return main_fn()


# --- end-to-end test ------------------------------------------------------


def test_transformer_full_chain(
    tmp_path, bundle_path, staged_inputs, mock_apiserver, monkeypatch
):
    """Walk every step of the transformer chain and verify the
    artifacts each command produces."""
    base_url, requests_log = mock_apiserver
    a_staged, b_staged = staged_inputs

    # Workspace layout.
    extracted = tmp_path / "extracted"
    in_dir = tmp_path / "in"
    state_dir = tmp_path / "state"
    fn_out_dir = tmp_path / "fn_out"
    uploaded_dir = tmp_path / "uploaded"
    in_dir.mkdir()
    state_dir.mkdir()
    uploaded_dir.mkdir()

    inputs_manifest = tmp_path / "inputs_manifest.yaml"
    state_manifest = tmp_path / "state_manifest.yaml"
    outputs_manifest = tmp_path / "outputs_manifest.yaml"
    localize_state_req = tmp_path / "localize_state_req.yaml"
    upload_tables_req = tmp_path / "upload_tables_req.yaml"
    upload_state_req = tmp_path / "upload_state_req.yaml"
    response_yaml = tmp_path / "response.yaml"

    request_yaml = _write_request(
        tmp_path / "request.yaml",
        bundle_uri=bundle_path.as_uri(),
        a_uri=a_staged.as_uri(),
        b_uri=b_staged.as_uri(),
        uploaded_dir=uploaded_dir,
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
    )
    assert rc == 0
    assert (extracted / "function_def.json").is_file()
    assert (extracted / "user_code" / "tx_pkg" / "transformer.py").is_file()

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
    )
    assert rc == 0
    # Two CopyPairs (one per input table), no system_input.
    localize_text = localize_state_req.read_text()
    assert localize_text.count("- - !LocalFile") == 2
    assert a_staged.name in localize_text
    assert b_staged.name in localize_text
    # Inputs manifest names both input tables; files point at in_dir.
    manifest_data = yaml.safe_load(inputs_manifest.read_text())
    assert [e["name"] for e in manifest_data["inputs"]] == ["t_a", "t_b"]
    assert manifest_data["inputs"][0]["files"] == [
        str((in_dir / "t_a.parquet").absolute())
    ]
    assert manifest_data["inputs"][1]["files"] == [
        str((in_dir / "t_b.parquet").absolute())
    ]
    # State manifest is empty (request.system_input is empty for this run).
    assert yaml.safe_load(state_manifest.read_text()) == {"state": []}

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
    )
    assert rc == 0
    # Both inputs landed where the manifest says.
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
            "--liveness",
            str(tmp_path / "liveness-fn"),
        ],
    )
    assert rc == 0

    # The user function returned (df, None); only `merged` parquet exists.
    fn_parquets = list(fn_out_dir.rglob("*.parquet"))
    assert len(fn_parquets) == 1
    assert fn_parquets[0].name == "merged.parquet"

    outputs = yaml.safe_load(outputs_manifest.read_text())
    by_name = {e["name"]: e for e in outputs["outputs"]}
    assert set(by_name) == {"merged", "untouched"}
    assert by_name["merged"]["file"] is not None
    assert Path(by_name["merged"]["file"]).samefile(fn_parquets[0])
    assert by_name["untouched"]["file"] is None

    # Concatenated content (2 + 2 rows from each input).
    df = pq.read_table(fn_parquets[0]).to_pydict()
    assert sorted(df["id"]) == [1, 2, 10, 11]

    # --- 5. prep-upload-tables -----------------------------------------
    from tabsdatak.server._tdrunner.function.commands.prep_upload_tables import (
        main as prep_up_tables_main,
    )

    rc = _invoke(
        monkeypatch,
        prep_up_tables_main,
        [
            "prep-upload-tables",
            "--request",
            str(request_yaml),
            "--outputs-manifest",
            str(outputs_manifest),
            "--out",
            str(upload_tables_req),
            "--liveness",
            str(tmp_path / "liveness-prep-up-tbl"),
        ],
    )
    assert rc == 0
    upload_text = upload_tables_req.read_text()
    # One CopyPair — merged. Untouched's file:null is skipped.
    assert upload_text.count("- - !LocalFile") == 1
    assert "merged.parquet" in upload_text
    assert "untouched.parquet" not in upload_text

    # --- 6. run-transporter (upload-tables) ----------------------------
    rc = _invoke(
        monkeypatch,
        run_transporter_main,
        [
            "run-transporter",
            "--request",
            str(upload_tables_req),
            "--report",
            str(tmp_path / "transporter-report-up-tbl.yaml"),
            "--liveness",
            str(tmp_path / "liveness-rt-up-tbl"),
        ],
    )
    assert rc == 0
    uploaded = uploaded_dir / "merged.parquet"
    assert uploaded.is_file()
    assert not (uploaded_dir / "untouched.parquet").exists()
    assert uploaded.read_bytes() == fn_parquets[0].read_bytes()

    # --- 7. prep-upload-state ------------------------------------------
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
    )
    assert rc == 0
    assert "source_target_pairs: []" in upload_state_req.read_text()

    # --- 8. run-transporter (upload-state, empty) ----------------------
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
    )
    assert rc == 0

    # --- 9. report-completion ------------------------------------------
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
            "--token",
            "val:mock-bearer-token",
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
    )
    assert rc == 0

    # response.yaml: merged has Data, untouched has NoData.
    response = yaml.safe_load(response_yaml.read_text())
    entries = {e["table"]: e for e in response["output"]}
    assert set(entries) == {"merged", "untouched"}
    assert "info" in entries["merged"]
    assert entries["merged"]["info"]["row_count"] == 4
    assert entries["merged"]["info"]["schema_hash"].startswith("sha256:")
    assert "info" not in entries["untouched"]
    assert set(entries["untouched"]) == {"table"}

    # Apiserver received exactly one POST with the local body.
    assert len(requests_log) == 1
    posted = requests_log[0]
    assert posted["path"] == "/api/v1/done"
    assert posted["headers"]["Authorization"] == "Bearer mock-bearer-token"
    assert posted["headers"]["Content-Type"] == "application/x-yaml"
    assert posted["body"] == response_yaml.read_bytes()


# --- different-cardinality transformer test ----------------------------


def _write_diff_card_request(
    path: Path,
    *,
    bundle_uri: str,
    a_uri: str,
    b_uri: str,
    c_uri: str,
    uploaded_dir: Path,
) -> Path:
    """request.yaml for the 3-in / 1-out transformer."""
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
            {"name": "t_c", "location": {"uri": c_uri}},
        ],
        "output": [
            {
                "name": "merged",
                "location": {
                    "uri": (uploaded_dir / "merged.parquet").as_uri(),
                },
            },
        ],
        "system_input": [],
        "system_output": [],
    }
    path.write_text(yaml.safe_dump(body))
    return path


def test_transformer_full_chain_different_cardinalities(
    tmp_path,
    diff_card_bundle_path,
    diff_card_staged_inputs,
    mock_apiserver,
    monkeypatch,
):
    """End-to-end with a transformer whose input cardinality (3) is
    different from its output cardinality (1). Also mixes SINGLE and
    ARRAY slot modes in the same signature."""
    base_url, requests_log = mock_apiserver
    a_staged, b_staged, c_staged = diff_card_staged_inputs

    extracted = tmp_path / "extracted"
    in_dir = tmp_path / "in"
    state_dir = tmp_path / "state"
    fn_out_dir = tmp_path / "fn_out"
    uploaded_dir = tmp_path / "uploaded"
    in_dir.mkdir()
    state_dir.mkdir()
    uploaded_dir.mkdir()

    inputs_manifest = tmp_path / "inputs_manifest.yaml"
    state_manifest = tmp_path / "state_manifest.yaml"
    outputs_manifest = tmp_path / "outputs_manifest.yaml"
    localize_state_req = tmp_path / "localize_state_req.yaml"
    upload_tables_req = tmp_path / "upload_tables_req.yaml"
    upload_state_req = tmp_path / "upload_state_req.yaml"
    response_yaml = tmp_path / "response.yaml"

    request_yaml = _write_diff_card_request(
        tmp_path / "request.yaml",
        bundle_uri=diff_card_bundle_path.as_uri(),
        a_uri=a_staged.as_uri(),
        b_uri=b_staged.as_uri(),
        c_uri=c_staged.as_uri(),
        uploaded_dir=uploaded_dir,
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
    )
    assert rc == 0

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
    )
    assert rc == 0
    # Inputs manifest has 3 entries (one per input table).
    manifest_data = yaml.safe_load(inputs_manifest.read_text())
    assert [e["name"] for e in manifest_data["inputs"]] == [
        "t_a",
        "t_b",
        "t_c",
    ]

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
    )
    assert rc == 0
    assert (in_dir / "t_a.parquet").is_file()
    assert (in_dir / "t_b.parquet").is_file()
    assert (in_dir / "t_c.parquet").is_file()

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
            "--liveness",
            str(tmp_path / "liveness-fn"),
        ],
    )
    assert rc == 0

    # Exactly one parquet (single output) — input cardinality 3, output 1.
    fn_parquets = sorted(fn_out_dir.rglob("*.parquet"))
    assert len(fn_parquets) == 1
    assert fn_parquets[0].name == "merged.parquet"

    outputs = yaml.safe_load(outputs_manifest.read_text())
    assert [e["name"] for e in outputs["outputs"]] == ["merged"]
    assert outputs["outputs"][0]["file"] is not None

    # Content: 1 + 2 + 1 = 4 rows across all three inputs.
    df = pl.read_parquet(outputs["outputs"][0]["file"])
    assert df.height == 4
    assert sorted(df["id"].to_list()) == [1, 2, 3, 4]

    # --- 5. prep-upload-tables -----------------------------------------
    from tabsdatak.server._tdrunner.function.commands.prep_upload_tables import (
        main as prep_up_tables_main,
    )

    rc = _invoke(
        monkeypatch,
        prep_up_tables_main,
        [
            "prep-upload-tables",
            "--request",
            str(request_yaml),
            "--outputs-manifest",
            str(outputs_manifest),
            "--out",
            str(upload_tables_req),
            "--liveness",
            str(tmp_path / "liveness-prep-up-tbl"),
        ],
    )
    assert rc == 0
    # Exactly one CopyPair — single output.
    assert upload_tables_req.read_text().count("- - !LocalFile") == 1

    # --- 6. run-transporter (upload-tables) ----------------------------
    rc = _invoke(
        monkeypatch,
        run_transporter_main,
        [
            "run-transporter",
            "--request",
            str(upload_tables_req),
            "--report",
            str(tmp_path / "transporter-report-up-tbl.yaml"),
            "--liveness",
            str(tmp_path / "liveness-rt-up-tbl"),
        ],
    )
    assert rc == 0
    assert (uploaded_dir / "merged.parquet").is_file()

    # --- 7. prep-upload-state ------------------------------------------
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
    )
    assert rc == 0

    # --- 8. run-transporter (upload-state, empty) ----------------------
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
    )
    assert rc == 0

    # --- 9. report-completion ------------------------------------------
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
            "--token",
            "val:mock-bearer-token",
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
    )
    assert rc == 0

    response = yaml.safe_load(response_yaml.read_text())
    entries = {e["table"]: e for e in response["output"]}
    assert set(entries) == {"merged"}
    assert entries["merged"]["info"]["row_count"] == 4

    assert len(requests_log) == 1
