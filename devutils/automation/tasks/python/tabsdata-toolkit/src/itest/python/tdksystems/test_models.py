"""Integration tests: every bundled model can generate data end-to-end.

Strategy: spin up one shared `fs` system (no docker required) and, per model,
`tdx-domain create --model M --data xs`. Verify CSVs materialise in the domain
directory.

All artefacts land under `target/itest/<test-id>/`."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import data_gen

from conftest import assert_ok, run_cmd, ITEST_ROOT

pytestmark = pytest.mark.integration

# Only models that can target a `file` system belong in the fs-domain test.
# This excludes salesforce (kinds=['salesforce'], a manual cci/snowfakery
# workflow) and finance (kinds=['sql','kafka'], no CSV output).
_MODELS = [m for m in data_gen.list_models()
           if "file" in data_gen.model_kinds(m)]


@pytest.fixture(scope="module")
def fs_system_for_models():
    """One fs system shared across all model tests (cheaper than per-model).
    Returns the fs root and the per-suite system/domain store dirs."""
    base = ITEST_ROOT / "models_fs"
    if base.exists():
        import shutil
        shutil.rmtree(base)
    base.mkdir(parents=True)
    fs_root = base / "root"
    sysdir, domdir = base / "systems", base / "domains"
    cp = run_cmd("tdx-system", "create", "--type", "fs", "--sys", "models_fs",
                 "--sys-dir", str(sysdir), "--sys-path", str(fs_root), "--force")
    assert_ok(cp, "tdx-system create fs (shared)")
    return fs_root, sysdir, domdir


@pytest.mark.parametrize("model", _MODELS)
def test_model_loads_into_fs_domain(model, fs_system_for_models):
    fs_root, sysdir, domdir = fs_system_for_models
    # create-domain writes CSVs (data create is a no-op for fs; --data loads).
    cp = run_cmd("tdx-domain", "create", "--sys", "models_fs", "--sys-dir", str(sysdir),
                 "--domain", model, "--domain-dir", str(domdir),
                 "--model", model, "--data", "xs", "--force")
    assert_ok(cp, f"tdx-domain create + data model={model}")

    dom_root = fs_root / model
    csvs = [p for p in dom_root.rglob("*.csv") if p.is_file()]
    assert csvs, f"model {model!r}: no .csv files materialised"
