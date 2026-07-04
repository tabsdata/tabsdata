"""Self-managing uv virtual environments for tdx-system / tdx-domain.

The commands run their real work inside dedicated uv venvs under
``~/.uv/envs``, so they never depend on — or pollute — whatever environment
happens to be active when they are invoked:

  ``~/.uv/envs/tdx-system-default``
      The main worker env: the ``tabsdatax.itest.tdksystems`` package plus the ``main``
      extra (all the SQL / cloud / kafka / saas drivers). Every command except
      the salesforce ones re-execs into this env.

  ``~/.uv/envs/tdx-system-salesforce``
      The package plus the ``salesforce`` extra (cumulusci-plus + snowfakery).
      Kept apart because cumulusci-plus pins ``sqlalchemy<2``, which conflicts
      with the ``main`` extra (``clickhouse-sqlalchemy`` needs ``>=2``).
      Because the package itself has no base deps, it coexists with the
      salesforce toolchain here without conflict. ``--model salesforce``
      commands re-exec into this env; created on first such use.

Both venvs are created — and their packages installed — on first use; delete
them (``rm -rf ~/.uv/envs/tdx-system-*``) to force a clean reinstall.

Set ``TD_GEN_NO_BOOTSTRAP=1`` to disable all of this and run in the current
interpreter (e.g. for tests, or when you manage the environment yourself).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from .errors import DatagenError

ENVS_DIR = Path.home() / ".uv" / "envs"
MAIN_VENV = ENVS_DIR / "tdx-system-default"
SALESFORCE_VENV = ENVS_DIR / "tdx-system-salesforce"

_MARKER = ".tdx-system-ready"      # written once an install completes
_DISABLE = "TD_GEN_NO_BOOTSTRAP"   # escape hatch: run in the current interpreter


def _disabled() -> bool:
    return bool(os.environ.get(_DISABLE))


def _uv() -> str:
    uv = shutil.which("uv")
    if not uv:
        raise DatagenError(
            "`uv` is required to manage the datagen venvs but is not on PATH.\n"
            "Install it from https://docs.astral.sh/uv/ (e.g. `brew install uv`),\n"
            "or set TD_GEN_NO_BOOTSTRAP=1 to run in the current interpreter."
        )
    return uv


def _venv_python(venv: Path) -> Path:
    return venv / ("Scripts" if os.name == "nt" else "bin") / "python"


def _project_dir() -> Path:
    """Directory holding the datagen ``pyproject.toml`` — found by walking up
    from this package's source, so the managed venv installs the same editable
    tree the launcher was run from."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    raise DatagenError(
        "cannot locate the datagen project (no pyproject.toml above "
        f"{__file__}); install datagen editable (`pip install -e datagen`) "
        "or run from a source checkout."
    )


def _run(cmd: list[str], *, what: str) -> None:
    # Inherit stdout/stderr so the user sees uv's progress on the (slow) first
    # install; raise a clean error if it fails.
    if subprocess.run(cmd).returncode != 0:
        raise DatagenError(f"{what} failed: {' '.join(cmd)}")


def _in_venv(venv: Path) -> bool:
    try:
        return Path(sys.prefix).resolve() == venv.resolve()
    except OSError:
        return False


def _ensure(venv: Path, *, editable_extra: str | None,
            packages: tuple[str, ...] = ()) -> Path:
    """Create ``venv`` (if absent) and install into it, idempotently. A marker
    file records a completed install so later runs skip straight through."""
    if (venv / _MARKER).is_file():
        return venv
    uv = _uv()
    label = venv.name
    print(f">>> setting up uv venv '{label}' at {venv} (one-time) ...",
          file=sys.stderr)
    venv.parent.mkdir(parents=True, exist_ok=True)
    _run([uv, "venv", str(venv)], what=f"creating venv '{label}'")
    py = str(_venv_python(venv))
    if editable_extra is not None:
        proj = _project_dir()
        spec = f"{proj}[{editable_extra}]" if editable_extra else str(proj)
        _run([uv, "pip", "install", "--python", py, "-e", spec],
             what=f"installing datagen into '{label}'")
    if packages:
        _run([uv, "pip", "install", "--python", py, *packages],
             what=f"installing {', '.join(packages)} into '{label}'")
    (venv / _MARKER).write_text(
        "tdx-system managed venv — delete this directory to force a "
        "clean reinstall\n"
    )
    print(f">>> '{label}' ready", file=sys.stderr)
    return venv


def ensure_main_venv() -> Path:
    """Create/populate the main worker venv (td_gen + the `main` extra)."""
    return _ensure(MAIN_VENV, editable_extra="main")


def ensure_salesforce_venv() -> Path:
    """Create/populate the salesforce venv (td_gen + the `salesforce` extra)."""
    return _ensure(SALESFORCE_VENV, editable_extra="salesforce")


def _wants_salesforce(argv: list[str]) -> bool:
    """Whether this command targets the salesforce model (so it needs the
    cumulusci/snowfakery venv). Only `data create/load --model salesforce`
    carries `--model salesforce`; system/domain creation and the rest run in
    the main venv."""
    for i, tok in enumerate(argv):
        if tok == "--model=salesforce":
            return True
        if tok == "--model" and i + 1 < len(argv) and argv[i + 1] == "salesforce":
            return True
    return False


def target_venv(argv: list[str]) -> Path:
    return SALESFORCE_VENV if _wants_salesforce(argv) else MAIN_VENV


def bootstrap(module: str = "tabsdatax.itest.tdksystems.cli") -> None:
    """Re-exec ``module`` inside the venv this command needs, creating it on
    first use. ``--model salesforce`` routes to the salesforce venv; everything
    else to the main venv. ``module`` is the entry module to re-exec — the
    default suits ``tdx-system`` / ``tdx-domain`` (each passes its own module).

    A no-op when bootstrapping is disabled (``TD_GEN_NO_BOOTSTRAP``), when
    already inside the target venv, or for ``delete-venvs`` — which must run in
    the launcher interpreter, since it can't delete the venv it is running in.

    "Already inside" is decided solely by ``sys.prefix`` — never an inherited
    env var: a `tdx-system` / `tdx-domain` that another process spawns
    runs in the launcher interpreter and MUST re-bootstrap into the venv
    itself, not assume the parent already did. ``sys.prefix`` is also loop-safe
    — once re-exec'd, the venv interpreter reports the venv prefix, so we
    stop."""
    if _disabled():
        return
    argv = sys.argv[1:]
    if argv and argv[0] == "delete-venvs":
        return
    venv = target_venv(argv)
    if _in_venv(venv):
        return
    if venv == SALESFORCE_VENV:
        ensure_salesforce_venv()
    else:
        ensure_main_venv()
    py = _venv_python(venv)
    os.execv(str(py), [str(py), "-m", module, *argv])


def delete_venvs() -> int:
    """Remove both managed venvs (the `delete-venvs` subcommand)."""
    removed = 0
    for venv in (MAIN_VENV, SALESFORCE_VENV):
        if venv.exists():
            shutil.rmtree(venv)
            print(f">>> removed {venv}")
            removed += 1
        else:
            print(f">>> (absent) {venv}")
    print()
    if removed:
        print(">>> done — the next `tdx-system` run recreates and reinstalls "
              "the default venv\n    (the salesforce venv is recreated when a "
              "salesforce command needs it).")
    else:
        print(">>> nothing to delete.")
    return 0
