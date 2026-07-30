#
# Copyright 2026 Tabsdata Inc.
#

"""Self-managing uv virtual environments for tdx-system / tdx-domain.

Every command re-execs into one of three dedicated uv venvs under
``~/.uv/envs``, so the tools never depend on — or pollute — whatever environment
happens to be active when they are invoked. Each venv holds the toolkit
(editable) plus the packages named by its ``env-defs/<env>.requirements``:

  ``_tdx-sys-a``
      The driver set: SQL engines, object stores, kafka, frame writer. Anything
      that reaches *inside* a system runs here.

  ``_tdx-sys-b``
      The salesforce data plane (cumulusci-plus + snowfakery). Kept apart
      because cumulusci-plus pins ``sqlalchemy<2``, which the driver set's
      dialects contradict — ``databricks-sqlalchemy`` needs ``>=2.0.21``.

  ``_tdx-sys-c``
      Driver-free — the toolkit's own dependencies only. Store reads and
      outside-the-system docker work run here, so creating a container never
      installs a database driver.

Which env a command needs is decided from raw argv *before* anything is
imported: a system declares ``sys_env`` (verbs that only touch its outside:
create / destroy / status / restart / fetch-bin) and ``dom_env`` (verbs that
reach inside it: every domain verb, plus ``purge``); a model may override the
domain env with ``models/<model>/model.json``. Reading either costs nothing —
backends defer their third-party imports into method bodies, so the launcher can
read a backend class with no drivers installed.

Both the venv and its packages are created on first use; ``tdx-system
clear-venvs`` removes them all. A venv is rebuilt only when it is missing or its
install did not complete (no marker): a fully-installed venv is always reused.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

from tabsdatax.itest.tdksystems2 import constants
from tabsdatax.itest.tdksystems2.errors import TdxError

ENV_A = "tdx-sys-a"
ENV_B = "tdx-sys-b"
ENV_C = "tdx-sys-c"
ENVS = (ENV_A, ENV_B, ENV_C)

ENVS_DIR = Path.home() / ".uv" / "envs"
ENV_DEFS_DIR = Path(__file__).resolve().parent / "env-defs"

_MARKER = ".tdx-sys-ready"  # written once an install completes

# System-tool verbs that only touch a system from the outside; every other
# delegated verb (`purge` included) reaches inside it and uses `dom_env`.
_SYS_PLANE_VERBS = frozenset({
    constants.V_CREATE,
    constants.V_DESTROY,
    constants.V_STATUS,
    constants.V_RESTART,
    constants.V_FETCH_BIN,
    constants.V_CONNECT,
})

# Verbs whose env a model may override (the data plane).
_MODEL_VERBS = frozenset({
    constants.V_CREATE_MODEL,
    constants.V_CREATE_DATA,
    constants.V_MODIFY_DATA,
})

# Verbs handled inside the tool itself (store reads, docker-free): see
# ``tdx._COMMON``. They never touch a system, so they need no drivers.
_COMMON_VERBS = frozenset({
    constants.V_TYPES,
    constants.V_LIST,
    constants.V_INFO,
    constants.V_MANIFEST,
    constants.V_SECRETS,
})


def venv_name(env: str) -> str:
    """The venv's directory name — the env name with the managed-venv prefix.
    What the user sees and types (``rm -rf ~/.uv/envs/_tdx-sys-a``); the bare env
    name only ever names the requirements file."""
    return f"_{env}"


def venv_path(env: str) -> Path:
    return ENVS_DIR / venv_name(env)


def requirements_file(env: str) -> Path:
    return ENV_DEFS_DIR / f"{env}.requirements"


def _uv() -> str:
    uv = shutil.which("uv")
    if not uv:
        raise TdxError(
            "`uv` is required to manage the tdx venvs but is not on PATH.\n"
            "Install it from https://docs.astral.sh/uv/ (e.g. `brew install uv`)."
        )
    return uv


def _venv_python(venv: Path) -> Path:
    paths = sysconfig.get_paths(
        scheme="venv",
        vars={"base": str(venv), "platbase": str(venv)},
    )
    return Path(paths["scripts"]) / ("python" + sysconfig.get_config_var("EXE"))


def _project_dir() -> Path:
    """Directory holding the toolkit ``pyproject.toml`` — found by walking up
    from this package's source, so the managed venv installs the same editable
    tree the launcher was run from."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    raise TdxError(
        "cannot locate the tabsdata-toolkit project (no pyproject.toml above "
        f"{__file__}); install it editable (`pip install -e .`) "
        "or run from a source checkout."
    )


def _run(cmd: list[str], *, what: str) -> None:
    # Inherit stdout/stderr so the user sees uv's progress on the (slow) first
    # install; raise a clean error if it fails.
    if subprocess.run(cmd).returncode != 0:
        raise TdxError(f"{what} failed: {' '.join(cmd)}")


def _in_venv(venv: Path) -> bool:
    try:
        return Path(sys.prefix).resolve() == venv.resolve()
    except OSError:
        return False


def _has_requirements(path: Path) -> bool:
    if not path.is_file():
        return False
    return any(
        line.strip() and not line.lstrip().startswith("#")
        for line in path.read_text(encoding="utf-8").splitlines()
    )


def ensure(env: str) -> Path:
    """Create ``env``'s venv (if absent) and install into it, idempotently. A
    marker file records a completed install so later runs skip straight through;
    a venv that exists without the marker (a first install that was interrupted)
    is rebuilt from scratch via ``uv venv --clear`` rather than left half-built."""
    venv = venv_path(env)
    if (venv / _MARKER).is_file():
        return venv
    uv = _uv()
    label = venv.name
    print(f">>> setting up uv venv '{label}' at {venv} (one-time) ...", file=sys.stderr)
    venv.parent.mkdir(parents=True, exist_ok=True)
    _run(
        [uv, "venv", "--clear", "--python", sys.executable, str(venv)],
        what=f"creating venv '{label}'",
    )
    py = str(_venv_python(venv))
    _run(
        [uv, "pip", "install", "--python", py, "-e", str(_project_dir())],
        what=f"installing tabsdata-toolkit into '{label}'",
    )
    reqs = requirements_file(env)
    if _has_requirements(reqs):
        _run(
            [uv, "pip", "install", "--python", py, "-r", str(reqs)],
            what=f"installing {reqs.name} into '{label}'",
        )
    (venv / _MARKER).write_text(
        "tdx managed venv — delete this directory to force a clean reinstall\n",
        encoding="utf-8",
    )
    print(f">>> '{label}' ready", file=sys.stderr)
    return venv


def env_for_system(slug: str, *, inside: bool) -> str:
    """The env `slug` declares for the outside (``sys_env``) or the inside
    (``dom_env``) plane."""
    from tabsdatax.itest.tdksystems2 import systems

    cls = systems.get_class(slug)
    return getattr(cls, "dom_env" if inside else "sys_env")


def env_for_model(model: str) -> str | None:
    """The env `model` demands for the data plane, from its optional
    ``models/<model>/model.json`` (``{"env": "tdx-sys-b"}``); ``None`` when the
    model is content with the system's ``dom_env``."""
    path = Path(__file__).resolve().parent / "models" / model / "model.json"
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding="utf-8")).get("env")
    return str(value) if value else None


def _slug_and_model(tool: str, verb: str, rest: list[str]) -> tuple[str, str | None]:
    """The system slug a verb targets, and the model in play (data-plane verbs
    only). Mirrors the target selection ``tdx._resolve`` does after argparse."""
    from tabsdatax.itest.tdksystems2 import cli, descriptor, store

    sys_dir = cli.peek(rest, constants.OPT_SYS_DIR)
    dom_dir = cli.peek(rest, constants.OPT_DOMAIN_DIR)

    if tool == constants.SYS_TOOL:
        if verb in (constants.V_CREATE, constants.V_FETCH_BIN):
            return cli.peek(rest, constants.OPT_TYPE) or "", None
        name = cli.peek(rest, constants.OPT_SYS) or ""
        info = descriptor.load(store.system_path(name, sys_dir))
        return info.system, None

    if verb == constants.V_CREATE:
        name = cli.peek(rest, constants.OPT_SYS) or ""
        info = descriptor.load(store.system_path(name, sys_dir))
        return info.system, None

    name = cli.peek(rest, constants.OPT_DOMAIN) or ""
    info = descriptor.load(store.domain_path(name, dom_dir))
    model = cli.peek(rest, constants.OPT_MODEL) or getattr(info, "model", None)
    return info.system, model


def target_env(tool: str, argv: list[str]) -> str | None:
    """The env this invocation needs, or ``None`` to stay in the launcher.

    ``None`` covers the cases that must not re-exec: top-level help (it has to
    print instantly, without building a venv), ``clear-venvs`` (it cannot delete
    the venv it is running in), and a batch parent (a pure orchestrator that
    spawns one child per target — each child bootstraps itself, so re-execing
    here would hand them all the wrong interpreter).

    Anything whose target cannot be resolved falls back to the driver-free env:
    an unknown system, a missing descriptor or an unset crypto key all fail
    before a driver is ever needed, and the real command reports them far better
    than this router could."""
    from tabsdatax.itest.tdksystems2 import batch

    if not argv or argv[0] in constants.HELP_FLAGS:
        return None
    verb, rest = argv[0], argv[1:]
    if verb == constants.V_CLEAR_VENVS:
        return None
    if verb == constants.V_CONNECT:
        # It shells out to ssh and writes a descriptor; the system's own drivers
        # only matter once a domain is created against it.
        return ENV_C
    if any(a in constants.HELP_FLAGS for a in rest):
        return ENV_C
    if verb in _COMMON_VERBS:
        return ENV_C
    try:
        if batch.is_batch(tool, verb, rest):
            return None
        slug, model = _slug_and_model(tool, verb, rest)
        inside = tool == constants.DOM_TOOL or verb not in _SYS_PLANE_VERBS
        if inside and model and verb in _MODEL_VERBS:
            override = env_for_model(model)
            if override:
                return override
        return env_for_system(slug, inside=inside)
    except Exception:
        return ENV_C


def bootstrap(tool: str, module: str) -> None:
    """Re-exec ``module`` inside the venv this command needs, creating it on
    first use.

    A no-op when the command must stay in the launcher (see :func:`target_env`)
    or when we are already inside the target venv. "Already inside" is decided
    solely by ``sys.prefix`` — never an inherited env var: a `tdx-system` /
    `tdx-domain` that another process spawns runs in the launcher interpreter
    and MUST re-bootstrap itself rather than assume the parent did.
    ``sys.prefix`` is also loop-safe — once re-exec'd, the venv interpreter
    reports the venv prefix, so we stop."""
    argv = sys.argv[1:]
    env = target_env(tool, argv)
    if env is None:
        return
    venv = venv_path(env)
    if _in_venv(venv):
        return
    ensure(env)
    py = _venv_python(venv)
    os.execv(str(py), [str(py), "-m", module, *argv])


def clear_venvs() -> int:
    """Remove every managed venv (the ``clear-venvs`` verb)."""
    removed = 0
    for env in ENVS:
        venv = venv_path(env)
        if venv.exists():
            shutil.rmtree(venv)
            print(f">>> removed {venv}")
            removed += 1
        else:
            print(f">>> (absent) {venv}")
    print()
    if removed:
        print(
            ">>> done — each venv is recreated and reinstalled the next time a "
            "command needs it."
        )
    else:
        print(">>> nothing to delete.")
    return 0
