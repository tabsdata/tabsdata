#
# Copyright 2026 Tabsdata Inc.
#

"""Generate per-table data files for a bundled model.

Models live in ``src/main/python/tabsdatax.itest.tdksystems/models/<name>/``
(shipped as package_data). Each model has:
  - ``generator.py`` — the source generator (calls
    ``tabsdatax.itest.tdksystems.emit.write_tables``)
  - ``sizing.json`` — scaling configuration
  - ``<system>.ddl`` — schema definitions (mysql, postgres, …)
"""

from __future__ import annotations

import contextlib
import importlib.resources
import importlib.util
import json
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Iterator

from .errors import DatagenError

SIZE_BUCKETS: dict[str, int] = {
    "xs": 100,
    "s": 1000,
    "m": 10000,
    "l": 100000,
    "xl": 1000000,
}
VALID_FORMATS = ("sql", "csv", "json", "parquet")


# ---------------------------------------------------------------------------
# Model resolution — bundled with the package
# ---------------------------------------------------------------------------


def _models_root() -> Path:
    """Path to ``src/main/python/tabsdatax.itest.tdksystems/models/`` (or its
    installed equivalent)."""
    root = importlib.resources.files("tabsdatax.itest.tdksystems") / "models"
    return Path(str(root))


def list_models() -> list[str]:
    root = _models_root()
    if not root.is_dir():
        return []
    return sorted(
        p.name for p in root.iterdir() if p.is_dir() and (p / "generator.py").is_file()
    )


def model_dir(name: str) -> Path:
    p = _models_root() / name
    if not (p / "generator.py").is_file():
        avail = ", ".join(list_models()) or "(none)"
        raise DatagenError(f"unknown model: {name} (available: {avail})")
    return p


def is_sizeable(name: str) -> bool:
    """A model is sizeable if its sizing.json has a `prolific` key.

    Models without sizing.json or without a `prolific` entry are treated
    as fixed-data: the row counts are baked into the source generator.py
    and `--size` doesn't apply."""
    cfg = model_dir(name) / "sizing.json"
    if not cfg.is_file():
        return False
    try:
        data = json.loads(cfg.read_text())
    except json.JSONDecodeError:
        return False
    return bool(data.get("prolific"))


def model_kinds(name: str) -> list[str]:
    """Return the list of system-kinds this model can target.

    Standard values: "sql", "file", "salesforce", "mongodb", "kafka".
    Read from sizing.json's `"kinds"` field. Defaults to
    ``["sql", "file", "kafka"]`` when the field is absent (the bundled
    tabular models ship DDL files, can emit CSV, and can be produced to
    kafka topics)."""
    cfg = model_dir(name) / "sizing.json"
    if not cfg.is_file():
        return ["sql", "file", "kafka"]
    try:
        data = json.loads(cfg.read_text())
    except json.JSONDecodeError:
        return ["sql", "file", "kafka"]
    kinds = data.get("kinds")
    if not kinds:
        return ["sql", "file", "kafka"]
    if not isinstance(kinds, list) or not all(isinstance(k, str) for k in kinds):
        raise DatagenError(
            f"model {name!r}: sizing.json `kinds` must be a list of strings "
            f"(got {kinds!r})"
        )
    return list(kinds)


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------


def generate(
    sourcedir: Path,
    size: str | None,
    output: Path,
    fmt: str,
    chunks: int | None = None,
    engine: str | None = None,
    schema_sidecar: bool = False,
) -> None:
    """Generate the model into `output`.

    For sizeable models (sizing.json has a `prolific` key), `size` selects a
    bucket from SIZE_BUCKETS and we pass scaled args to the source.
    For fixed models, `size` is ignored — the source emits its baked-in
    row counts.

    `engine` — optional target-engine slug ('mysql', 'postgres', …). Exposed
    to the model's generator.py via the ``TDX_ENGINE`` env var. Most models
    are engine-agnostic and ignore it; models with engine-specific SQL
    quirks (e.g. bronze, whose CDC table uses identifier names that need
    engine-appropriate quoting) read it.

    `schema_sidecar` — when True, ask the emitter to also drop a
    ``_schema.json`` column-type sidecar next to the data (via the
    ``TDX_SCHEMA_SIDECAR`` env var). The kafka and saas-sql loaders set
    this so they can rebuild Avro/Protobuf/JSON schemas and typed INSERTs
    from the otherwise type-erased json output.
    """
    if size is not None and size not in SIZE_BUCKETS:
        raise DatagenError(
            f"unknown size '{size}'; expected one of {list(SIZE_BUCKETS)}"
        )
    if fmt not in VALID_FORMATS:
        raise DatagenError(
            f"unknown format '{fmt}'; expected one of {list(VALID_FORMATS)}"
        )
    if chunks is not None and chunks < 1:
        raise DatagenError(f"--chunks must be >= 1 (got {chunks})")

    sourcedir = Path(sourcedir)
    cfg_path = sourcedir / "sizing.json"
    generator_path = sourcedir / "generator.py"
    if not generator_path.is_file():
        raise DatagenError(f"missing {generator_path}")

    cfg: dict = {}
    if cfg_path.is_file():
        cfg = json.loads(cfg_path.read_text())

    argv = [str(generator_path), "--format", fmt, "--out", str(output)]
    if cfg.get("prolific"):
        # Sizeable model — scale every default by the chosen size bucket.
        bucket = size or "xs"
        n = SIZE_BUCKETS[bucket]
        prolific = cfg["prolific"]
        base = int(cfg["prolific_base"])
        defaults: dict[str, int] = cfg.get("defaults", {})
        caps: dict[str, int] = cfg.get("caps", {})
        mins: dict[str, int] = cfg.get("mins", {})
        argv += [prolific, str(n)]
        for flag, default in defaults.items():
            floor = int(mins.get(flag, 1))
            value = max(int(default) * n // base, floor)
            if flag in caps:
                value = min(value, int(caps[flag]))
            argv += [flag, str(value)]
    if chunks is not None:
        argv += ["--chunks", str(chunks)]

    module = _load_generator(generator_path)
    if not hasattr(module, "main"):
        raise DatagenError(f"{generator_path} has no main() entry point")
    with (
        _patched_argv(argv),
        _patched_engine_env(engine),
        _patched_env("TDX_SCHEMA_SIDECAR", "1" if schema_sidecar else None),
    ):
        module.main()


def _load_generator(path: Path) -> ModuleType:
    mod_name = f"_generator_{path.parent.name.replace('-', '_')}"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    if spec is None or spec.loader is None:
        raise DatagenError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    # Register before exec so features like @dataclass that look the module
    # up in sys.modules during class creation work.
    sys.modules[mod_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(mod_name, None)
        raise
    return module


@contextlib.contextmanager
def _patched_argv(argv: list[str]) -> Iterator[None]:
    saved = sys.argv
    sys.argv = argv
    try:
        yield
    finally:
        sys.argv = saved


@contextlib.contextmanager
def _patched_engine_env(engine: str | None) -> Iterator[None]:
    """Expose the target engine slug to the model's generator via
    ``TDX_ENGINE`` (no-op when no engine is supplied)."""
    if not engine:
        yield
        return
    prev = os.environ.get("TDX_ENGINE")
    os.environ["TDX_ENGINE"] = engine
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("TDX_ENGINE", None)
        else:
            os.environ["TDX_ENGINE"] = prev


@contextlib.contextmanager
def _patched_env(name: str, value: str | None) -> Iterator[None]:
    """Temporarily set (value is not None) or leave untouched (value is None)
    an env var for the duration of the block, restoring the prior value."""
    if value is None:
        yield
        return
    prev = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = prev
