#
# Copyright 2026 Tabs Data Inc.
#

"""Function-definition loader for tdrunner commands.

Folds the three per-command ``_load_function_def`` implementations
(``run_function``, ``run_source_plugin``, ``run_destination_plugin``)
into a single reusable helper. The previous bodies were nearly
identical; the only variability was:

  * which set of kinds the command accepts (one, one, or three),
  * which exit codes the failures map to.

This module exposes :func:`load_function_def` parametrised by both.

The loader is also responsible for prepending the bundle's
``user_code/`` subdir to ``sys.path`` *before* deserialising the
``*Info`` instance — the decorator-stamped ``func`` field is stored
as ``"module:qualname"`` and needs the user package importable for
``callable_from_str`` to resolve.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from pydantic import ValidationError

from tabsdatak._bundle import BUNDLE_MANIFEST_FILENAME, BUNDLE_USER_CODE_DIRNAME
from tabsdatak._info import (
    PublisherInfo,
    SubscriberInfo,
    TransformerInfo,
)
from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError

# Union of every `*Info` shape a command might load.
FunctionInfo = PublisherInfo | TransformerInfo | SubscriberInfo


def load_function_def(
    bundle_dir: Path,
    *,
    allowed_kinds: dict[str, type[FunctionInfo]],
    exit_bad_bundle: int,
    exit_bad_user_code: int,
    exit_wrong_kind: int,
) -> FunctionInfo:
    """Load ``function_def.json`` from ``bundle_dir`` and return the
    matching ``*Info`` instance.

    Args:
        bundle_dir: Extracted bundle root (the directory containing
            ``function_def.json`` and ``user_code/``).
        allowed_kinds: Mapping ``kind-string -> Info subclass``. A
            single-entry dict pins the command to one kind (e.g.
            run-source-plugin → ``{KIND_PUBLISHER_DEF: PublisherInfo}``);
            a multi-entry dict allows dispatch (e.g. run-function
            accepts publisher/transformer/subscriber).
        exit_bad_bundle: Exit code for bundle-shape failures —
            manifest file missing, unreadable, malformed JSON, or
            unexpected internal errors during deserialisation.
        exit_bad_user_code: Exit code for user-code resolution
            failures — pydantic ``ValidationError`` paths that
            originate from ``callable_from_str`` raising
            ``ImportError`` / ``AttributeError``.
        exit_wrong_kind: Exit code when the bundle's ``kind`` is
            valid YAML/JSON but not in ``allowed_kinds``.

    Side effect: ``<bundle_dir>/user_code/`` is prepended to
    ``sys.path`` before deserialisation, so the user package is
    importable when pydantic resolves the stored ``"module:qualname"``
    string for ``info.func``. The insert is idempotent — if the path
    is already present, nothing changes.

    Raises:
        CommandSpecificError(exit_bad_bundle): manifest absent,
            unreadable, malformed JSON, or unexpected error.
        CommandSpecificError(exit_wrong_kind): ``kind`` value is not
            in ``allowed_kinds``.
        CommandSpecificError(exit_bad_user_code): pydantic validation
            of the resolved ``Info`` failed (typically because
            ``callable_from_str`` couldn't import the user function).
    """
    manifest = bundle_dir / BUNDLE_MANIFEST_FILENAME
    if not manifest.is_file():
        raise CommandSpecificError(
            exit_bad_bundle,
            f"{BUNDLE_MANIFEST_FILENAME} not found in {bundle_dir}",
        )

    user_code_path = str((bundle_dir / BUNDLE_USER_CODE_DIRNAME).resolve())
    if user_code_path not in sys.path:
        sys.path.insert(0, user_code_path)

    try:
        text = manifest.read_text(encoding="utf-8")
    except OSError as exc:
        raise CommandSpecificError(
            exit_bad_bundle, f"unreadable {manifest}: {exc}"
        ) from exc

    # JSON parsing is a bundle concern (producer wrote garbage);
    # schema validation is a separate failure mode below. Pydantic's
    # `validate_json` collapses both into `ValidationError`, so we
    # split them by pre-parsing here.
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CommandSpecificError(
            exit_bad_bundle, f"malformed JSON in {manifest}: {exc}"
        ) from exc

    kind = data.get("kind") if isinstance(data, dict) else None
    info_cls = allowed_kinds.get(kind) if kind else None
    if info_cls is None:
        raise CommandSpecificError(
            exit_wrong_kind,
            f"unsupported function_def kind {kind!r} in {manifest}; "
            f"expected one of {sorted(allowed_kinds)}",
        )

    try:
        return info_cls.from_dict(data)
    except ValidationError as exc:
        # ImportError / AttributeError from callable_from_str surface
        # here as ValidationError; user-code failure, not bundle shape.
        raise CommandSpecificError(exit_bad_user_code, str(exc)) from exc
    except Exception as exc:
        # Anything else (unexpected pydantic internals, IO oddities)
        # — surface as a bundle problem.
        raise CommandSpecificError(exit_bad_bundle, str(exc)) from exc
