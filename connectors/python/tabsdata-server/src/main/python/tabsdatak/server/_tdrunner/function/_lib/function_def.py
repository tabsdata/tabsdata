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

from tabsdatak._info import (
    PublisherInfo,
    SubscriberInfo,
    TransformerInfo,
)
from tabsdatak.common._archive.bundle import (
    BUNDLE_MANIFEST_FILENAME,
    BUNDLE_USER_CODE_DIRNAME,
)
from tabsdatak.error import ErrorCode

# Union of every `*Info` shape a command might load.
FunctionInfo = PublisherInfo | TransformerInfo | SubscriberInfo


def load_function_def_or_raise(
    bundle_dir: Path,
    *,
    allowed_kinds: dict[str, type[FunctionInfo]],
    bad_bundle_code: ErrorCode,
    bad_user_code_code: ErrorCode,
    wrong_kind_code: ErrorCode,
) -> FunctionInfo:
    """:func:`load_function_def` with per-command typed-error wraps.

    Typed counterpart of :func:`load_function_def`: instead of three
    integer ``exit_*`` codes, takes three :class:`ErrorCode` variants
    (the per-command "bad bundle" / "bad user code" / "wrong kind"
    enum members). Re-raises via ``.exception(...)`` so the bound
    typed exception class carries its own ``exit_code``.

    Templates that bind ``bad_bundle_code`` / ``bad_user_code_code`` /
    ``wrong_kind_code`` can reference ``{bundle_dir}``, ``{manifest}``,
    ``{kind}``, ``{allowed}``, and ``{detail}`` as placeholders
    (whichever apply at the failing site).

    Removed alongside the legacy form once every consumer migrates
    (TD-2263).
    """
    manifest = bundle_dir / BUNDLE_MANIFEST_FILENAME
    if not manifest.is_file():
        raise bad_bundle_code.exception(
            bundle_dir=bundle_dir,
            manifest=manifest,
            detail=f"{BUNDLE_MANIFEST_FILENAME} not found in {bundle_dir}",
        )

    user_code_path = str((bundle_dir / BUNDLE_USER_CODE_DIRNAME).resolve())
    if user_code_path not in sys.path:
        sys.path.insert(0, user_code_path)

    try:
        text = manifest.read_text(encoding="utf-8")
    except OSError as exc:
        raise bad_bundle_code.exception(
            cause=exc,
            bundle_dir=bundle_dir,
            manifest=manifest,
            detail=f"unreadable {manifest}: {exc}",
        )

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise bad_bundle_code.exception(
            cause=exc,
            bundle_dir=bundle_dir,
            manifest=manifest,
            detail=f"malformed JSON in {manifest}: {exc}",
        )

    kind = data.get("kind") if isinstance(data, dict) else None
    info_cls = allowed_kinds.get(kind) if kind else None
    if info_cls is None:
        raise wrong_kind_code.exception(
            bundle_dir=bundle_dir,
            manifest=manifest,
            kind=kind,
            allowed=sorted(allowed_kinds),
            detail=(
                f"unsupported function_def kind {kind!r} in {manifest}; "
                f"expected one of {sorted(allowed_kinds)}"
            ),
        )

    try:
        return info_cls.from_dict(data)
    except ValidationError as exc:
        raise bad_user_code_code.exception(
            cause=exc, bundle_dir=bundle_dir, detail=str(exc)
        )
    except Exception as exc:
        raise bad_bundle_code.exception(
            cause=exc, bundle_dir=bundle_dir, detail=str(exc)
        )
