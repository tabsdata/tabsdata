#
# Copyright 2026 Tabs Data Inc.
#

"""Placeholder for the retired legacy-CLI connection-template engine.

The real engine now lives in :mod:`tabsdatak.cli.template` (the ``tdk`` CLI).
The legacy ``td`` connection-template surface is going away, so this stub only
keeps :mod:`tabsdata._cli.td.collection_group` importable; every entry raises.
"""

from __future__ import annotations

from typing import Any, NoReturn


class TemplateError(Exception):
    """Kept so ``except template.TemplateError`` still resolves."""


def _retired(*args: Any, **kwargs: Any) -> NoReturn:
    raise NotImplementedError(
        "the legacy 'td' connection-template commands have been retired; "
        "use the 'tdk' CLI instead"
    )


from_yaml = _retired
to_yaml = _retired
validate = _retired
