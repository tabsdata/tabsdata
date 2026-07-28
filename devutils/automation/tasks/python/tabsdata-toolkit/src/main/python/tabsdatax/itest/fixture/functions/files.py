#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end helpers shared by the file-based connectors (localfile, s3, gcs,
azureblob): discover a provisioned file/object and fabricate the plugin contexts
a test drives a ``SrcPlugin`` / ``DestPlugin`` with.

Connector-agnostic: for object stores the caller passes an :class:`ObjectStore`
adapter over its own cloud client, so nothing here depends on a specific
connector. Generic (non-file) harness lives in :mod:`.harness`; the functions a
test registers are checked in per connector under its own ``functions/``
project.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pytest
from tabsdatak.spi import DestContext, SrcPluginCtx


class _PluginCtx:
    """The fixed attributes every fabricated plugin context carries -- shared by
    the ``Src`` and ``Dest`` context stubs the integration tests drive the plugin
    with. The values are inert; the plugins only read ``work_dir`` in practice."""

    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "e2e-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "e2e-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def set_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass


class PluginSrcCtx(_PluginCtx, SrcPluginCtx):
    """Fabricated ``SrcPluginCtx`` for driving a connector's ``SrcPlugin`` directly
    in an integration test."""


class PluginDestCtx(_PluginCtx, DestContext):
    """Fabricated ``DestContext`` for driving a connector's ``DestPlugin`` directly
    in an integration test."""


class ObjectStore(Protocol):
    """A connector-provided adapter over its cloud client (boto3 /
    google-cloud-storage / azure-storage-blob), so the shared object-store helpers
    stay client-agnostic. Keys are container/bucket-relative object paths."""

    def exists(self, key: str) -> bool: ...

    def download(self, key: str, dest: Path) -> None: ...

    def upload(self, local: Path, key: str) -> None: ...


def download_object(store: ObjectStore, key: str, dest: Path) -> Path:
    """Download ``key`` to ``dest``, failing if the object is absent. Callers are
    destination tests reading back what the connector just wrote, so a missing
    object is the connector's failure to report -- not a reason to skip."""
    if not store.exists(key):
        pytest.fail(f"object {key!r} was not written", pytrace=False)
    store.download(key, dest)
    return dest


def first_provisioned_file(base_dir: Path, pattern: str = "*.csv") -> Path:
    """The first file matching ``pattern`` in ``base_dir`` (resolved absolute).
    Absolute matters: the connection base_path is handed to the function pod,
    which resolves it against its own cwd -- a relative path would match no file
    there and the function would see empty input.

    A domain that resolved but holds no such file is a provisioning failure, so
    this fails rather than skipping: the harness already fails outright on a
    missing domain, and a silent skip here would hide the same problem."""
    resolved = base_dir.resolve()
    matches = sorted(resolved.glob(pattern))
    if not matches:
        pytest.fail(
            f"no {pattern!r} file provisioned at {resolved}; (re)seed the domain",
            pytrace=False,
        )
    return matches[0]
