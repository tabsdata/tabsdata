#
# Copyright 2026 Tabs Data Inc.
#

"""Unit tests for the new v2 ``register_function`` / ``patch_function``
plumbing in ``tabsdatak.sdk.tabsdata_server``.

Network-bound flows (the actual HTTP request to the apiserver) are
exercised end-to-end against a live server in the integration suite.
Here we test the bundle-producer wiring + per-``FunctionInfo`` metadata
extraction, which is what the v2 migration rewrote."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from tabsdatak.api import (
    AllTablesTrigger,
    CronTrigger,
    TablesTrigger,
)

# noinspection PyProtectedMember
from tabsdatak.sdk import tabsdata_server as ts


# ---------------------------------------------------------------------------
# Duck-typed FunctionInfo stand-ins for the metadata extractor tests.
#
# The real ``PublisherInfo`` / ``TransformerInfo`` / ``SubscriberInfo``
# are pydantic dataclasses whose ``__init__`` runs full validation
# (signature check against the source's slot count, table-name regex
# enforcement, etc.). For unit-testing the SDK plumbing we don't want
# to thread a complete registry-aware setup -- the extractors only
# read public attributes, so a thin duck-typed object suffices.
#
# The ``_create_archive`` bundle-producer tests build *real* Info
# instances (the bundle producer cares about deep structure).
# ---------------------------------------------------------------------------


class _FakeSrc:
    """A class whose ``type(...).__name__`` is what the test expects
    to surface as the ``plugin_name`` in the register payload."""

    pass


class _FakeDest:
    pass


def _decorated(name: str):
    def f():  # pragma: no cover - never called
        return None

    f.__name__ = name
    return f


class _InfoStub:
    """Duck-typed Info: exposes only the attributes the metadata
    extractors read."""

    def __init__(self, *, kind, func, trigger_by=None, **kw):
        self.__kind__ = kind
        self.func = func
        self.trigger_by = trigger_by
        for k, v in kw.items():
            setattr(self, k, v)


def _publisher_info(
    *,
    func_name: str = "pub_fn",
    output_tables: list[str] | None = None,
    trigger_by=None,
):
    from tabsdatak._info import KIND_PUBLISHER_DEF

    return _InfoStub(
        kind=KIND_PUBLISHER_DEF,
        func=_decorated(func_name),
        source=_FakeSrc(),
        output_tables=output_tables or ["t1"],
        trigger_by=trigger_by,
        on_tables=None,
    )


def _transformer_info(
    *,
    func_name: str = "transform_fn",
    input_tables: list[str] | None = None,
    output_tables: list[str] | None = None,
    trigger_by=None,
):
    from tabsdatak._info import KIND_TRANSFORMER_DEF

    return _InfoStub(
        kind=KIND_TRANSFORMER_DEF,
        func=_decorated(func_name),
        input_tables=input_tables or ["coll/a"],
        output_tables=output_tables or ["b"],
        trigger_by=trigger_by,
        on_tables=None,
    )


def _subscriber_info(
    *,
    func_name: str = "sub_fn",
    input_tables: list[str] | None = None,
    trigger_by=None,
):
    from tabsdatak._info import KIND_SUBSCRIBER_DEF

    return _InfoStub(
        kind=KIND_SUBSCRIBER_DEF,
        func=_decorated(func_name),
        destination=_FakeDest(),
        input_tables=input_tables or ["coll/x"],
        trigger_by=trigger_by,
    )


# ---------------------------------------------------------------------------
# Trivial metadata extractors
# ---------------------------------------------------------------------------


def test_kind_to_api_type_maps_publisher_to_p():
    from tabsdatak._info import (
        KIND_PUBLISHER_DEF,
        KIND_STREAM_PUBLISHER_DEF,
        KIND_SUBSCRIBER_DEF,
        KIND_TRANSFORMER_DEF,
    )

    assert ts._KIND_TO_API_TYPE[KIND_PUBLISHER_DEF] == "P"
    assert ts._KIND_TO_API_TYPE[KIND_STREAM_PUBLISHER_DEF] == "P"
    assert ts._KIND_TO_API_TYPE[KIND_TRANSFORMER_DEF] == "T"
    assert ts._KIND_TO_API_TYPE[KIND_SUBSCRIBER_DEF] == "S"


def test_input_tables_returns_publisher_empty_list():
    assert ts._input_tables(_publisher_info()) == []


def test_input_tables_returns_transformer_input_tables():
    info = _transformer_info(input_tables=["c/a", "c/b"])
    assert ts._input_tables(info) == ["c/a", "c/b"]


def test_input_tables_returns_subscriber_input_tables():
    info = _subscriber_info(input_tables=["c/x"])
    assert ts._input_tables(info) == ["c/x"]


def test_output_tables_returns_publisher_outputs():
    info = _publisher_info(output_tables=["t1", "t2"])
    assert ts._output_tables(info) == ["t1", "t2"]


def test_output_tables_returns_empty_for_subscriber():
    assert ts._output_tables(_subscriber_info()) == []


def test_source_or_destination_name_for_publisher_returns_source_class_name():
    assert ts._source_or_destination_name(_publisher_info()) == "_FakeSrc"


def test_source_or_destination_name_for_subscriber_returns_destination_class_name():
    assert ts._source_or_destination_name(_subscriber_info()) == "_FakeDest"


def test_source_or_destination_name_for_transformer_is_none():
    assert ts._source_or_destination_name(_transformer_info()) is None


def test_cron_workers_returns_ct_payload_for_cron_trigger():
    info = _publisher_info(trigger_by=CronTrigger(mask="0 * * * *"))
    workers = ts._cron_workers(info)
    assert workers is not None
    assert "CT" in workers
    assert "0 * * * *" in workers["CT"]


def test_cron_workers_returns_none_for_non_cron_trigger():
    info = _transformer_info(trigger_by=TablesTrigger(tables=["c/a"]))
    assert ts._cron_workers(info) is None


def test_cron_workers_returns_none_for_none_trigger():
    assert ts._cron_workers(_publisher_info(trigger_by=None)) is None


def test_trigger_string_list_returns_table_list_for_tables_trigger():
    info = _transformer_info(trigger_by=TablesTrigger(tables=["c/a", "c/b"]))
    assert ts._trigger_string_list(info) == ["c/a", "c/b"]


def test_trigger_string_list_returns_none_for_cron_trigger():
    info = _publisher_info(trigger_by=CronTrigger(mask="0 * * * *"))
    assert ts._trigger_string_list(info) is None


def test_trigger_string_list_returns_none_for_all_tables_trigger():
    info = _transformer_info(trigger_by=AllTablesTrigger())
    assert ts._trigger_string_list(info) is None


def test_trigger_string_list_returns_none_when_no_trigger():
    assert ts._trigger_string_list(_publisher_info(trigger_by=None)) is None


# ---------------------------------------------------------------------------
# _create_archive: metadata extraction (bundle producer mocked out)
#
# ``tabsdatak._bundle.build_bundle`` has its own dedicated tests in the
# tabsdata-api package; here we monkeypatch it so the metadata
# extraction can be tested in isolation against simple Info stubs.
# ---------------------------------------------------------------------------


@pytest.fixture
def project_root(tmp_path: Path) -> Path:
    """An arbitrary directory used as ``project_root`` (the bundle
    producer is mocked, so the contents don't matter)."""
    return tmp_path


@pytest.fixture
def _no_op_bundle(monkeypatch):
    """Replace ``build_bundle`` with a no-op that just touches the
    target path, and ``capture_source_snippet`` with a constant."""
    captured: dict = {}

    def fake_build(info, project_root, output_path, **kw):
        captured["info"] = info
        captured["project_root"] = project_root
        captured["output_path"] = output_path
        captured["kw"] = kw
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_bytes(b"stub")
        return output_path

    monkeypatch.setattr(ts, "build_bundle", fake_build)
    monkeypatch.setattr(ts, "capture_source_snippet", lambda info: "<source>")
    return captured


def test_create_archive_calls_build_bundle_and_returns_path(
    project_root, _no_op_bundle
):
    info = _publisher_info()
    with tempfile.TemporaryDirectory() as tmp:
        archive = ts._create_archive(info, project_root, tmp)
        assert Path(archive.context_location).exists()
        # The bundle was written under the supplied tmp dir.
        assert Path(archive.context_location).parent == Path(tmp)
    # ``build_bundle`` received the same info and project_root.
    assert _no_op_bundle["info"] is info
    assert _no_op_bundle["project_root"] == project_root


def test_create_archive_publisher_cron_extracts_metadata(project_root, _no_op_bundle):
    info = _publisher_info(
        func_name="pub_fn",
        output_tables=["t1", "t2"],
        trigger_by=CronTrigger(mask="*/5 * * * *"),
    )
    with tempfile.TemporaryDirectory() as tmp:
        archive = ts._create_archive(info, project_root, tmp)

    assert archive.function_name == "pub_fn"
    assert archive.function_type == "P"
    assert archive.tables == ["t1", "t2"]
    assert archive.string_dependencies == []
    assert archive.trigger_string_list is None  # cron -> workers
    assert archive.workers is not None and "CT" in archive.workers
    assert archive.source_or_destination == "_FakeSrc"
    assert archive.table_types == {}
    assert archive.function_snippet == "<source>"


def test_create_archive_transformer_tables_trigger(project_root, _no_op_bundle):
    info = _transformer_info(
        func_name="tx_fn",
        input_tables=["coll/a", "coll/b"],
        output_tables=["out"],
        trigger_by=TablesTrigger(tables=["coll/trig"]),
    )
    with tempfile.TemporaryDirectory() as tmp:
        archive = ts._create_archive(info, project_root, tmp)
    assert archive.function_type == "T"
    assert archive.string_dependencies == ["coll/a", "coll/b"]
    assert archive.tables == ["out"]
    assert archive.trigger_string_list == ["coll/trig"]
    assert archive.workers is None
    assert archive.source_or_destination is None  # transformers have no plugin name


def test_create_archive_subscriber_no_outputs(project_root, _no_op_bundle):
    info = _subscriber_info(input_tables=["coll/x", "coll/y"])
    with tempfile.TemporaryDirectory() as tmp:
        archive = ts._create_archive(info, project_root, tmp)
    assert archive.function_type == "S"
    assert archive.tables == []
    assert archive.string_dependencies == ["coll/x", "coll/y"]
    assert archive.source_or_destination == "_FakeDest"


def test_create_archive_accepts_temporary_directory_object(project_root, _no_op_bundle):
    """The ``temporary_directory`` arg may also be a
    ``tempfile.TemporaryDirectory`` (legacy call shape); the helper
    must accept either."""
    info = _publisher_info()
    td = tempfile.TemporaryDirectory()
    try:
        archive = ts._create_archive(info, project_root, td)
        assert Path(archive.context_location).exists()
    finally:
        td.cleanup()


def test_create_archive_forwards_local_packages_and_python_versions(
    project_root, _no_op_bundle
):
    info = _publisher_info()
    with tempfile.TemporaryDirectory() as tmp:
        ts._create_archive(
            info,
            project_root,
            tmp,
            local_packages=["/some/pkg"],
            valid_python_versions=["3.12"],
        )
    # The kwargs reached build_bundle unchanged (paths converted to
    # Path objects).
    kw = _no_op_bundle["kw"]
    assert kw["valid_python_versions"] == ["3.12"]
    assert kw["local_packages"] == [Path("/some/pkg")]


# ---------------------------------------------------------------------------
# Module-level smoke
# ---------------------------------------------------------------------------


def test_function_info_alias_covers_all_kinds():
    """``FunctionInfo`` is the union the SDK accepts. Verify it
    covers the four ``*Info`` classes we publish."""
    from tabsdatak._info import (
        PublisherInfo,
        StreamPublisherInfo,
        SubscriberInfo,
        TransformerInfo,
    )

    assert PublisherInfo in ts.FunctionInfo.__args__
    assert StreamPublisherInfo in ts.FunctionInfo.__args__
    assert TransformerInfo in ts.FunctionInfo.__args__
    assert SubscriberInfo in ts.FunctionInfo.__args__
