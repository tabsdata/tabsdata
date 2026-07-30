#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the new v2 ``register_function`` / ``patch_function``
plumbing in ``tabsdatak.sdk.tabsdata_server``.

Network-bound flows (the actual HTTP request to the apiserver) are
exercised end-to-end against a live server in the integration suite.
Here we test the bundle-producer wiring + per-``FunctionInfo`` metadata
extraction, which is what the v2 migration rewrote."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

# noinspection PyProtectedMember
from tabsdatak._api import qualified_class
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
    """A source class whose FQN (``qualified_class(...)``) is what the test
    expects to surface as the ``connector`` in the register payload."""

    pass


class _FakeDest:
    pass


# Connection (Conn) classes the *registry* binds ``_FakeSrc`` / ``_FakeDest``
# to. ``_connection`` reports the Conn class FQN (``qualified_class(def.conn)``)
# from the registry lookup, so these -- not the decorator's ``conn`` -- decide
# the surfaced connection.
class _FakeSrcConn:
    pass


class _FakeDestConn:
    pass


@pytest.fixture(autouse=True)
def _stub_connector_registry(monkeypatch):
    """``_connector`` / ``_connection`` resolve from the connector registry by
    the source/destination type (not the decorator ``conn``). Map the
    duck-typed fakes to stub defs whose ``conn`` is the expected Conn class."""
    from types import SimpleNamespace

    def get_src(cls):
        if cls is _FakeSrc:
            return SimpleNamespace(conn=_FakeSrcConn)
        raise KeyError(cls)

    def get_dest(cls):
        if cls is _FakeDest:
            return SimpleNamespace(conn=_FakeDestConn)
        raise KeyError(cls)

    def get_stream_src(cls):
        raise KeyError(cls)

    monkeypatch.setattr(ts._connectors, "get_src", get_src)
    monkeypatch.setattr(ts._connectors, "get_dest", get_dest)
    monkeypatch.setattr(ts._connectors, "get_stream_src", get_stream_src)


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
        # The reported connection kind comes from the registry
        # (``_FakeSrcConn`` -> ``mysqlSrcConn``), keyed by the source
        # type -- the function descriptor no longer carries a conn.
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
        # See ``_publisher_info``: the registry, keyed by the destination
        # type, decides the reported kind (``_FakeDestConn`` ->
        # ``mysqlDestConn``).
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


def test_connector_for_publisher_returns_source_class_fqn():
    assert ts._connector(_publisher_info()) == qualified_class(_FakeSrc)


def test_connector_for_subscriber_returns_destination_class_fqn():
    assert ts._connector(_subscriber_info()) == qualified_class(_FakeDest)


def test_connector_for_transformer_is_none():
    assert ts._connector(_transformer_info()) is None


def test_connection_for_publisher_comes_from_registry_not_decorator():
    # The registry binds the source to ``_FakeSrcConn``; ``_connection``
    # reports that Conn class's FQN, not anything from the decorator.
    assert ts._connection(_publisher_info()) == qualified_class(_FakeSrcConn)


def test_connection_for_subscriber_comes_from_registry_not_decorator():
    assert ts._connection(_subscriber_info()) == qualified_class(_FakeDestConn)


def test_connection_for_transformer_is_none():
    assert ts._connection(_transformer_info()) is None


def test_cron_workers_returns_ct_payload_for_cron_trigger():
    info = _publisher_info(trigger_by=CronTrigger(mask="0 * * * *"))
    workers = ts._workers(info)
    assert workers is not None
    assert "CT" in workers
    assert "0 * * * *" in workers["CT"]


def test_cron_workers_returns_none_for_non_cron_trigger():
    info = _transformer_info(trigger_by=TablesTrigger(tables=["c/a"]))
    assert ts._workers(info) is None


def test_cron_workers_returns_none_for_none_trigger():
    assert ts._workers(_publisher_info(trigger_by=None)) is None


def test_workers_returns_st_payload_for_stream_publisher():
    """``@stream_publisher`` registers must emit ``{"ST": ...}``;
    without it the apiserver never enqueues the long-running
    stream-publisher worker (TD-2569)."""
    from tabsdatak._info import KIND_STREAM_PUBLISHER_DEF

    info = _InfoStub(
        kind=KIND_STREAM_PUBLISHER_DEF,
        func=_decorated("stream_fn"),
        source=_FakeSrc(),
        output_tables=["streamed"],
        # @stream_publisher does not support a `trigger_by` arg; the
        # streamer pod IS the trigger source for downstream functions.
        trigger_by=None,
    )

    workers = ts._workers(info)
    assert workers is not None
    assert list(workers) == ["ST"]
    # StageTriggerConfig fields all carry serde defaults Rust-side
    # (TimeoutThresholdMin / SizeThreshold typed-int wrappers,
    # Option<DateTime> start/end); the SDK sends an empty body.
    assert json.loads(workers["ST"]) == {}


def test_workers_backcompat_alias_preserved():
    """The old name ``_cron_workers`` is kept as an alias for any
    out-of-tree caller -- both names resolve to the same function."""
    assert ts._cron_workers is ts._workers


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
# ``tabsdatak.common._archive.bundle.build_bundle`` has its own dedicated tests in the
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
        archive = ts._create_archive("coll", info, project_root, tmp)
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
        archive = ts._create_archive("coll", info, project_root, tmp)

    assert archive.function_name == "pub_fn"
    assert archive.function_type == "P"
    assert archive.tables == ["t1", "t2"]
    assert archive.string_dependencies == []
    assert archive.trigger_string_list is None  # cron -> workers
    assert archive.workers is not None and "CT" in archive.workers
    assert archive.connector == qualified_class(_FakeSrc)
    assert archive.connection == qualified_class(_FakeSrcConn)
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
        archive = ts._create_archive("coll", info, project_root, tmp)
    assert archive.function_type == "T"
    assert archive.string_dependencies == ["coll/a", "coll/b"]
    assert archive.tables == ["out"]
    assert archive.trigger_string_list == ["coll/trig"]
    assert archive.workers is None
    assert archive.connector is None  # transformers have no connector
    assert archive.connection is None


def test_create_archive_subscriber_no_outputs(project_root, _no_op_bundle):
    info = _subscriber_info(input_tables=["coll/x", "coll/y"])
    with tempfile.TemporaryDirectory() as tmp:
        archive = ts._create_archive("coll", info, project_root, tmp)
    assert archive.function_type == "S"
    assert archive.tables == []
    assert archive.string_dependencies == ["coll/x", "coll/y"]
    assert archive.connector == qualified_class(_FakeDest)
    assert archive.connection == qualified_class(_FakeDestConn)


def test_create_archive_accepts_temporary_directory_object(project_root, _no_op_bundle):
    """The ``temporary_directory`` arg may also be a
    ``tempfile.TemporaryDirectory`` (legacy call shape); the helper
    must accept either."""
    info = _publisher_info()
    td = tempfile.TemporaryDirectory()
    try:
        archive = ts._create_archive("coll", info, project_root, td)
        assert Path(archive.context_location).exists()
    finally:
        td.cleanup()


def test_create_archive_forwards_local_packages_and_python_versions(
    project_root, _no_op_bundle
):
    info = _publisher_info()
    with tempfile.TemporaryDirectory() as tmp:
        ts._create_archive(
            "coll",
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


def test_create_archive_forwards_requirements_to_build_bundle(
    project_root, _no_op_bundle, tmp_path
):
    """``--requirements`` flag in the CLI plumbs through to the
    ``requirements_yaml`` kwarg on ``build_bundle``."""
    requirements_file = tmp_path / "my-requirements.yaml"
    requirements_file.write_text("publicPackages: []\n", encoding="utf-8")
    info = _publisher_info()
    with tempfile.TemporaryDirectory() as tmp:
        ts._create_archive(
            "coll",
            info,
            project_root,
            tmp,
            requirements=str(requirements_file),
        )
    kw = _no_op_bundle["kw"]
    assert kw["requirements_yaml"] == requirements_file


def test_create_archive_passes_none_requirements_yaml_when_not_set(
    project_root, _no_op_bundle
):
    info = _publisher_info()
    with tempfile.TemporaryDirectory() as tmp:
        ts._create_archive("coll", info, project_root, tmp)
    assert _no_op_bundle["kw"]["requirements_yaml"] is None


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


# ---------------------------------------------------------------------------
# Collection._build_and_upload_bundle -- shared helper used by
# register_function / update_function / patch_function. The bundle
# producer and the apiserver client are both mocked out; we verify
# the helper bundles via ``_create_archive`` and uploads the bytes
# to the apiserver, returning the archive + bundle_id pair.
# ---------------------------------------------------------------------------


def test_build_and_upload_bundle_invokes_bundle_and_upload(
    project_root, _no_op_bundle, monkeypatch
):
    """End-to-end: ``_build_and_upload_bundle`` produces a bundle
    (via the mocked ``build_bundle``), passes the bytes to
    ``function_upload_bundle``, and returns the assigned bundle_id."""
    info = _publisher_info()

    class _FakeResponse:
        def json(self):
            return {"data": {"id": "bundle-abc"}}

    seen_uploads: dict = {}

    class _FakeConnection:
        def runtime_info_get(self):
            class R:
                @staticmethod
                def json():
                    return {"data": {"python_versions": ["3.12"]}}

            return R()

        def function_upload_bundle(self, collection_name, bundle, raise_for_status):
            seen_uploads["collection_name"] = collection_name
            seen_uploads["bundle"] = bundle
            seen_uploads["raise_for_status"] = raise_for_status
            return _FakeResponse()

    coll = ts.Collection.__new__(ts.Collection)
    coll.connection = _FakeConnection()
    coll.name = "my_coll"

    archive, bundle_id = coll._build_and_upload_bundle(
        info, project_root, local_packages=None, raise_for_status=True
    )

    assert bundle_id == "bundle-abc"
    assert seen_uploads["collection_name"] == "my_coll"
    assert seen_uploads["bundle"] == b"stub"  # written by the _no_op_bundle fixture
    assert seen_uploads["raise_for_status"] is True
    # The archive carries the v2 metadata extracted from ``info``.
    assert archive.function_type == "P"
    assert archive.function_name == "pub_fn"


# ---------------------------------------------------------------------------
# Collection base_path field wiring.
#
# ``Collection.__init__`` keeps two distinct slots:
#   * ``_base_path`` -- the *create-time* override (what ``create()``
#     forwards to ``collection_create``); None unless the caller passed
#     ``base_path=`` explicitly.
#   * ``base_path``  -- the *effective* value; falls back to the server
#     value carried in the data dict / kwargs when no override is given.
# ---------------------------------------------------------------------------


def test_download_function_bundle_writes_response_content(tmp_path):
    """``download_function_bundle`` fetches by fvid and writes the raw bytes
    to the destination file."""
    seen: dict = {}

    class _FakeResponse:
        content = b"TGZ-BYTES"

    class _FakeConnection:
        def function_bundle_download(self, function_version_id, raise_for_status=True):
            seen["fvid"] = function_version_id
            seen["raise_for_status"] = raise_for_status
            return _FakeResponse()

    server = ts.TabsdataServer.__new__(ts.TabsdataServer)
    server.connection = _FakeConnection()

    dest = tmp_path / "bundle.tar.zst"
    server.download_function_bundle("fv-123", str(dest))

    assert seen["fvid"] == "fv-123"
    assert seen["raise_for_status"] is True
    assert dest.read_bytes() == b"TGZ-BYTES"


def test_collection_stores_explicit_base_path_in_both_slots():
    coll = ts.Collection(connection=None, name="c", base_path="/data/c")
    assert coll._base_path == "/data/c"
    assert coll.base_path == "/data/c"


def test_collection_base_path_none_when_not_supplied():
    coll = ts.Collection(connection=None, name="c")
    # No create-time override...
    assert coll._base_path is None
    # ...and nothing in kwargs to fall back to.
    assert coll.base_path is None


def test_collection_base_path_from_server_data_dict_splat():
    # Read path: ``Collection(conn, **raw_collection)`` splats the
    # server's data dict. ``base_path`` is a named __init__ param, so a
    # server-provided value binds it directly and surfaces on both slots.
    raw_collection = {"name": "c", "base_path": "/server/resolved"}
    coll = ts.Collection(None, **raw_collection)
    assert coll.base_path == "/server/resolved"
    assert coll._base_path == "/server/resolved"


# ---------------------------------------------------------------------------
# Collection.update_function: forwards base_path / reset_state /
# reset_stage into the apiserver ``function_update`` call. The bundle
# producer and upload are mocked (reusing the same machinery as the
# _build_and_upload_bundle test); we capture the kwargs handed to the
# connection's ``function_update``.
# ---------------------------------------------------------------------------


def _coll_with_fake_connection(_no_op_bundle):
    """A Collection whose connection records the ``function_update``
    kwargs and stubs out the bundle upload + runtime-info calls."""
    seen: dict = {}

    class _FakeResponse:
        def json(self):
            return {"data": {"id": "bundle-xyz"}}

    class _FakeConnection:
        def runtime_info_get(self):
            class R:
                @staticmethod
                def json():
                    return {"data": {"python_versions": ["3.12"]}}

            return R()

        def function_upload_bundle(self, collection_name, bundle, raise_for_status):
            return _FakeResponse()

        def function_update(self, **kwargs):
            seen.update(kwargs)

    coll = ts.Collection.__new__(ts.Collection)
    coll.connection = _FakeConnection()
    coll.name = "my_coll"
    return coll, seen


def test_update_function_forwards_base_path_and_reset_flags(
    project_root, _no_op_bundle
):
    coll, seen = _coll_with_fake_connection(_no_op_bundle)
    info = _publisher_info()

    coll.update_function(
        "old_fn",
        info,
        project_root,
        base_path="/data/fn",
        reset_state=True,
        reset_stage=True,
    )

    assert seen["base_path"] == "/data/fn"
    assert seen["reset_state"] is True
    assert seen["reset_stage"] is True


def test_update_function_defaults_reset_flags_to_false(project_root, _no_op_bundle):
    coll, seen = _coll_with_fake_connection(_no_op_bundle)
    info = _publisher_info()

    coll.update_function("old_fn", info, project_root)

    # The SDK-level defaults: reset flags off, no base_path override.
    assert seen["reset_state"] is False
    assert seen["reset_stage"] is False
    assert seen["base_path"] is None


def _coll_with_fake_register_connection():
    """A Collection whose connection records the ``function_register``
    kwargs and stubs out the bundle upload + runtime-info calls."""
    seen: dict = {}

    class _FakeResponse:
        def json(self):
            return {"data": {"id": "bundle-xyz"}}

    class _FakeConnection:
        def runtime_info_get(self):
            class R:
                @staticmethod
                def json():
                    return {"data": {"python_versions": ["3.12"]}}

            return R()

        def function_upload_bundle(self, collection_name, bundle, raise_for_status):
            return _FakeResponse()

        def function_register(self, **kwargs):
            seen.update(kwargs)

    coll = ts.Collection.__new__(ts.Collection)
    coll.connection = _FakeConnection()
    coll.name = "my_coll"
    return coll, seen


def test_register_function_forwards_base_path(project_root, _no_op_bundle):
    coll, seen = _coll_with_fake_register_connection()
    info = _publisher_info()

    coll.register_function(info, project_root, base_path="/data/fn")

    assert seen["base_path"] == "/data/fn"


def test_register_function_base_path_defaults_none(project_root, _no_op_bundle):
    coll, seen = _coll_with_fake_register_connection()
    info = _publisher_info()

    coll.register_function(info, project_root)

    # No override supplied -> the server applies the collection default.
    assert seen["base_path"] is None
