#
# Copyright 2026 Tabsdata Inc.
#

"""Concrete :class:`StreamSrcPluginCtx` for the streamer pod.

Implements the runtime side of the
:class:`tabsdatak.spi.StreamSrcPluginCtx` contract. The streamer pod
hands an instance to a :class:`tabsdatak.spi.StreamSrcPlugin`'s
``run(ctx, conn, stream_src)`` and the plugin drives a long-running
loop, calling ``ctx.stage(...)`` once per rollover and exiting when
``ctx.active`` flips to ``False``.

Layout (single source of truth: :mod:`stage_layout`):

    <stage_root>/
        stream-state.json                    <- envelope of the inner state
        manifests/
            stream-manifest-<TS_ISO8601>.json
        files/
            <TS_ISO8601>/
                <filename>.parquet           <- uploaded from plugin's work_dir

All writes go through a :class:`StageBackend` resolved from
``function_stage`` (``stage_backend_from_location``): a local-filesystem
backend (``shutil.move`` / direct reads) for ``file://`` stages, or a
transporter-backed backend for cloud stages (``s3://`` / ``az://`` /
``gs://``). Callers never branch on scheme.

Responsibilities on each :meth:`StreamerPluginCtx.stage` call:

  1. Upload parquets from the plugin's ``work_dir`` into
     ``<stage_root>/files/<TS>/`` via the backend.
  2. Emit ``<stage_root>/manifests/stream-manifest-<TS>.json`` listing
     the moved files as paths *relative to ``stage_root``* (so the
     publisher's ``ManifestStageSrcPlugin`` can resolve them against
     its conn's ``base_path``).
  3. Persist the new resume state, via two branches that match the
     two consumption-loop shapes in the TED:

     * a :class:`tabsdatak.api.BasicDictSpec` (a plain ``dict``) --
       Tabsdata owns the external-system offset. We persist the dict
       into the envelope at ``<stage_root>/stream-state.json``.

     * a :class:`tabsdatak.spi.CommitStateFn` (any callable) -- the
       external system owns the offset. We invoke the callable *first*
       (so a failure in committing to the external system leaves us with
       no Tabsdata-side state, and the plugin's ``ctx.stream_state is
       None`` branch on next start replays from ``config.initial_values``
       -- at-least-once semantics by design). Once the callable returns,
       we persist an empty-dict envelope as the first-invocation marker.

  4. Flip :attr:`first_stage` to ``False``.
"""

from __future__ import annotations

import json
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tabsdatak.api import BasicDictSpec
from tabsdatak.conn.stream.stage._backend import StageBackend
from tabsdatak.server._tdrunner.function._lib.request import Location
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromFlowRequestCtx
from tabsdatak.server._tdrunner.function._lib.stage_backend import (
    stage_backend_from_location,
)
from tabsdatak.server._tdrunner.function._lib.stage_layout import (
    FILES_SUBDIR,
    MANIFESTS_SUBDIR,
    STREAM_STATE_FILENAME,
    InvalidStateEnvelopeError,
    StreamStateView,
    format_state_envelope,
    parse_state_envelope,
)
from tabsdatak.server._tdrunner.function._lib.stage_manifest import (
    external_uid_from_filename,
    format_manifest_filename,
)
from tabsdatak.spi import StreamSrcPluginCtx, StreamStateSpec

logger = logging.getLogger(__name__)


def _load_state(backend: StageBackend) -> StreamStateView | None:
    """Read the prior ``stream-state.json`` from the stage if present.

    Returns ``None`` when:
      * the file does not exist (streamer has never run for this
        function before, or the stage was wiped)
      * the file is unreadable / malformed JSON (log + fall back to
        first-run semantics — the plugin's ``config.initial_values``
        branch is the recovery path; better than fail-looping the pod)
      * the file's envelope is structurally wrong (wrong ``kind`` /
        unknown ``apiVersion`` / missing ``state`` -- same recovery)

    The read goes through the backend (a direct file read for a local
    stage; a transporter download for a cloud stage), so a missing or
    unreadable object surfaces as ``None`` -> first-run semantics.

    A successful parse returns the full :class:`StreamStateView`; the
    caller exposes only the inner ``state`` dict (which may be ``{}``,
    the first-invocation marker) via :attr:`StreamerPluginCtx.stream_state`.
    """
    raw_text = backend.read_text(STREAM_STATE_FILENAME)
    if raw_text is None:
        return None
    try:
        raw = json.loads(raw_text)
    except ValueError:
        logger.exception(
            "stream_src_ctx: could not parse %s; treating as first-stage",
            STREAM_STATE_FILENAME,
        )
        return None
    if not isinstance(raw, dict):
        logger.warning(
            "stream_src_ctx: %s contains %s, expected dict envelope; "
            "treating as first-stage",
            STREAM_STATE_FILENAME,
            type(raw).__name__,
        )
        return None
    try:
        return parse_state_envelope(raw)
    except InvalidStateEnvelopeError:
        logger.exception(
            "stream_src_ctx: %s has unexpected envelope shape; treating as first-stage",
            STREAM_STATE_FILENAME,
        )
        return None


class StreamerPluginCtx(FromFlowRequestCtx, StreamSrcPluginCtx):
    """Concrete :class:`StreamSrcPluginCtx` for the streamer pod.

    Multiple-inheritance composition mirrors the SPI: a streamer is
    :class:`StreamSrcPluginCtx`, which extends :class:`PluginCtx`
    (= :class:`FlowCtx` + ``work_dir``) but NOT :class:`TrxCtx` -- a
    streamer is a long-lived worker, not part of any execution plan or
    transaction. The downstream per-batch publisher pods (spawned by
    the manifest-trigger sidecar via ``fn_trigger``) are where real
    ``execution_id`` / ``transaction_id`` / ``function_run_id`` values
    live; the streamer pod itself has none. This is why the runtime
    mixin used here is :class:`FromFlowRequestCtx` rather than the
    full :class:`FromRequestCtx` used by ``_SrcContext`` /
    ``_DestContext`` / ``_FunctionContext`` -- those role-typed
    contexts inherit :class:`TrxCtx`, so they get the trx fields too.
    """

    def __init__(
        self,
        *,
        work_dir: Path,
        collection_id: str,
        collection_name: str,
        function_id: str,
        function_name: str,
        function_stage: Location,
        flush_interval_mins: int,
        tabsdata_version: str,
    ) -> None:
        super().__init__(
            work_dir=work_dir,
            collection_id=collection_id,
            collection_name=collection_name,
            function_id=function_id,
            function_name=function_name,
        )
        self._backend = stage_backend_from_location(function_stage)
        self._flush_interval_mins = flush_interval_mins
        self._tabsdata_version = tabsdata_version
        self._active = True
        self._first_stage = True
        self._state_view: StreamStateView | None = _load_state(self._backend)

    # --- StreamSrcPluginCtx contract ----------------------------------

    @property
    def active(self) -> bool:
        return self._active

    @property
    def first_stage(self) -> bool:
        return self._first_stage

    @property
    def stream_state(self) -> BasicDictSpec | None:
        """The inner state dict, or ``None`` if no state file exists.

        Note ``{}`` is a meaningful, non-``None`` value: it's the
        first-invocation marker for the lambda-callback branch (see
        :meth:`stage`). The plugin's consumption-loop distinguishes
        ``None`` ("first run, seed from config") from ``{}`` ("we've
        already committed once via the external system, don't re-seed")
        on its own.
        """
        if self._state_view is None:
            return None
        return self._state_view.state

    @property
    def flush_interval_mins(self) -> int:
        return self._flush_interval_mins

    def stage(
        self,
        tables_files: list[list[Path]],
        new_state: StreamStateSpec,
    ) -> None:
        """Publish a batch and commit its new resume state.

        See the module docstring for the side-effect order and the
        rationale around at-least-once semantics on the lambda branch.

        Raises:
            FileNotFoundError: any path in ``tables_files`` does not
                exist in the plugin's ``work_dir``.
        """
        # The batch timestamp drives both the manifest filename and the
        # per-batch files subdir, so the two share a name. It must be
        # fixed up front: the files upload into files/<external_id>/ before
        # the manifest that records their paths is written.
        ts = datetime.now(tz=timezone.utc)
        manifest_filename = format_manifest_filename(ts)
        external_id = external_uid_from_filename(manifest_filename)

        # 1. Upload files to <stage>/files/<external_id>/ via the backend
        # (a rename for a local stage; a transporter copy for cloud). The
        # relative-to-stage-root path the manifest stores is what the
        # publisher's ManifestStageSrcPlugin resolves against its conn's
        # base_path (the stage root).
        moved: list[list[str]] = []
        for slot_idx, slot in enumerate(tables_files):
            moved_slot: list[str] = []
            for file_idx, src_path in enumerate(slot):
                if not src_path.is_file():
                    raise FileNotFoundError(
                        f"plugin staged file slot {slot_idx} file {file_idx} "
                        f"({src_path}) does not exist"
                    )
                rel = f"{FILES_SUBDIR}/{external_id}/{src_path.name}"
                self._backend.upload(src_path, rel)
                moved_slot.append(rel)
            moved.append(moved_slot)

        # 2. Upload the manifest under <stage>/manifests/. Its filename TS
        # portion is the external_id we paired the files subdir with above
        # and that we cross-reference the state file with, so an operator
        # can tie state ↔ manifest ↔ files together. Uploaded after the
        # files so a visible manifest never points at not-yet-staged data.
        self._upload_text(f"{MANIFESTS_SUBDIR}/{manifest_filename}", json.dumps(moved))
        logger.info(
            "stream_src_ctx.stage: wrote manifest %s with %d slot(s); "
            "files per slot=%s",
            manifest_filename,
            len(moved),
            [len(s) for s in moved],
        )

        # 3. Persist new state. Two branches per the TED.
        if isinstance(new_state, dict):
            # Tabsdata owns the offset: ``new_state`` is the resume-state
            # dict, which we persist into the envelope (persistence is
            # what counts -- there is no side-effect callable here).
            inner_state = dict(new_state)
            self._write_state(inner_state, external_id=external_id)
            self._state_view = StreamStateView(
                state=inner_state,
                external_id=external_id,
                tabsdata_version=self._tabsdata_version,
                kind="streamState",
                api_version=1,
            )
            logger.debug(
                "stream_src_ctx.stage: persisted stream state; %d key(s)",
                len(inner_state),
            )
        else:
            # External system owns the offset. Invoke the callable
            # *first* -- if it raises, the marker is not written and
            # the plugin's next start re-seeds from config.initial_values
            # (at-least-once). On success, write the empty-dict marker
            # so a subsequent restart distinguishes "I've run before
            # for this stream" from "fresh install".
            new_state()
            self._write_state({}, external_id=external_id)
            self._state_view = StreamStateView(
                state={},
                external_id=external_id,
                tabsdata_version=self._tabsdata_version,
                kind="streamState",
                api_version=1,
            )
            logger.debug(
                "stream_src_ctx.stage: persisted first-invocation marker "
                "(external-offset path)"
            )

        # 4. Flip the first-stage flag.
        self._first_stage = False

    # --- streamer-pod-only surface -----------------------------------

    def mark_inactive(self) -> None:
        """Flip :attr:`active` to ``False`` so the plugin's poll loop
        exits at its next ``ctx.active`` check.

        Called by the streamer command's signal handler (SIGTERM /
        SIGINT) so the pod can shut down cleanly: the plugin may
        emit one last :meth:`stage` to flush its buffer, then
        ``run()`` returns and the streamer container exits 0.
        """
        self._active = False

    # --- private helpers ---------------------------------------------

    def _write_state(self, state: dict[str, Any], *, external_id: str) -> None:
        """Write ``<stage>/stream-state.json`` with the envelope around
        ``state`` (via the backend: a rename for local, a transporter
        copy for cloud -- both atomic at the destination)."""
        envelope = format_state_envelope(
            state,
            external_id=external_id,
            tabsdata_version=self._tabsdata_version,
        )
        self._upload_text(STREAM_STATE_FILENAME, json.dumps(envelope))

    def _upload_text(self, rel: str, text: str) -> None:
        """Serialise ``text`` to a temp file and upload it to the stage at
        ``rel`` -- the path the control files (manifest, state) take to
        the stage regardless of backend."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir) / "upload"
            tmp.write_text(text, encoding="utf-8")
            self._backend.upload(tmp, rel)


__all__ = [
    "StreamerPluginCtx",
]
