from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote_plus

from tabsdatak.conn.mongodb import (
    DestCfgKey,
    DestCfgSpec,
    MongoDBConn,
    MongoDBDest,
)
from tabsdatak.conn.mongodb.error import MongodbErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    TableFileSpec,
)

if TYPE_CHECKING:
    from pymongo.client_session import ClientSession
    from pymongo.collection import Collection


logger = logging.getLogger(__name__)


# --- dest_cfg keys --------------------------------------------------
_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "mongodb.logging_level"

_DEST_CFGS_DEFAULTS: dict[DestCfgKey, Any] = {
    _KEY_DEST_LOGGING_LEVEL: logging.ERROR,
}


@dataclass(frozen=True)
class _WriterCfg:
    logging_level: int


def _resolve_dest_cfg(dest_cfg: DestCfgSpec | None) -> _WriterCfg:
    cfg = dest_cfg or {}
    for k in cfg:
        if k not in _DEST_CFGS_DEFAULTS:
            logger.warning(
                f"Unknown dest_cfg key {k!r}, ignoring (allowed: "
                f"{sorted(_DEST_CFGS_DEFAULTS)})"
            )

    def _get(key: DestCfgKey) -> Any:
        return cfg.get(key, _DEST_CFGS_DEFAULTS[key])

    return _WriterCfg(
        logging_level=int(_get(_KEY_DEST_LOGGING_LEVEL)),
    )


# --- Constants --------------------------------------------------------

# MongoDB's reserved primary-key field name. When a user-declared
# ``id_field`` is configured the plugin copies that field's value into
# ``_id`` before persisting so MongoDB treats it as the document key.
MONGODB_ID_COLUMN = "_id"

# MongoDB duplicate-key error code (E11000).
_DUP_KEY_CODE = 11000


# --- URI / client helpers --------------------------------------------


def _resolve_uri(conn: MongoDBConn) -> str:
    uri = conn.uri
    return uri if isinstance(uri, str) else uri.value()


def _uri_with_credentials(conn: MongoDBConn) -> str:
    uri = _resolve_uri(conn)
    user = quote_plus(conn.credentials.user.value())
    password = quote_plus(conn.credentials.password.value())
    return uri.replace("://", f"://{user}:{password}@", 1)


def _pymongo_kwargs(conn: MongoDBConn) -> dict[str, Any]:
    cfg = conn.conn_cfg or {}
    out: dict[str, Any] = {}
    for k, v in cfg.items():
        if k.startswith("pymongo."):
            out[k[len("pymongo.") :]] = v
    return out


def _build_client(conn: MongoDBConn) -> Any:
    import pymongo
    from pymongo.errors import PyMongoError

    uri = _uri_with_credentials(conn)
    kwargs = _pymongo_kwargs(conn)
    try:
        client = pymongo.MongoClient(uri, **kwargs)
        # The ping command is cheap and does not require auth -- it
        # surfaces connection failures eagerly rather than letting them
        # surface mid-write.
        client.admin.command("ping")
    except PyMongoError as e:
        raise MongodbErrorCode.MONGODB_19.exception(cause=e)
    except Exception as e:
        raise MongodbErrorCode.MONGODB_19.exception(cause=e)
    return client


# --- Collection helpers ----------------------------------------------


def _split_collection(value: str) -> tuple[str, str]:
    if "." not in value:
        raise MongodbErrorCode.MONGODB_22.exception(value=value)
    db, _, coll = value.partition(".")
    if not db or not coll:
        raise MongodbErrorCode.MONGODB_22.exception(value=value)
    return db, coll


def _ensure_clean_collection(collection: "Collection") -> None:
    if collection.count_documents({}):
        collection.drop()
    collection.insert_one({})
    collection.delete_one({})


# --- Row iteration ---------------------------------------------------


def _iter_parquet_documents(path: TableFileSpec):
    import polars as pl

    df = pl.read_parquet(str(path))
    yield from df.iter_rows(named=True)


# --- Plugin: destination ---------------------------------------------


class _MongoDBDestPlugin(DestPlugin[MongoDBDest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: MongoDBDest,
        tables: list[TableFileSpec],
    ) -> None:
        from pymongo.errors import BulkWriteError, PyMongoError

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)

        # Defensive recheck -- the framework already enforces
        # ``len(tables) == cardinality(dest)`` via the binding, but
        # the SPI is generic so we don't trust that here.
        if len(dest.collections_with_ids) != len(tables):
            raise MongodbErrorCode.MONGODB_18.exception(
                collections=len(dest.collections_with_ids),
                slots=len(tables),
            )

        conn = cast(MongoDBConn, dest.conn)
        client = _build_client(conn)
        try:
            session_cm = client.start_session()
            session = session_cm.__enter__()
            try:
                for table, (collection_fqn, id_field) in zip(
                    tables, dest.collections_with_ids,
                ):
                    self._write_one(
                        client=client,
                        session=session if dest.use_trxs else None,
                        dest=dest,
                        collection_fqn=collection_fqn,
                        id_field=id_field,
                        table=table,
                    )
            finally:
                session_cm.__exit__(None, None, None)
        except (BulkWriteError, PyMongoError):
            # ``_write_one`` already wrapped these as
            # ``MONGODB_20`` -- a bare ``PyMongoError`` here only
            # reaches us when the session itself fails, which we
            # treat as the same write-side failure category.
            raise
        finally:
            client.close()

    # --- Per-collection writer ----------------------------------------

    def _write_one(
        self,
        *,
        client: Any,
        session: "ClientSession | None",
        dest: MongoDBDest,
        collection_fqn: str,
        id_field: str | None,
        table: TableFileSpec,
    ) -> None:

        db_name, coll_name = _split_collection(collection_fqn)
        database = client[db_name]
        collection = database.get_collection(coll_name)

        if dest.if_collection_exists == "replace":
            _ensure_clean_collection(collection)

        # Build the bulk-write operations from the parquet rows. The
        # batch is broken up into ``docs_per_trx``-sized chunks so a
        # single transaction never grows unbounded.
        batch: list[Any] = []
        for doc in _iter_parquet_documents(table):
            op = self._build_op(
                doc=doc,
                id_field=id_field,
                update_existing=dest.update_existing,
                collection_fqn=collection_fqn,
            )
            batch.append(op)
            if len(batch) >= dest.docs_per_trx:
                self._flush_batch(
                    collection=collection,
                    session=session,
                    batch=batch,
                    dest=dest,
                    collection_fqn=collection_fqn,
                )
                batch = []
        if batch:
            self._flush_batch(
                collection=collection,
                session=session,
                batch=batch,
                dest=dest,
                collection_fqn=collection_fqn,
            )

    # --- Build a single bulk_write op --------------------------------

    @staticmethod
    def _build_op(
        *,
        doc: dict,
        id_field: str | None,
        update_existing: bool,
        collection_fqn: str,
    ) -> Any:
        import pymongo

        if id_field is None:
            # No declared id -- let MongoDB auto-generate ``_id``.
            return pymongo.InsertOne(doc)

        if id_field not in doc:
            raise MongodbErrorCode.MONGODB_21.exception(
                id_field=id_field,
                collection=collection_fqn,
            )
        doc[MONGODB_ID_COLUMN] = doc[id_field]

        if update_existing:
            # Upsert -- update if present, insert if not.
            return pymongo.UpdateOne(
                {MONGODB_ID_COLUMN: doc[MONGODB_ID_COLUMN]},
                {"$set": doc},
                upsert=True,
            )
        # Strict insert -- duplicate keys propagate up as
        # ``BulkWriteError`` and are handled by ``_flush_batch``.
        return pymongo.InsertOne(doc)

    # --- Persist a batch ----------------------------------------------

    def _flush_batch(
        self,
        *,
        collection: "Collection",
        session: "ClientSession | None",
        batch: list[Any],
        dest: MongoDBDest,
        collection_fqn: str,
    ) -> None:
        from pymongo.errors import BulkWriteError, PyMongoError

        if dest.use_trxs and session is not None:
            session.start_transaction()
        try:
            collection.bulk_write(
                batch,
                session=session,
                ordered=dest.maintain_order,
            )
            if dest.use_trxs and session is not None:
                session.commit_transaction()
            if dest.log_intermediate_files:
                logger.debug(
                    "MongoDB flushed %d docs into %s",
                    len(batch),
                    collection_fqn,
                )
        except BulkWriteError as e:
            if dest.use_trxs and session is not None:
                session.abort_transaction()
            if not dest.fail_on_duplicate_key and _all_dup_key_errors(e):
                logger.warning(
                    "MongoDB duplicate-key errors swallowed by"
                    " fail_on_duplicate_key=False (collection=%s)",
                    collection_fqn,
                )
                return
            raise MongodbErrorCode.MONGODB_20.exception(
                cause=e,
                collection=collection_fqn,
            )
        except PyMongoError as e:
            if dest.use_trxs and session is not None:
                session.abort_transaction()
            raise MongodbErrorCode.MONGODB_20.exception(
                cause=e,
                collection=collection_fqn,
            )


def _all_dup_key_errors(e: Any) -> bool:
    details = getattr(e, "details", None) or {}
    errors = details.get("writeErrors") or []
    if not errors:
        return False
    return all(err.get("code") == _DUP_KEY_CODE for err in errors)


# --- Explorer ---------------------------------------------------------


class _MongoDBExplorer(Explorer[MongoDBConn]):
    def metadata(self, conn: MongoDBConn) -> Any:
        # TODO
        return None

    def data(self, conn: MongoDBConn, query: str) -> Any:
        # TODO
        return None


# --- Binding ----------------------------------------------------------


MONGODB_DEST = DestDef(
    conn=MongoDBConn,
    dest_type="mongodb",
    dest_version="v1",
    dest=MongoDBDest,
    cardinality=lambda dest: len(dest.collections_with_ids),
    plugin=_MongoDBDestPlugin,
    explorer=_MongoDBExplorer,
)
