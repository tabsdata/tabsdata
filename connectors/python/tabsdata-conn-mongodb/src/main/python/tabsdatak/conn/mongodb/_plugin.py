from __future__ import annotations

import functools
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.mongodb import (
    DestCfgKey,
    DestCfgSpec,
    MongoDBDest,
    MongoDBDestConn,
)
from tabsdatak.conn.mongodb._explorer._explorer import MongoDBConnectionExplorer
from tabsdatak.conn.mongodb.error import MongodbErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
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


def _uri_with_credentials(conn: MongoDBDestConn) -> str:
    """Inject the user/password from ``conn.credentials`` into ``conn.uri``.

    Mirrors the legacy ``_connector.py`` behaviour: take the first
    ``://`` separator and splice ``user:password@`` after it. The
    credentials are URL-encoded with ``quote_plus`` so special chars
    (``@``, ``:``, ``/``, ...) round-trip safely.
    """
    uri = conn._uri()
    user = quote_plus(_convert(conn.credentials.user, str))
    password = quote_plus(_convert(conn.credentials.password, str))
    return uri.replace("://", f"://{user}:{password}@", 1)


def _pymongo_kwargs(conn: MongoDBDestConn) -> dict[str, Any]:
    """Extract ``pymongo.MongoClient`` kwargs from ``conn.conn_cfg``.

    Recognises keys prefixed with ``"pymongo."`` per the connector
    cfg convention; strips the prefix before forwarding.
    """
    cfg = conn.conn_cfg or {}
    out: dict[str, Any] = {}
    for k, v in cfg.items():
        if k.startswith("pymongo."):
            out[k[len("pymongo.") :]] = v
    return out


def _build_client(conn: MongoDBDestConn) -> Any:
    """Build a ``pymongo.MongoClient`` and verify connectivity.

    Wraps every connect / ping failure as ``MONGODB_19`` so the runtime
    sees a single, well-typed error category for "couldn't reach
    MongoDB".
    """
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


def _split_collection(value: str, default_db: str | None) -> tuple[str, str]:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common._validation import complete_levels

    if "." in value:
        head, _, tail = value.partition(".")
        parts: list[str | None] = [head, tail]
    else:
        parts = [None, value]
    db, coll = complete_levels(parts, [default_db, None])
    if not db or not coll:
        raise MongodbErrorCode.MONGODB_22.exception(value=value)
    return db, coll


def _default_database_name(client: Any) -> str | None:
    try:
        db = client.get_default_database(default=None)
    except Exception:
        return None
    return db.name if db is not None else None


@functools.lru_cache(maxsize=1)
def _decimal_codec_options() -> Any:
    """CodecOptions that BSON-encode ``decimal.Decimal`` as ``Decimal128``.

    Polars materialises parquet Decimal columns as Python ``decimal.Decimal``
    (via ``iter_rows``), which pymongo cannot BSON-encode natively. A
    ``TypeRegistry`` codec maps every such value in the document -- including
    values nested inside sub-documents and lists -- to ``bson.Decimal128``.
    """
    import decimal

    from bson.codec_options import CodecOptions, TypeCodec, TypeRegistry
    from bson.decimal128 import Decimal128

    class _DecimalCodec(TypeCodec):
        python_type = decimal.Decimal
        bson_type = Decimal128

        def transform_python(self, value: decimal.Decimal) -> Decimal128:
            return Decimal128(value)

        def transform_bson(self, value: Decimal128) -> decimal.Decimal:
            return value.to_decimal()

    return CodecOptions(type_registry=TypeRegistry([_DecimalCodec()]))


def _ensure_clean_collection(collection: "Collection") -> None:
    """Implement ``if_collection_exists="replace"``.

    Drop the collection if it has any documents, then insert + delete
    a sentinel so the collection materialises (collections can't be
    created inside a transaction).
    """
    if collection.count_documents({}):
        collection.drop()
    collection.insert_one({})
    collection.delete_one({})


# --- Row iteration ---------------------------------------------------


def _iter_parquet_documents(path: TableFileSpec):
    """Yield ``dict`` documents from a parquet file using polars.

    Polars is the project's preferred dataframe lib and handles
    nested / list / struct columns when reading parquet. ``to_dicts()``
    materialises the column-major frame into row-major dicts ready
    for ``bulk_write``.
    """
    import polars as pl

    df = pl.read_parquet(str(path))
    yield from df.iter_rows(named=True)


# --- Plugin: destination ---------------------------------------------


class _MongoDBDestPlugin(DestPlugin[MongoDBDestConn, MongoDBDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: MongoDBDestConn,
        dest: MongoDBDest,
        tables: list[TableFileSpec],
    ) -> None:
        from pymongo.errors import BulkWriteError, PyMongoError

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info(
            "mongodb: write_out starting (%d collection(s))",
            len(dest.collections_with_ids or []),
        )

        # Defensive recheck -- the framework already enforces
        # ``len(tables) == cardinality(dest)`` via the binding, but
        # the SPI is generic so we don't trust that here.
        if len(dest.collections_with_ids) != len(tables):
            raise MongodbErrorCode.MONGODB_18.exception(
                collections=len(dest.collections_with_ids),
                slots=len(tables),
            )

        logger.debug("mongodb: establishing pymongo client")
        client = _build_client(conn)
        default_db = _default_database_name(client)
        total = len(dest.collections_with_ids)
        try:
            session_cm = client.start_session()
            session = session_cm.__enter__()
            try:
                for i, (table, (collection_fqn, id_field)) in enumerate(
                    zip(
                        tables,
                        dest.collections_with_ids,
                    ),
                    1,
                ):
                    logger.info(
                        "mongodb: writing collection %d/%d: %r",
                        i,
                        total,
                        collection_fqn,
                    )
                    self._write_one(
                        client=client,
                        session=session if dest.use_trxs else None,
                        dest=dest,
                        collection_fqn=collection_fqn,
                        default_db=default_db,
                        id_field=id_field,
                        table=table,
                    )
                    logger.debug("mongodb: finished collection %r", collection_fqn)
            finally:
                session_cm.__exit__(None, None, None)
        except (BulkWriteError, PyMongoError):
            # ``_write_one`` already wrapped these as
            # ``MONGODB_20`` -- a bare ``PyMongoError`` here only
            # reaches us when the session itself fails, which we
            # treat as the same write-side failure category.
            raise
        finally:
            logger.debug("mongodb: closing pymongo client")
            client.close()

    # --- Per-collection writer ----------------------------------------

    def _write_one(
        self,
        *,
        client: Any,
        session: "ClientSession | None",
        dest: MongoDBDest,
        collection_fqn: str,
        default_db: str | None,
        id_field: str | None,
        table: TableFileSpec,
    ) -> None:

        db_name, coll_name = _split_collection(collection_fqn, default_db)
        database = client[db_name]
        collection = database.get_collection(
            coll_name, codec_options=_decimal_codec_options()
        )

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
    """Return True iff every writeError in the bulk-write error is
    a duplicate-key (code 11000)."""
    details = getattr(e, "details", None) or {}
    errors = details.get("writeErrors") or []
    if not errors:
        return False
    return all(err.get("code") == _DUP_KEY_CODE for err in errors)


# --- Binding ----------------------------------------------------------


MONGODB_DEST = DestDef(
    conn=MongoDBDestConn,
    type_="mongodb-out",
    system="MongoDB",
    dest_version="v1",
    dest=MongoDBDest,
    cardinality=lambda dest: len(dest.collections_with_ids),
    plugin=_MongoDBDestPlugin,
    explorer=MongoDBConnectionExplorer,
    icon="conn/mongodb/resources/mongodb.svg",
)
