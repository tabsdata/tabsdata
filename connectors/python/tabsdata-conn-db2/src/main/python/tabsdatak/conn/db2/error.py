#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class Db2ValidateException(ConnException):
    """Cross-field validation failure for the Db2 connector.

    :categories: error
    """


class Db2RuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Db2 connector.

    :categories: error
    """


class Db2ErrorCode(ErrorCode):
    """Error codes for the Db2 connector.

    :categories: error

    One variant per validation / runtime site (batch and CDC). Raise via
    `Db2ErrorCode.DB2_N.exception`.
    """

    DB2_1 = ErrorDef(
        ConnInitException,
        "Db2 connection validation failed",
    )  # Db2SrcConn / Db2DestConn
    DB2_2 = ErrorDef(
        ConnInitException,
        "Db2Src validation failed",
    )  # Db2Src
    DB2_3 = ErrorDef(
        Db2ValidateException,
        "Db2Src.schema_overrides length {overrides} must equal queries"
        " length {queries}",
    )  # Db2Src.validate()
    DB2_4 = ErrorDef(
        ConnInitException,
        "Db2Dest validation failed",
    )  # Db2Dest
    DB2_5 = ErrorDef(
        Db2RuntimeException,
        "Db2 source connection failed (uri={uri})",
    )  # _Db2SrcPlugin.read_in: engine / connect failure
    DB2_6 = ErrorDef(
        Db2RuntimeException,
        "Db2 query failed (query_index={query_index})",
    )  # _Db2SrcPlugin: read failure inside read_in
    DB2_7 = ErrorDef(
        Db2RuntimeException,
        "Db2Dest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _Db2DestPlugin: defensive recheck
    DB2_8 = ErrorDef(
        Db2RuntimeException,
        "Db2 destination connection failed (uri={uri})",
    )  # _Db2DestPlugin.write_out: engine / connect failure
    DB2_9 = ErrorDef(
        Db2RuntimeException,
        "Db2 write failed (table={table})",
    )  # _Db2DestPlugin: write failure inside write_out

    # --- CDC --------------------------------------------------------

    DB2_10 = ErrorDef(
        ConnInitException,
        "Db2CdcStreamSrcConn validation failed",
    )  # Db2CdcStreamSrcConn dataclass tag
    DB2_11 = ErrorDef(
        ConnInitException,
        "Db2CdcStreamSrc validation failed",
    )  # Db2CdcStreamSrc dataclass tag
    DB2_12 = ErrorDef(
        ConnInitException,
        "Cannot create CommitSeqPosition",
    )  # CommitSeqPosition dataclass tag
    DB2_13 = ErrorDef(
        ConnInitException,
        "Cannot create TableCommitSeqPosition",
    )  # TableCommitSeqPosition dataclass tag
    DB2_14 = ErrorDef(
        ConnInitException,
        "Cannot create TimestampPosition",
    )  # db2-local TimestampPosition dataclass tag
    DB2_15 = ErrorDef(
        Db2RuntimeException,
        "Db2 SQL replication init failed (start_from={start_from!r})",
    )  # SqlReplicaIngester: cannot translate start_from
    DB2_16 = ErrorDef(
        Db2RuntimeException,
        "Db2 capture registration missing for cd_owner={cd_owner!r} "
        "cd_table={cd_table!r} in ASN.IBMSNAP_REGISTER",
    )  # gather_table_identity: CD table not registered
    DB2_17 = ErrorDef(
        Db2RuntimeException,
        "Db2 source table missing: {schema!r}.{table!r}",
    )  # gather_table_identity: source table no longer exists
    DB2_18 = ErrorDef(
        Db2RuntimeException,
        "Db2 CD before-image prefix missing on registration "
        "(cd_owner={cd_owner!r} cd_table={cd_table!r}); UPDATE "
        "pre-images cannot be matched",
    )  # match_cd_columns: registration has no IBMSNAP_REGISTER.before_img_prefix
    DB2_19 = ErrorDef(
        Db2RuntimeException,
        "Db2 CD column matching failed -- unmatched before-image "
        "columns {unmatched_before!r} (after-image: {unmatched_after!r}, "
        "prefix={prefix!r})",
    )  # match_cd_columns: orphaned before-image columns
    DB2_20 = ErrorDef(
        Db2RuntimeException,
        "Db2 CD column matching failed -- after-image columns "
        "{unmatched_after!r} have no before-image sibling under "
        "prefix={prefix!r}",
    )  # match_cd_columns: orphaned after-image columns
    DB2_21 = ErrorDef(
        Db2RuntimeException,
        "Db2 sqlreplica offset type mismatch (got {actual!r})",
    )  # commit_offset / CommitSeqOffset.update: unexpected offset type
    DB2_22 = ErrorDef(
        Db2RuntimeException,
        "Db2 sqlreplica event: unknown IBMSNAP_OPERATION {op!r} for {table!r}",
    )  # poll_changes: row carries an unrecognised operation code
    DB2_23 = ErrorDef(
        Db2RuntimeException,
        "Db2 commit sequence {commitseq!r} exceeds the {digits}-digit decimal "
        "limit of the offset position column for {table!r}",
    )  # poll_changes: IBMSNAP_COMMITSEQ too large for decimal128 position

    # --- Field-value rules (tabsdatak.conn.db2) ---------------------
    DB2_24 = ErrorDef(
        ConnInitException,
        "DB2 URI must start with db2:// or ibm_db_sa:// (optionally with a "
        "+driver suffix): {value!r}",
    )  # UriSpec
    DB2_25 = ErrorDef(
        Db2RuntimeException,
        "An initial load read its resume position before pinning the source "
        "tables, so the position cannot be trusted",
    )  # SqlReplicaInitialLoadSession.offset
