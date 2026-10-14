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
    )  # CommitSeqOffset.update: unexpected offset type
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
        "An initial load read its resume position before every table was "
        "loaded, so the position cannot be trusted",
    )  # SqlReplicaInitialLoadSession.offset
    DB2_26 = ErrorDef(
        Db2RuntimeException,
        "The Capture program did not reach log position {lsn} within "
        "{timeout}s after the initial load, so the load cannot be handed over "
        "to the change stream (is asncap running?)",
    )  # SqlReplicaInitialLoadSession._wait_for_capture
    DB2_27 = ErrorDef(
        Db2RuntimeException,
        "Could not read the current log position from "
        "MON_GET_TRANSACTION_LOG: {reason}. The connection user needs EXECUTE "
        "on SYSPROC.MON_GET_TRANSACTION_LOG or SYSMON authority",
    )  # SqlReplicaInitialLoadSession._current_lsn
    DB2_28 = ErrorDef(
        Db2RuntimeException,
        "The stream state holds an offset this connector cannot read (it needs "
        "a map of CD tables to 16-byte hex commit sequences): {state!r}. Run "
        "the initial load again",
    )  # CommitSeqOffset.read
    DB2_29 = ErrorDef(
        Db2RuntimeException,
        "CD table {table!r} has no registration in IBMSNAP_REGISTER: it was "
        "deregistered, so the changes since are lost to this stream. Register "
        "it again and run the initial load again",
    )  # verify_positions
    DB2_30 = ErrorDef(
        Db2RuntimeException,
        "The registration of CD table {table!r} is not active (state {state!r}, "
        "info {info!r}), so Capture is not recording its changes. Reactivate it "
        "and run the initial load again",
    )  # verify_positions
    DB2_31 = ErrorDef(
        Db2RuntimeException,
        "Capture has captured CD table {table!r} without a break only from "
        "{start}, past the stream's position {position}: the registration was "
        "restarted, Capture cold-started, or retention-limit pruning removed "
        "rows the stream had not read. Run the initial load again",
    )  # verify_positions
    DB2_32 = ErrorDef(
        Db2RuntimeException,
        "The subscriptions to CD table {table!r} let Capture prune it up to "
        "{bound}, past the stream's position {position}, so rows the stream had "
        "not read may be gone. Run the initial load again",
    )  # verify_positions
    DB2_33 = ErrorDef(
        Db2RuntimeException,
        "The stream has no position for CD table {table!r}: it was added after "
        "the stream started, or left out of start_from. Run the initial load "
        "again",
    )  # verify_positions
    DB2_34 = ErrorDef(
        Db2RuntimeException,
        "Capture started capturing CD table {table!r} at {start}, after the "
        "initial load's log position {lsn}: the changes in between are in "
        "neither the load nor the change stream. Run the initial load again",
    )  # SqlReplicaInitialLoadSession._pin
    DB2_35 = ErrorDef(
        Db2RuntimeException,
        "CD table {table!r} is registered to capture every update as a delete and "
        "an insert (CHG_UPD_TO_DEL_INS = 'Y'), which an initial load cannot "
        "deduplicate: a key swap in one statement would lose a row",
    )  # SqlReplicaInitialLoadSession._pin
    DB2_36 = ErrorDef(
        Db2RuntimeException,
        "Capture cold-started at {first}, past the stream's position {position} "
        "for CD table {table!r}: a cold start empties every CD table and captures "
        "only from that point, so changes the stream had not read, and any made "
        "while Capture was down, are gone. Run the initial load again",
    )  # verify_positions
