#
# Copyright 2026 Tabs Data Inc.
#

"""Development connectors — public API.

Connectors meant for local development and testing, backed by no
external system:

* :class:`TrashDestConn` / :class:`TrashDest` -- a sink to nowhere;
  the subscriber's frame is discarded.
* :class:`DataGenSrcConn` / :class:`DataGenSrc` -- a source that emits
  in-line :class:`Data` (a Polars or Pandas dataframe), one parquet
  slot per :class:`Data`. Each :class:`Data` carries its dataframe as
  base64-encoded parquet on the wire (see :class:`Data`); the combined
  encoded size of all dataframes may not exceed 10 MB.
"""

from __future__ import annotations

import base64
import io
from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import (
    Dest,
    Src,
)
from tabsdatak.conn.dev.error import DevErrorCode
from tabsdatak.spi import Conn

# Combined budget for every dataframe carried by a ``DataGenSrc``,
# measured as the length of its base64-encoded parquet payload.
MAX_DATA_BYTES = 10 * 1024 * 1024


def _to_polars(df: Any) -> Any:
    import polars as pl

    if isinstance(df, pl.DataFrame):
        return df
    try:
        import pandas as pd
    except ImportError:
        pd = None
    if pd is not None and isinstance(df, pd.DataFrame):
        return pl.from_pandas(df)
    raise DevErrorCode.DEV_5.exception(type=type(df).__name__)


class Data:
    """One dataframe emitted by :class:`DataGenSrc`, optionally chunked.

    A ``Data`` wraps a Polars or Pandas dataframe (a Pandas frame is
    converted to Polars on construction) plus a per-run ``chunk_size``:

    * ``chunk_size=None`` -- the whole dataframe is emitted on every run.
    * ``chunk_size=N`` -- ``N`` rows are emitted per run, advancing an
      offset kept in the plugin's per-flow state; once the offset reaches
      the end the slot yields ``None`` (no more rows).

    On the wire a ``Data`` serialises to ``{"parquet": <base64>,
    "chunk_size": <int|null>}``: the dataframe is written to parquet and
    base64-encoded on serialisation, and decoded back to a Polars frame
    on deserialisation so the plugin can materialise it to parquet for
    its output. Like :class:`~tabsdatak.api.Secret`, it round-trips via
    its own pydantic core schema rather than the descriptor envelope.
    """

    def __init__(self, df: Any, chunk_size: int | None = None):
        if chunk_size is not None and (
            isinstance(chunk_size, bool)
            or not isinstance(chunk_size, int)
            or chunk_size <= 0
        ):
            raise DevErrorCode.DEV_6.exception(value=repr(chunk_size))
        self._frame = _to_polars(df)
        self.chunk_size = chunk_size
        self._parquet_b64: str | None = None

    @property
    def frame(self) -> Any:
        """The wrapped dataframe as a Polars ``DataFrame``."""
        return self._frame

    def parquet_b64(self) -> str:
        """The dataframe as base64-encoded parquet (computed once)."""
        if self._parquet_b64 is None:
            buffer = io.BytesIO()
            self._frame.write_parquet(buffer)
            self._parquet_b64 = base64.b64encode(buffer.getvalue()).decode("ascii")
        return self._parquet_b64

    def _to_serialized(self) -> dict[str, Any]:
        return {"parquet": self.parquet_b64(), "chunk_size": self.chunk_size}

    @classmethod
    def _from_serialized(cls, d: dict[str, Any]) -> "Data":
        import polars as pl

        raw = base64.b64decode(d["parquet"])
        frame = pl.read_parquet(io.BytesIO(raw))
        return cls(frame, chunk_size=d.get("chunk_size"))

    def __eq__(self, other: Any) -> bool:
        return (
            type(self) is type(other)
            and self.chunk_size == other.chunk_size
            and self._frame.equals(other._frame)
        )

    def __repr__(self) -> str:
        return f"Data(shape={self._frame.shape}, chunk_size={self.chunk_size})"

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        def validate(value: Any) -> Any:
            if isinstance(value, cls):
                return value
            if isinstance(value, dict):
                return cls._from_serialized(value)
            raise ValueError(
                f"{cls.__name__} expects a {cls.__name__} instance or a "
                f"serialized dict (got {type(value).__name__})"
            )

        return core_schema.no_info_plain_validator_function(
            validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda instance: instance._to_serialized(),
                return_schema=core_schema.dict_schema(),
            ),
        )


# --- Connections ----------------------------------------------------


@_api.dataclass(DevErrorCode.DEV_1, kw_only=True)
class DataGenSrcConn(Conn):
    """Connection for :class:`DataGenSrc` -- carries no configuration
    (the source generates its data in-line)."""


@_api.dataclass(DevErrorCode.DEV_3, kw_only=True)
class TrashDestConn(Conn):
    """Connection for :class:`TrashDest` -- carries no configuration
    (the destination discards everything)."""


# --- Source ---------------------------------------------------------


@_api.dataclass(DevErrorCode.DEV_2, kw_only=True)
class DataGenSrc(Src):
    """Source emitting in-line dataframes -- one parquet slot per
    :class:`Data`.

    :param data: The :class:`Data` entries to emit. The combined
        base64-parquet size of every entry may not exceed 10 MB.
    """

    data: list[Data]

    def validate(self) -> None:
        total = sum(len(d.parquet_b64()) for d in self.data)
        if total > MAX_DATA_BYTES:
            raise DevErrorCode.DEV_7.exception(size=total, limit=MAX_DATA_BYTES)


# --- Destination ----------------------------------------------------


@_api.dataclass(DevErrorCode.DEV_4, kw_only=True)
class TrashDest(Dest):
    """Destination that discards the frame -- a sink to nowhere with no
    configuration. The subscriber returns a single frame, which is
    dropped."""
