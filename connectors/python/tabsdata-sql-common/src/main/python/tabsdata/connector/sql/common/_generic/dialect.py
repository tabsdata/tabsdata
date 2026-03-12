#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import tzinfo
from typing import Any

from sqlalchemy.sql.elements import quoted_name


class Dialect(ABC):
    @property
    @abstractmethod
    def open_quote(self) -> str: ...

    @property
    @abstractmethod
    def close_quote(self) -> str: ...

    @abstractmethod
    def default_case_fold(self, name: str) -> str: ...

    @abstractmethod
    def get_catalog_columns(
        self,
        connection: Any,
        schema: str,
        table: str,
    ) -> list[str]: ...

    @abstractmethod
    def server_timezone(self, engine: Any) -> tzinfo | None: ...

    def parse_qualified_table(self, spec: str) -> tuple[quoted_name, quoted_name]:
        raw = spec
        spec = spec.strip()
        oq = self.open_quote
        cq = self.close_quote

        def token(s: str, start: int) -> tuple[quoted_name, int]:
            if start >= len(s):
                raise ValueError(
                    f"Invalid qualified table spec: {raw!r}; unexpected end of string"
                )
            if s[start] == oq:
                pos = start + 1
                while True:
                    end = s.find(cq, pos)
                    if end == -1:
                        raise ValueError(
                            f"Invalid qualified table spec: {raw!r}; "
                            f"unclosed {oq} at position {start}"
                        )
                    if end + 1 < len(s) and s[end + 1] == cq:
                        pos = end + 2
                        continue
                    break
                escaped = s[start + 1 : end]
                name = escaped.replace(cq + cq, cq)
                if not name:
                    raise ValueError(
                        f"Invalid qualified table spec: {raw!r}; "
                        f"empty quoted identifier at position {start}"
                    )
                return quoted_name(name, quote=True), end + 1
            else:
                end = s.find(".", start)
                if end == -1:
                    end = len(s)
                name = s[start:end].strip()
                if not name:
                    raise ValueError(
                        f"Invalid qualified table spec: {raw!r}; "
                        f"empty identifier at position {start}"
                    )
                if oq in name or cq in name:
                    raise ValueError(
                        f"Invalid qualified table spec: {raw!r}; "
                        f"unquoted identifier contains quote "
                        f"character at position {start}"
                    )
                return quoted_name(name, quote=False), end

        schema, idx = token(spec, 0)
        if idx >= len(spec) or spec[idx] != ".":
            raise ValueError(
                f"Invalid qualified table spec: {raw!r}, expected '.' after schema"
            )
        table, idx = token(spec, idx + 1)
        if idx != len(spec):
            raise ValueError(
                f"Invalid qualified table spec: {raw!r}, "
                f"unexpected content after table name"
            )
        return schema, table
