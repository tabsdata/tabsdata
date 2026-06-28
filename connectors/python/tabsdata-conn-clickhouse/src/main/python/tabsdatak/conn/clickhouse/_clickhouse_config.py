from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ClickHouseConfig:
    """Connection parameters for a ClickHouse server.

    Credentials are plain strings (the new-model connector resolves
    Secret into raw values before constructing this config). When
    ``username``/``password`` are empty strings, the ``default`` user
    with no password is used.
    """

    host: str
    port: int = 8123
    database: str = "default"
    username: str = "default"
    password: str = ""
    secure: bool = False

    def __repr__(self) -> str:
        username = "***redacted***" if self.username else ""
        password = "***redacted***" if self.password else ""
        return (
            f"ClickHouseConfig("
            f"host={self.host!r}, "
            f"port={self.port!r}, "
            f"database={self.database!r}, "
            f"username={username!r}, "
            f"password={password!r}, "
            f"secure={self.secure!r})"
        )
