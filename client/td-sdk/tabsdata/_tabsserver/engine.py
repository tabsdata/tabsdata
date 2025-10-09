#
# Copyright 2025 Tabs Data Inc.
#

from dataclasses import dataclass
from threading import Lock
from typing import ClassVar, Optional


@dataclass(frozen=True)
class Engine:
    on_server: bool


class EngineProvider:
    _instance: ClassVar[Optional[Engine]] = None
    _lock: ClassVar[Lock] = Lock()

    @classmethod
    def instance(cls, on_server: bool | None = None) -> Engine:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = Engine(
                        on_server=on_server if on_server is not None else False
                    )
        else:
            if on_server is not None and cls._instance.on_server != on_server:
                raise ValueError(
                    "Engine instance already exists with "
                    f"on_server={cls._instance.on_server}"
                )
        return cls._instance


def instance(on_server: bool | None = None) -> Engine:
    return EngineProvider.instance(on_server=on_server)
