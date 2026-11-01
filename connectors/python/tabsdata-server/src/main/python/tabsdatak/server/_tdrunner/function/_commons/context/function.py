#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Self

from tabsdatak.api import FunctionCtx
from tabsdatak.server._tdrunner.function._commons.context.request import (
    FromRequestCtx,
)
from tabsdatak.server._tdrunner.function._commons.solver.secrets import (
    ProjectSecret,
)


class FunctionContext(FromRequestCtx, FunctionCtx):
    _project_secret: ProjectSecret = ProjectSecret({})

    def with_project_secret(self, project_secret: ProjectSecret) -> Self:
        self._project_secret = project_secret
        return self

    @property
    def secret_names(self) -> list[str]:
        return self._project_secret.names

    def secret_keys(self, name: str) -> list[str]:
        return self._project_secret.keys(name)

    def secret(
        self,
        name: str,
        key: str | None = None,
    ) -> str:
        return self._project_secret.value(name, key)
