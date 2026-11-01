#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

from pydantic import ValidationError

from tabsdatak._info import (
    PublisherInfo,
    StreamPublisherInfo,
    SubscriberInfo,
    TransformerInfo,
)
from tabsdatak.common._archive.bundle import (
    BUNDLE_MANIFEST_FILENAME,
    BUNDLE_USER_CODE_DIRNAME,
)
from tabsdatak.error import ErrorCode, TabsdataException

ENCODING = "utf-8"
KIND = "kind"

FunctionInfo = PublisherInfo | StreamPublisherInfo | TransformerInfo | SubscriberInfo


class FunctionDefinitionError(Exception):
    def __init__(
        self,
        manifest: Path,
        detail: str,
    ) -> None:
        super().__init__(detail)
        self.manifest = manifest
        self.detail = detail


class InvalidDefinitionError(FunctionDefinitionError):
    pass


class UnexpectedKindError(FunctionDefinitionError):
    def __init__(
        self,
        manifest: Path,
        kind: str | None,
        allowed: list[str],
    ) -> None:
        super().__init__(
            manifest,
            f"Function kind {kind!r} in {manifest} is not one of {allowed}",
        )
        self.kind = kind
        self.allowed = allowed


@dataclass(frozen=True)
class DefinitionErrors:
    bundle: ErrorCode
    user_code: ErrorCode
    kind: ErrorCode


class FunctionBundle:
    folder: Path
    manifest: Path

    def __init__(self, folder: Path) -> None:
        self.folder = folder
        self.manifest = folder / BUNDLE_MANIFEST_FILENAME

    def definition[Definition: FunctionInfo](
        self,
        kinds: dict[str, type[Definition]],
        errors: DefinitionErrors,
    ) -> Definition:
        try:
            return self.load(kinds)
        except UnexpectedKindError as error:
            raise errors.kind.exception(
                cause=error,
                kind=error.kind,
                manifest=error.manifest,
                allowed=error.allowed,
            )
        except InvalidDefinitionError as error:
            raise errors.user_code.exception(
                cause=error,
                bundle_folder=self.folder,
                detail=error.detail,
            )
        except FunctionDefinitionError as error:
            raise errors.bundle.exception(
                cause=error,
                bundle_folder=self.folder,
                detail=error.detail,
            )

    def load[Definition: FunctionInfo](
        self,
        kinds: dict[str, type[Definition]],
    ) -> Definition:
        manifest = self.manifest
        if not manifest.is_file():
            raise FunctionDefinitionError(manifest, f"{manifest} does not exist")
        user_code = str((self.folder / BUNDLE_USER_CODE_DIRNAME).resolve())
        if user_code not in sys.path:
            sys.path.insert(0, user_code)
        try:
            document = json.loads(manifest.read_text(encoding=ENCODING))
        except (OSError, json.JSONDecodeError) as error:
            raise FunctionDefinitionError(manifest, str(error)) from error
        kind = document.get(KIND) if isinstance(document, dict) else None
        if not isinstance(kind, str) or kind not in kinds:
            raise UnexpectedKindError(
                manifest,
                kind,
                sorted(kinds),
            )
        try:
            return kinds[kind].from_dict(document)
        except ValidationError as error:
            raise InvalidDefinitionError(manifest, str(error)) from error
        except TabsdataException as error:
            if isinstance(error.cause, ValidationError):
                raise InvalidDefinitionError(
                    manifest, str(error.cause)
                ) from error.cause
            raise FunctionDefinitionError(manifest, str(error)) from error
        except Exception as error:
            raise FunctionDefinitionError(manifest, str(error)) from error
