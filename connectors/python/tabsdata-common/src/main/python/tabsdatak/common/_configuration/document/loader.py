#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import TypeVar

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from tabsdatak.common._configuration.document.error import DocumentErrorCode
from tabsdatak.common._configuration.materializer.error import MaterializerException
from tabsdatak.common._configuration.materializer.secret import materialize

PATH_SEPARATOR = "."
FIELD_SEPARATOR = ", "


class DocumentModel(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True, hide_input_in_errors=True)


Model = TypeVar("Model", bound=DocumentModel)


def undeclared(node: object, path: tuple[str, ...] = ()) -> list[str]:
    if isinstance(node, BaseModel):
        found = [PATH_SEPARATOR.join((*path, name)) for name in node.model_extra or {}]
        for name in type(node).model_fields:
            found += undeclared(getattr(node, name), (*path, name))
        return found
    if isinstance(node, list | tuple):
        return [
            field
            for index, item in enumerate(node)
            for field in undeclared(item, (*path, str(index)))
        ]
    if isinstance(node, dict):
        return [
            field
            for key, value in node.items()
            for field in undeclared(value, (*path, str(key)))
        ]
    return []


def load(
    path: Path,
    model: type[Model],
    *,
    strict: bool = False,
    resolve_secrets: bool = True,
) -> Model:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as error:
        raise DocumentErrorCode.DOCUMENT_1.exception(path=path, detail=str(error))
    if resolve_secrets:
        try:
            document = materialize(document)
        except MaterializerException as error:
            raise DocumentErrorCode.DOCUMENT_4.exception(path=path, detail=str(error))
    try:
        loaded = model.model_validate(document)
    except ValidationError as error:
        raise DocumentErrorCode.DOCUMENT_2.exception(path=path, detail=str(error))
    if strict:
        fields = undeclared(loaded)
        if fields:
            raise DocumentErrorCode.DOCUMENT_3.exception(
                path=path,
                fields=FIELD_SEPARATOR.join(fields),
            )
    return loaded
