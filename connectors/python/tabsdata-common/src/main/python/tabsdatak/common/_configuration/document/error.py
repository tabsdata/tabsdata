#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.common._error import CommonException
from tabsdatak.error import ErrorCode, ErrorDef


class DocumentException(CommonException): ...


class DocumentErrorCode(ErrorCode):
    DOCUMENT_1 = ErrorDef(
        DocumentException,
        "Cannot read the document {path}: {detail}",
    )
    DOCUMENT_2 = ErrorDef(
        DocumentException,
        "The document {path} does not match its model: {detail}",
    )
    DOCUMENT_3 = ErrorDef(
        DocumentException,
        "The document {path} holds fields its model does not declare: {fields}",
    )
    DOCUMENT_4 = ErrorDef(
        DocumentException,
        "Cannot resolve the secrets of the document {path}: {detail}",
    )
