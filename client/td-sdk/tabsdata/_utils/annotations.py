#
#  Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import inspect
import logging
import warnings
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, List, Literal, TypeAlias, Union, cast

logger = logging.getLogger(__name__)


class TabsdataDeprecationWarning(DeprecationWarning):
    pass


def deprecation(
    reason: str,
    replacement: str | None = None,
    since: str | None = None,
) -> type[TabsdataDeprecationWarning]:
    # noinspection PyUnusedLocal
    def __str__(self) -> str:
        message = []
        if reason:
            message.append(f"Reason: {reason}.")
        if since:
            message.append(f"Since: {since}.")
        if replacement:
            message.append(f"Use {replacement} instead.")
        return " ".join(message)

    return cast(
        type[TabsdataDeprecationWarning],
        type(
            "TabsdataDeprecationWarning",
            (TabsdataDeprecationWarning,),
            {
                "reason": reason,
                "replacement": replacement,
                "version": since,
                "__str__": __str__,
            },
        ),
    )


class UnstableWarning(UserWarning):
    pass


def unstable(
    reason: str = "This feature is experimental and may change without notice.",
    category: type[Warning] = UnstableWarning,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorate(obj: Callable[..., Any]) -> Callable[..., Any]:
        message = f"Component {obj.__name__} is unstable: {reason}"

        if inspect.isclass(obj):
            original_init = obj.__init__

            @wraps(original_init)
            def wrapped_init(self, *args: Any, **kwargs: Any) -> None:
                warnings.warn(message, category, stacklevel=2)
                logger.warning(message)
                original_init(self, *args, **kwargs)

            obj.__init__ = wrapped_init
            return obj

        @wraps(obj)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            warnings.warn(message, category, stacklevel=2)
            logger.warning(message)
            return obj(*args, **kwargs)

        return wrapper

    return decorate


PydocCategories: TypeAlias = Literal[
    "aggregation",
    "attributes",
    "date",
    "description",
    "logic",
    "numeric",
    "filters",
    "generation",
    "join",
    "manipulation",
    "projection",
    "string",
    "tableframe",
    "type_casting",
    "union",
]


@dataclass
class PydocMetadata:
    categories: Union[str, List[str]]


def pydoc(categories: Union[PydocCategories, List[PydocCategories]]) -> Callable:
    def decorator(obj):
        metadata = PydocMetadata(categories=categories)
        if isinstance(obj, property):
            obj = property(obj.fget)
            obj.fget._pydoc_metadata = metadata
        else:
            obj._pydoc_metadata = metadata
        return obj

    return decorator
