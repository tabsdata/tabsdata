#
#  Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
from dataclasses import dataclass
from functools import wraps
from typing import Callable, List, Literal, ParamSpec, TypeAlias, TypeVar, Union

P = ParamSpec("P")
T = TypeVar("T")

logger = logging.getLogger(__name__)


def unstable() -> Callable[[Callable[P, T]], Callable[P, T]]:
    def decorate(function: Callable[P, T]) -> Callable[P, T]:
        @wraps(function)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            logger.warning(
                f"Function '{function.__name__}' is under development, subject to "
                "change, and deemed unstable."
            )
            return function(*args, **kwargs)

        wrapper.__signature__ = inspect.signature(function)
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
