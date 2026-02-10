#
#  Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import inspect
import logging
import warnings
from functools import wraps
from typing import Any, Callable, TypeVar, cast

_T = TypeVar("_T")

logger = logging.getLogger(__name__)


class TabsdataDeprecatedWarning(DeprecationWarning):
    pass


def deprecated_category(
    reason: str,
    replacement: str | None = None,
    since: str | None = None,
) -> type[TabsdataDeprecatedWarning]:
    # noinspection DuplicatedCode
    def __str__(self) -> str:
        message = []
        msg = getattr(self, "_unstable_message", None)
        if msg:
            message.append(f"\n{str(msg)}")
        if reason:
            message.append(f"\n - Reason: {reason}")
        if since:
            message.append(f"\n - Since: {since}")
        if replacement:
            message.append(f"\n - Replacement: {replacement}")
        return " ".join(message)

    return cast(
        type[TabsdataDeprecatedWarning],
        type(
            "TabsdataDeprecatedWarning",
            (TabsdataDeprecatedWarning,),
            {
                "reason": reason,
                "replacement": replacement,
                "version": since,
                "__str__": __str__,
            },
        ),
    )


class TabsdataUnstableWarning(FutureWarning):
    pass


def unstable_category(
    reason: str,
    replacement: str | None = None,
    since: str | None = None,
) -> type[TabsdataUnstableWarning]:
    # noinspection DuplicatedCode
    def __str__(self) -> str:
        message = []
        msg = getattr(self, "_unstable_message", None)
        if msg:
            message.append(f"\n{str(msg)}")
        if reason:
            message.append(f"\n - Reason: {reason}")
        if since:
            message.append(f"\n - Since: {since}")
        if replacement:
            message.append(f"\n - Replacement: {replacement}")
        return " ".join(message)

    return cast(
        type[TabsdataUnstableWarning],
        type(
            "TabsdataUnstableWarning",
            (TabsdataUnstableWarning,),
            {
                "reason": reason,
                "replacement": replacement,
                "version": since,
                "__str__": __str__,
            },
        ),
    )


def _message(obj: Any, msg: str) -> str:
    name = getattr(obj, "__qualname__", getattr(obj, "__name__", "<unknown>"))
    return f"Call to unstable component '{name}': '{msg}'"


def _warn(
    log_message: str,
    msg: str,
    category: type[Warning],
) -> None:
    pass
    logger.warning(log_message)
    warnings.warn(
        msg,
        category=category,
        stacklevel=3,
    )


def _decorate(
    obj: _T,
    message: str,
    msg: str,
    category: type[Warning],
) -> _T:
    if inspect.isclass(obj):
        original_init = obj.__init__

        @wraps(original_init)
        def __init__(self: Any, *args: Any, **kwargs: Any) -> None:
            _warn(message, msg, category)
            original_init(self, *args, **kwargs)

        obj.__init__ = __init__
        return obj

    if inspect.iscoroutinefunction(obj):

        @wraps(obj)
        async def _inner_async(*args: Any, **kwargs: Any) -> Any:
            _warn(message, msg, category)
            return await obj(*args, **kwargs)

        # noinspection PyUnnecessaryCast
        return cast(_T, _inner_async)

    else:

        @wraps(obj)
        def _inner_sync(*args: Any, **kwargs: Any) -> Any:
            _warn(message, msg, category)
            return obj(*args, **kwargs)

        # noinspection PyUnnecessaryCast
        return cast(_T, _inner_sync)


def _unwrap_callable(obj: Any) -> Callable[..., Any] | None:
    if obj is None:
        return None
    if isinstance(obj, (staticmethod, classmethod)):
        return obj.__func__
    return cast(Callable[..., Any], obj)


def _msg(obj: Any) -> str | None:
    return getattr(obj, "__deprecated__", None)


def _category(obj: Any) -> type[Warning] | None:
    func = _unwrap_callable(obj)
    if func is None:
        return None
    closure = getattr(func, "__closure__", None)
    if not closure:
        return None
    for cell in closure:
        cell_value = cell.cell_contents
        if inspect.isclass(cell_value) and issubclass(cell_value, Warning):
            return cell_value
    return None


def deprecated(
    message: str,
    category: type[TabsdataDeprecatedWarning] = TabsdataDeprecatedWarning,
) -> Callable[[_T], _T]:
    def decorator(obj: _T) -> _T:
        setattr(obj, "_deprecated_message", message)
        setattr(obj, "_deprecated_category", category)
        setattr(category, "_deprecated_message", message)
        return obj

    return decorator


def unstable() -> Callable[[_T], _T]:
    def decorator(obj: _T) -> _T:
        if not (inspect.isclass(obj) or callable(obj)):
            raise TypeError(
                "@unstable decorator must be applied to a class or callable, not"
                f" '{obj!r}'"
            )

        msg = _msg(obj)
        if msg is None:
            raise AttributeError(
                "@unstable must be stacked with @deprecated and the decorated object "
                "must expose the '__deprecated__' message."
            )

        if inspect.isclass(obj):
            category = _category(getattr(obj, "__new__", None))
            if category is None:
                category = _category(getattr(obj, "__init_subclass__", None))
        else:
            category = _category(obj)

        if category is None:
            raise AttributeError(
                "@unstable could not determine the warning category that @deprecated "
                "was configured with."
            )
        setattr(category, "_unstable_message", msg)

        log_message = _message(obj, msg)
        return _decorate(obj, log_message, msg, category)

    return decorator


warnings.filterwarnings("ignore", category=TabsdataDeprecatedWarning)
warnings.filterwarnings("ignore", category=TabsdataUnstableWarning)
