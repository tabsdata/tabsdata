#
# Copyright 2026 Tabsdata Inc.
#

"""Small HTTP invocation helper for in-pod calls to the apiserver.

Built for the manifest-trigger sidecar (NU7), which needs to POST
``fn_trigger`` as a JSON ``InternalExecutionRequest`` body and inspect
the response without treating 4xx as exceptional.

Why not :mod:`urllib` directly: the standard library's default
behaviour raises :class:`urllib.error.HTTPError` on 4xx/5xx, which
hides the status from callers that want to distinguish (for example)
``200 OK`` (triggered) from ``409 Conflict`` (apiserver dedup'd via
``external_uid`` -- a benign skip, not a failure). This wrapper folds
both branches into :class:`HttpResponse` so the caller's code-by-
status logic stays linear.

Why not :mod:`requests`: the runtime image keeps the dependency
footprint small; :mod:`urllib` is already on the classpath.

No retry: the sidecar's outer poll loop is the natural retry boundary
(if a trigger call fails, the manifest stays in ``manifests/`` and the
next iteration picks it up). If a specific call-site needs tenacity
retry, it wraps :func:`invoke` itself.
"""

from __future__ import annotations

import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field


@dataclass(frozen=True)
class HttpRequest:
    """A reusable HTTP request descriptor.

    Built once by the sidecar from its ``--trigger-url`` /
    ``--trigger-method`` / ``--trigger-authorization`` args (the
    pattern parallels how commands consume the responder triplet
    today) and replayed per-manifest with a different JSON body
    each time.
    """

    url: str
    method: str = "POST"
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class HttpResponse:
    """Result of a successful HTTP exchange.

    "Successful" means the server returned a status code -- 4xx and
    5xx land in :attr:`status_code` and are not raised. Callers decide
    how to interpret each code (e.g. NU7 treats 200 as triggered, the
    apiserver's dedup-rejection code as a benign skip, others as
    transient failure).
    """

    status_code: int
    body: bytes
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        """``True`` iff the status is in the 2xx range."""
        return 200 <= self.status_code < 300


class HttpRequestError(Exception):
    """An :func:`invoke` call could not complete (DNS / TCP / TLS /
    timeout) -- distinct from a successful HTTP exchange that returned
    a non-2xx status (see :class:`HttpResponse.status_code`)."""


def invoke(
    request: HttpRequest,
    *,
    query: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout_seconds: float | None = 30.0,
) -> HttpResponse:
    """Send ``request``; return the response.

    Args:
        request: The reusable request descriptor.
        query: Optional query-string overlay merged onto the URL's
            existing query. Caller-supplied keys win over duplicates
            already in the URL. Values are stringified. Retained as a
            general-purpose escape hatch even though the sidecar's
            ``fn_trigger`` call now carries its args in the JSON body.
        body: Optional request body bytes. NU7 passes a JSON-encoded
            ``InternalExecutionRequest``; ``None`` sends no body.
        timeout_seconds: Socket timeout. ``None`` disables (not
            recommended for the sidecar; the apiserver should respond
            in well under 30s).

    Raises:
        HttpRequestError: the underlying connection failed (DNS /
            TCP / TLS / timeout -- anything that prevents the server
            from returning a status code). Successful exchanges with
            4xx / 5xx responses are returned, not raised.
    """
    url = _apply_query(request.url, query)
    req = urllib.request.Request(
        url,
        data=body,
        method=request.method,
        headers=dict(request.headers),
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            return HttpResponse(
                status_code=resp.status,
                body=resp.read(),
                headers={k.lower(): v for k, v in resp.headers.items()},
            )
    except urllib.error.HTTPError as exc:
        # The server did respond with a status code -- surface it as a
        # HttpResponse, not as an error, so the caller can do
        # code-specific handling.
        return HttpResponse(
            status_code=exc.code,
            body=exc.read(),
            headers={k.lower(): v for k, v in exc.headers.items()}
            if exc.headers is not None
            else {},
        )
    except urllib.error.URLError as exc:
        raise HttpRequestError(
            f"HTTP {request.method} {url} failed: {exc.reason}"
        ) from exc
    except TimeoutError as exc:
        raise HttpRequestError(
            f"HTTP {request.method} {url} timed out after {timeout_seconds}s"
        ) from exc


def _apply_query(url: str, query: dict[str, str] | None) -> str:
    """Merge ``query`` onto ``url``'s existing query string.

    Caller-supplied keys take precedence over duplicates in the
    original URL. ``None`` or empty ``query`` returns ``url`` unchanged.
    """
    if not query:
        return url
    parsed = urllib.parse.urlparse(url)
    existing = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    existing.update({k: str(v) for k, v in query.items()})
    new_query = urllib.parse.urlencode(existing)
    return urllib.parse.urlunparse(parsed._replace(query=new_query))


__all__ = [
    "HttpRequest",
    "HttpRequestError",
    "HttpResponse",
    "invoke",
]
