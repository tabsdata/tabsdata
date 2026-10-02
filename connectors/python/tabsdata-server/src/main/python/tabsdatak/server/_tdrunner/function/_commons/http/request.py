#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from http import HTTPStatus

DEFAULT_TIMEOUT_SECONDS = 30.0


@dataclass(frozen=True)
class HttpRequest:
    url: str
    method: str = "POST"
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, kw_only=True)
class HttpResponse:
    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes

    @property
    def ok(self) -> bool:
        return HTTPStatus.OK <= self.status < HTTPStatus.MULTIPLE_CHOICES


# No HTTP response: DNS, TCP, connection, TLS or timeout failures.
class HttpRequestError(Exception):
    pass


def invoke(
    request: HttpRequest,
    *,
    query: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: float | None = DEFAULT_TIMEOUT_SECONDS,
) -> HttpResponse:
    url = apply_query(request.url, query)
    outgoing = urllib.request.Request(
        url,
        data=body,
        method=request.method,
        headers=dict(request.headers),
    )

    try:
        with urllib.request.urlopen(outgoing, timeout=timeout) as response:
            return HttpResponse(
                status=response.status,
                headers=lowercase_keys(response.headers.items()),
                body=response.read(),
            )
    except urllib.error.HTTPError as error:
        return HttpResponse(
            status=error.code,
            headers=lowercase_keys(error.headers.items())
            if error.headers is not None
            else {},
            body=error.read(),
        )
    except urllib.error.URLError as error:
        raise HttpRequestError(
            f"HTTP {request.method} {url} failed: {error.reason}"
        ) from error
    except TimeoutError as error:
        raise HttpRequestError(
            f"HTTP {request.method} {url} timed out after {timeout}s"
        ) from error


def lowercase_keys(items) -> dict[str, str]:
    return {key.lower(): value for key, value in items}


def apply_query(url: str, query: dict[str, str] | None) -> str:
    if not query:
        return url
    parsed = urllib.parse.urlparse(url)
    merged = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    merged.update({key: str(value) for key, value in query.items()})
    return parsed._replace(query=urllib.parse.urlencode(merged)).geturl()
