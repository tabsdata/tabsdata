#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator

import pytest


class _OkHandler(BaseHTTPRequestHandler):
    def _ok(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self._ok()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        self._ok()

    def log_message(self, format, *args):
        pass


@pytest.fixture
def http_ok_url() -> Iterator[str]:
    """An always-200 local HTTP server. Yields its base URL.

    Used by error-path tests that would otherwise stall for ~32s on the
    framework's responder retry budget when the responder URL can't be
    reached. Pointing `--responder-url` here lets the real POST succeed
    immediately while exercising the genuine code path.
    """
    server = ThreadingHTTPServer(("127.0.0.1", 0), _OkHandler)
    host, port = server.server_address
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
