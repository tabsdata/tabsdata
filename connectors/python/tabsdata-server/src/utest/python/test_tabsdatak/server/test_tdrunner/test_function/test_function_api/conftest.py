#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from test_tabsdatak.server.conftest import TESTING_RESOURCES_FOLDER

from .driver import Driver

RESOURCES = Path(TESTING_RESOURCES_FOLDER) / "function_api"


@pytest.fixture
def apiserver():
    posts: list[dict] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *arguments):
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            posts.append({"path": self.path, "body": body})
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://{host}:{port}", posts
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def driver(tmp_path, monkeypatch, apiserver, no_http_retry) -> Driver:
    url, posts = apiserver
    return Driver(tmp_path / "run", monkeypatch, url, posts)
