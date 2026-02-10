#
# Copyright 2025 Tabs Data Inc.
#

import http.server
import os
import socket
import socketserver
import threading
import webbrowser
from pathlib import Path

MIN_PORT = 8000
MAX_PORT = 9000

BOOK_INDEX = "http://localhost:{port}/index.html"


def select_port() -> int:
    for port in range(MIN_PORT, MAX_PORT + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_candidate:
            try:
                socket_candidate.bind(("", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No free port found in range {MIN_PORT}-{MAX_PORT}")


def select_root() -> Path:
    # noinspection PyProtectedMember
    import tabsdata.extensions._examples.guides.book as book_module

    book_path = Path(book_module.__path__[0])
    return book_path


def start_server(folder: Path, port: int):

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, _format, *args):
            pass

    os.chdir(folder)
    handler = QuietHandler
    # noinspection PyTypeChecker
    with socketserver.TCPServer(("", port), handler) as httpd:
        print(f"Examples guide served at {BOOK_INDEX.format(port=port)}")
        try:
            httpd.serve_forever()
        except Exception as e:
            print(f"Server error: {e}")


def launch_browser(port: int):
    webbrowser.open(BOOK_INDEX.format(port=port))


def run():
    port = select_port()
    root = select_root()
    server_thread = threading.Thread(
        target=start_server,
        args=(root, port),
        daemon=True,
    )
    server_thread.start()
    launch_browser(port)
    server_thread.join()


if __name__ == "__main__":
    run()
