#
# Copyright 2026 Tabsdata Inc.
#

"""System utility functions."""

import socket


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
    return port
