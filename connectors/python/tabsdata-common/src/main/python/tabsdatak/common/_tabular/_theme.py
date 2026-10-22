#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

GREEN = "#6ab35c"
BLUE = "#39aaf1"
RED = "#ef8282"
YELLOW = "#ccb233"
GRAY_100 = "#0a0a0a"
GRAY_200 = "#1f1f1f"
GRAY_300 = "#3d3d3d"
GRAY_500 = "#8f8f8f"
GRAY_900 = "#d6d6d6"

BORDER = GREEN
HEADING = f"bold {GREEN}"
DETAIL = GRAY_500
TITLE = f"bold {BLUE}"
CONTEXT = BLUE
CELL = GRAY_900
MUTED = GRAY_500
PLACEHOLDER = f"italic {GRAY_500}"
SEPARATOR = GRAY_300
KEY = BLUE
BACKGROUND = GRAY_100
PANEL = GRAY_200

STATUS_STYLES = {
    "committed": GREEN,
    "complete": GREEN,
    "completed": GREEN,
    "done": GREEN,
    "finished": GREEN,
    "published": GREEN,
    "succeeded": GREEN,
    "success": GREEN,
    "running": BLUE,
    "run requested": BLUE,
    "on hold": YELLOW,
    "pending": YELLOW,
    "queued": YELLOW,
    "rescheduled": YELLOW,
    "scheduled": YELLOW,
    "stalled": YELLOW,
    "canceled": RED,
    "cancelled": RED,
    "error": RED,
    "failed": RED,
    "stopped": RED,
    "unexpected": RED,
    "yanked": RED,
}


def status_style(status: str) -> str:
    key = status.split("(", 1)[0].strip().lower().replace("_", " ").replace("-", " ")
    return STATUS_STYLES.get(key, CELL)
