#
#  Copyright 2026 Tabs Data Inc.
#

"""Textual widgets and color helpers for the harvester dashboard.

This module contains the reusable ``TableSection`` widget and color/emoji
mapping functions used by ``client.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rich.table import Table

from rich.console import Group as RichGroup
from rich.text import Text as RichText
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Sparkline, Static

from tabsdatax.pytest.plugins.harvester.internal.constants import (
    FIXTURE_SCOPE_ORDER,
    Color,
    Emoji,
)


# ---------------------------------------------------------------------------
# Custom DataTable that preserves Rich Text colors under cursor
# ---------------------------------------------------------------------------
class HarvesterDataTable(DataTable):
    """DataTable subclass that prevents cursor highlighting from
    overriding cell Rich Text foreground colors.

    Only the background changes on cursor; text colors stay as set.
    """

    DEFAULT_CSS = """
    HarvesterDataTable > .datatable--cursor {
        background: transparent;
    }

    HarvesterDataTable:focus > .datatable--cursor {
        background: #2e3a50 60%;
    }
    """

    def _render_cell(
        self, row_index, column_index, base_style, width, cursor=False, hover=False
    ):
        # Render normally
        lines = super()._render_cell(
            row_index,
            column_index,
            base_style,
            width,
            cursor=cursor,
            hover=hover,
        )
        if not cursor or row_index == -1:  # skip header
            return lines

        # Re-render WITHOUT cursor to get original colors,
        # then only take the background from the cursor render
        from rich.segment import Segment
        from rich.style import Style

        original_lines = super()._render_cell(
            row_index,
            column_index,
            base_style,
            width,
            cursor=False,
            hover=hover,
        )

        # Get cursor background from the component class
        if self.has_focus:
            bg_style = Style(bgcolor="#2e3a50")
        else:
            bg_style = Style()

        # Apply only background from cursor, keep original text colors
        result = []
        for orig_line, cursor_line in zip(original_lines, lines):
            new_line = []
            for seg in orig_line:
                if seg.style:
                    new_style = seg.style + bg_style
                else:
                    new_style = bg_style
                new_line.append(Segment(seg.text, new_style, seg.control))
            result.append(new_line)
        return result

    def _scroll_cursor_into_view(self, animate: bool = False) -> None:
        """Scroll cursor into view with tighter margins so the viewport
        follows the cursor more closely at boundaries."""
        from textual.geometry import Region, Spacing

        fixed_offset = self._get_fixed_offset()
        top, right, bottom, left = fixed_offset

        if self.cursor_type == "row":
            x, y, width, height = self._get_row_region(self.cursor_row)
            region = Region(int(self.scroll_x) + left, y, width - left, height)
        elif self.cursor_type == "column":
            x, y, width, height = self._get_column_region(self.cursor_column)
            region = Region(x, int(self.scroll_y) + top, width, height - top)
        else:
            region = self._get_cell_region(self.cursor_coordinate)

        # Use extra spacing so scroll triggers 1 row before the edge
        spacing = Spacing(top + 1, right, bottom + 1, left)
        self.scroll_to_region(region, animate=animate, spacing=spacing, force=True)


# ---------------------------------------------------------------------------
# TableSection widget
# ---------------------------------------------------------------------------
class TableSection(Vertical):
    """A titled DataTable with fixed height and its own scrollbar."""

    DEFAULT_CSS = """
    TableSection {
        height: auto;
        margin-bottom: 1;
    }

    TableSection > Static {
        height: 1;
        background: #2e5090;
        color: #e0e8f0;
        text-style: bold;
        padding: 0 1;
    }

    TableSection > DataTable {
        height: 14;
        background: #282838;
        scrollbar-size: 1 1;
    }

    TableSection > DataTable > .datatable--header {
        text-style: bold;
    }

    """

    def __init__(
        self, title: str, table_id: str, table_height: int = 14, **kwargs
    ) -> None:
        super().__init__()
        self.title_text = title
        self.table_id = table_id
        self.table_height = table_height

    def compose(self) -> ComposeResult:
        yield Static(self.title_text)
        dt = HarvesterDataTable(id=self.table_id, zebra_stripes=False)
        dt.cursor_type = "row"
        dt.styles.height = self.table_height
        yield dt


# ---------------------------------------------------------------------------
# Summary header widget
# ---------------------------------------------------------------------------
class SummaryHeader(Vertical):
    """Renders progress bars, averages, legend, and duration distribution."""

    DEFAULT_CSS = """
    SummaryHeader {
        height: auto;
        padding: 1 1;
        margin-bottom: 1;
        background: #282838;
    }

    SummaryHeader > Horizontal {
        height: auto;
        align-vertical: top;
    }

    SummaryHeader > Horizontal > #summary-text {
        width: auto;
        height: auto;
    }

    SummaryHeader > Horizontal > #per-minute-panel {
        height: 13;
        width: 120;
        margin: 0 0 0 2;
        overflow-y: auto;
        scrollbar-size: 1 1;
    }

    SummaryHeader > Horizontal > #per-minute-panel > #per-minute-text {
        width: auto;
        height: auto;
    }

    SummaryHeader > Horizontal > #sparkline-container {
        height: 100%;
        width: auto;
        margin: 0 0 0 2;
    }

    SummaryHeader .sparkline-col {
        width: 20;
        height: 100%;
        margin: 0 1 0 0;
    }

    SummaryHeader .sparkline-col > Sparkline {
        width: 100%;
        height: 1fr;
    }

    SummaryHeader .spark-label {
        width: 100%;
        height: 1;
        color: #808080;
        text-align: center;
    }

    SummaryHeader #sparkline-legend {
        width: auto;
        height: 1;
        color: #808080;
        dock: bottom;
    }
    """

    def __init__(self) -> None:
        super().__init__()

    def compose(self) -> ComposeResult:
        with Horizontal():
            yield Static("", id="summary-text")
            with Horizontal(id="sparkline-container"):
                with Vertical(classes="sparkline-col"):
                    yield Sparkline(
                        data=[0],
                        min_color="#5080c0",
                        max_color="#5080c0",
                        id="spark-all",
                    )
                    yield Static("All", classes="spark-label")
                with Vertical(classes="sparkline-col"):
                    yield Sparkline(
                        data=[0],
                        min_color="#50b850",
                        max_color="#50b850",
                        id="spark-passed",
                    )
                    yield Static("Passed", classes="spark-label")
                with Vertical(classes="sparkline-col", id="spark-failed-col"):
                    yield Sparkline(
                        data=[0],
                        min_color="#e05050",
                        max_color="#e05050",
                        id="spark-failed",
                    )
                    yield Static("Failed", classes="spark-label")
                yield Static("", id="sparkline-legend")
            with Vertical(id="per-minute-panel"):
                yield Static("", id="per-minute-text")

    def update_summary(
        self,
        collected: int,
        passed: int,
        failed: int,
        skipped: int,
        running: int,
        orphaned: int,
        markers: dict,
        fixtures: dict,
        pid: int = 0,
        pid_alive: bool = False,
        session_start_ts: float | None = None,
        test_averages: dict | None = None,
        running_tests: list | None = None,
        test_durations: list[float] | None = None,
        session_end_ts: float | None = None,
        tests_per_minute: list | None = None,
    ) -> None:
        """Rebuild the summary with current data."""
        import time as _time

        parts = []

        # --- Title and PID ---
        try:
            from importlib.metadata import version as pkg_version

            ver = pkg_version("tabsdata-toolkit")
        except Exception:
            ver = "?"
        title = RichText()
        title.append("Tabsdata Harvester Monitor", style=f"bold {Color.WHITE}")
        title.append(f"  v{ver}", style=Color.DIM)
        parts.append(title)
        parts.append(RichText(""))

        pid_line = RichText()
        pid_line.append("Main pytest process: ", style=Color.DIM)
        if pid:
            pid_color = Color.STATUS_DONE if pid_alive else Color.STATUS_ERROR
            pid_line.append(str(pid), style=f"bold {pid_color}")
        else:
            pid_line.append("—", style=f"bold {Color.WHITE}")
        parts.append(pid_line)

        # --- Start time and elapsed ---
        time_line = RichText()
        if session_start_ts:
            started = _time.strftime(
                "%Y-%m-%d %H:%M:%S", _time.localtime(session_start_ts)
            )
            if session_end_ts:
                elapsed = session_end_ts - session_start_ts
            else:
                elapsed = _time.time() - session_start_ts
            time_line.append("Started: ", style=Color.DIM)
            time_line.append(started, style=f"bold {Color.WHITE}")
            if session_end_ts:
                ended = _time.strftime(
                    "%Y-%m-%d %H:%M:%S", _time.localtime(session_end_ts)
                )
                time_line.append("  Ended: ", style=Color.DIM)
                time_line.append(ended, style=f"bold {Color.WHITE}")
            time_line.append("  Elapsed: ", style=Color.DIM)
            time_line.append(format_duration(elapsed), style=f"bold {Color.WHITE}")
        else:
            time_line.append("Waiting for session...", style=Color.DIM)
        parts.append(time_line)

        # --- Average test durations (from DB + running tests) ---
        avgs = test_averages or {}
        avg_passed_db = avgs.get("avg_passed")
        avg_failed_db = avgs.get("avg_failed")
        count_passed_db = avgs.get("count_passed", 0) or 0
        count_failed_db = avgs.get("count_failed", 0) or 0

        # Include running tests in the overall average
        running_duration = 0.0
        running_count = 0
        if running_tests:
            for rt in running_tests:
                running_duration += rt.elapsed
                running_count += 1

        total_db = (avg_passed_db or 0) * count_passed_db + (
            avg_failed_db or 0
        ) * count_failed_db
        count_all = count_passed_db + count_failed_db + running_count
        avg_all = (total_db + running_duration) / count_all if count_all > 0 else None

        avg_line = RichText()
        avg_line.append("Average Time: ", style=f"bold {Color.DIM}")
        if avg_all is not None:
            avg_line.append(f"🎯 All: {format_duration(avg_all)}", style=Color.WHITE)
        else:
            avg_line.append("🎯 All: ?", style=Color.DIM)
        avg_line.append("   ", style=Color.DIM)
        if avg_passed_db is not None:
            avg_line.append(
                f"✅ Passed: {format_duration(avg_passed_db)}", style=Color.STATUS_DONE
            )
        else:
            avg_line.append("✅ Passed: ?", style=Color.DIM)
        avg_line.append("   ", style=Color.DIM)
        if avg_failed_db is not None:
            avg_line.append(
                f"❌ Failed: {format_duration(avg_failed_db)}", style=Color.STATUS_ERROR
            )
        else:
            avg_line.append("❌ Failed: ?", style=Color.DIM)
        parts.append(avg_line)
        parts.append(RichText(""))

        # --- Tests by runs ---
        total_done = passed + failed + skipped + orphaned
        pending_count = max(0, collected - total_done - running) if collected else 0

        # --- Tests per marker (aggregated across markers) ---
        # A test with N markers contributes 1 to each marker's counters,
        # so the aggregated totals are larger than the test count.
        # Markers only update on completion, so pending includes running.
        m_total = sum(m.get("total", 0) for m in markers.values()) if markers else 0
        m_passed = sum(m.get("passed", 0) for m in markers.values()) if markers else 0
        m_failed = sum(m.get("failed", 0) for m in markers.values()) if markers else 0
        m_skipped = sum(m.get("skipped", 0) for m in markers.values()) if markers else 0
        m_pending = sum(m.get("pending", 0) for m in markers.values()) if markers else 0

        # --- Tests per fixture (aggregated) ---
        f_total = sum(f.get("total", 0) for f in fixtures.values()) if fixtures else 0
        f_passed = sum(f.get("passed", 0) for f in fixtures.values()) if fixtures else 0
        f_failed = sum(f.get("failed", 0) for f in fixtures.values()) if fixtures else 0
        f_skipped = (
            sum(f.get("skipped", 0) for f in fixtures.values()) if fixtures else 0
        )
        f_remaining = (
            sum(f.get("remaining", 0) for f in fixtures.values()) if fixtures else 0
        )
        if collected and f_total < collected:
            none_total = collected - f_total
            none_done = max(0, total_done - (f_passed + f_failed + f_skipped))
            f_total += none_total
            f_remaining += max(0, none_total - none_done)
            f_passed += max(0, none_done)

        progress_table = self._build_progress_table([
            (
                "Tests",
                collected,
                passed,
                failed,
                skipped,
                running,
                pending_count,
                Color.BAR_TESTS,
            ),
            (
                "Markers",
                m_total,
                m_passed,
                m_failed,
                m_skipped,
                0,
                m_pending,
                Color.BAR_MARKERS,
            ),
            (
                "Fixtures",
                f_total,
                f_passed,
                f_failed,
                f_skipped,
                0,
                f_remaining,
                Color.BAR_FIXTURES,
            ),
        ])
        parts.append(progress_table)

        # --- Legend ---
        parts.append(RichText(""))
        parts.append(self._legend())

        # Update text
        text_widget = self.query_one("#summary-text", Static)
        text_widget.update(RichGroup(*parts))

        # Update sparklines with duration distributions
        durations = test_durations or {"all": [], "passed": [], "failed": []}
        # Include running tests in "all"
        all_d = list(durations.get("all", []))
        if running_tests:
            all_d.extend(rt.elapsed for rt in running_tests)
        passed_d = durations.get("passed", [])
        failed_d = durations.get("failed", [])

        container = self.query_one("#sparkline-container")
        spark_all = self.query_one("#spark-all", Sparkline)
        spark_passed = self.query_one("#spark-passed", Sparkline)
        spark_failed = self.query_one("#spark-failed", Sparkline)
        legend = self.query_one("#sparkline-legend", Static)

        num_bins = 20
        has_data = len(all_d) > 1

        if has_data:
            spark_all.data = self._build_distribution(all_d, num_bins)
            spark_passed.data = (
                self._build_distribution(passed_d, num_bins)
                if len(passed_d) > 1
                else [0]
            )
            failed_col = self.query_one("#spark-failed-col")
            if failed_d:
                spark_failed.data = self._build_distribution(failed_d, num_bins)
                failed_col.display = True
            else:
                spark_failed.data = [0]
                failed_col.display = False
            container.display = True

            min_d = min(all_d)
            max_d = max(all_d)
            legend_text = RichText()
            legend_text.append(f"{format_duration(min_d)}", style=Color.DIM)
            legend_text.append(" ⟵  duration  ⟶  ", style=Color.DIM)
            legend_text.append(f"{format_duration(max_d)}", style=Color.DIM)
            legend.update(legend_text)
        else:
            spark_all.data = [0]
            spark_passed.data = [0]
            spark_failed.data = [0]
            container.display = False

        # Update per-minute panel
        per_minute_widget = self.query_one("#per-minute-text", Static)
        pm_table = self._build_per_minute_table(
            tests_per_minute or [],
            collected,
            session_start_ts,
        )
        per_minute_widget.update(pm_table)

    @staticmethod
    def _build_per_minute_table(
        tests_per_minute: list,
        collected: int,
        session_start_ts: float | None = None,
    ) -> "RichGroup":
        """Build a table showing test counts per elapsed minute.

        Rows at minutes that are multiples of 5 are rendered in bold.
        Remaining = collected - cumulative done so far.
        Always shows rows up to the current elapsed minute.
        """
        from rich.table import Table

        title = RichText()
        title.append("Tests per Minute", style=f"bold {Color.WHITE}")

        tbl = Table(
            show_header=True,
            show_edge=False,
            show_lines=False,
            box=None,
            padding=(0, 1),
            expand=False,
        )
        tbl.add_column(
            "Minute",
            justify="right",
            no_wrap=True,
            min_width=6,
            header_style=Color.WHITE,
        )
        tbl.add_column(
            "Total",
            justify="right",
            no_wrap=True,
            min_width=6,
            header_style=Color.TABLE_PHASES,
        )
        tbl.add_column(
            "",
            justify="right",
            no_wrap=True,
            min_width=6,
            header_style=Color.DIM,
        )
        tbl.add_column(
            "Passed",
            justify="right",
            no_wrap=True,
            min_width=6,
            header_style=Color.STATUS_DONE,
        )
        tbl.add_column(
            "",
            justify="right",
            no_wrap=True,
            min_width=6,
            header_style=Color.DIM,
        )
        tbl.add_column(
            "Failed",
            justify="right",
            no_wrap=True,
            min_width=6,
            header_style=Color.STATUS_ERROR,
        )
        tbl.add_column(
            "",
            justify="right",
            no_wrap=True,
            min_width=6,
            header_style=Color.DIM,
        )
        tbl.add_column(
            "Skipped",
            justify="right",
            no_wrap=True,
            min_width=6,
            header_style=Color.STATUS_PENDING,
        )
        tbl.add_column(
            "",
            justify="right",
            no_wrap=True,
            min_width=6,
            header_style=Color.DIM,
        )
        tbl.add_column(
            "Remaining",
            justify="right",
            no_wrap=True,
            min_width=6,
            header_style=Color.STATUS_RUNNING,
        )
        tbl.add_column(
            "Percentage",
            justify="right",
            no_wrap=True,
            min_width=10,
            header_style=Color.MAGENTA,
        )

        # Build a lookup and fill gaps so every minute from 1..current appears
        import time as _time

        by_minute = {int(r.get("minute", 0)): r for r in tests_per_minute}
        data_max = max(by_minute.keys()) if by_minute else 0
        if session_start_ts:
            elapsed_minutes = int((_time.time() - session_start_ts) / 60) + 1
        else:
            elapsed_minutes = data_max
        max_minute = max(data_max, elapsed_minutes, 1)
        empty_row = {"total": 0, "passed": 0, "failed": 0, "skipped": 0}
        all_minutes = []
        for m in range(1, max_minute + 1):
            all_minutes.append(by_minute.get(m, {"minute": m, **empty_row}))

        # Compute cumulative values in chronological order
        computed = []
        cum_total = 0
        cum_passed = 0
        cum_failed = 0
        cum_skipped = 0
        for row in all_minutes:
            total = int(row.get("total", 0))
            passed = int(row.get("passed", 0))
            failed = int(row.get("failed", 0))
            skipped = int(row.get("skipped", 0))
            cum_total += total
            cum_passed += passed
            cum_failed += failed
            cum_skipped += skipped
            remaining = max(0, collected - cum_total)
            rem_pct = (remaining / collected * 100) if collected > 0 else 0
            computed.append((
                row,
                cum_total,
                cum_passed,
                cum_failed,
                cum_skipped,
                remaining,
                rem_pct,
            ))

        # Display most recent first
        for row, c_total, c_passed, c_failed, c_skipped, remaining, rem_pct in reversed(
            computed
        ):
            minute = int(row.get("minute", 0))
            total = int(row.get("total", 0))
            passed = int(row.get("passed", 0))
            failed = int(row.get("failed", 0))
            skipped = int(row.get("skipped", 0))

            is_milestone = minute % 5 == 0

            if is_milestone:
                ms = "bold underline "
                m_txt = RichText(f"▸ {minute}", style=f"{ms}{Color.WHITE}")
            else:
                ms = ""
                m_txt = RichText(str(minute), style=Color.WHITE)

            tbl.add_row(
                m_txt,
                RichText(str(total), style=f"{ms}{Color.TABLE_PHASES}"),
                RichText(str(c_total), style=f"{ms}{Color.DIM}"),
                RichText(str(passed), style=f"{ms}{Color.STATUS_DONE}"),
                RichText(str(c_passed), style=f"{ms}{Color.DIM}"),
                RichText(str(failed), style=f"{ms}{Color.STATUS_ERROR}"),
                RichText(str(c_failed), style=f"{ms}{Color.DIM}"),
                RichText(str(skipped), style=f"{ms}{Color.STATUS_PENDING}"),
                RichText(str(c_skipped), style=f"{ms}{Color.DIM}"),
                RichText(str(remaining), style=f"{ms}{Color.STATUS_RUNNING}"),
                RichText(f"{rem_pct:.1f}%", style=f"{ms}{Color.MAGENTA}"),
            )

        return RichGroup(title, RichText(""), tbl)

    @staticmethod
    def _build_distribution(durations: list[float], num_bins: int = 40) -> list[float]:
        """Build a smoothed histogram from raw durations for the sparkline."""
        import math

        if not durations:
            return [0.0]

        # Use log-scale bins for better distribution visualization
        log_durations = [math.log10(max(d, 0.001)) for d in durations]
        lo = min(log_durations)
        hi = max(log_durations)
        if hi <= lo:
            hi = lo + 1

        bin_width = (hi - lo) / num_bins
        bins = [0.0] * num_bins
        for ld in log_durations:
            idx = min(int((ld - lo) / bin_width), num_bins - 1)
            bins[idx] += 1

        # Simple 3-point moving average for smoothing
        smoothed = [0.0] * num_bins
        for i in range(num_bins):
            total = bins[i]
            count = 1
            if i > 0:
                total += bins[i - 1]
                count += 1
            if i < num_bins - 1:
                total += bins[i + 1]
                count += 1
            smoothed[i] = total / count

        return smoothed

    @staticmethod
    def _legend() -> RichText:
        c = Emoji.CIRCLE
        line = RichText()
        line.append(" State: ", style=f"bold {Color.WHITE}")
        line.append(c, style=Emoji.CIRCLE_BLUE_STYLE)
        line.append("  pending   ", style=Color.DIM)
        line.append(c, style=Emoji.CIRCLE_ORANGE_STYLE)
        line.append("  active   ", style=Color.DIM)
        line.append(c, style=Emoji.CIRCLE_GREEN_STYLE)
        line.append("  done   ", style=Color.DIM)
        line.append(c, style=Emoji.CIRCLE_RED_STYLE)
        line.append("  failed   ", style=Color.DIM)
        line.append(c, style=Emoji.CIRCLE_PURPLE_STYLE)
        line.append("  unknown", style=Color.DIM)
        line.append("    ", style=Color.DIM)
        line.append("Scope: ", style=f"bold {Color.WHITE}")
        line.append(f"{Emoji.SESSION} session   ", style=Color.DIM)
        line.append(f"{Emoji.MODULE} module   ", style=Color.DIM)
        line.append(f"{Emoji.FUNCTION} function", style=Color.DIM)
        line.append("    ", style=Color.DIM)
        line.append("Fixture: ", style=f"bold {Color.WHITE}")
        line.append("Ⓐ", style=Emoji.CIRCLE_PURPLE_STYLE)
        line.append("  autouse", style=Color.DIM)
        return line

    def _build_progress_table(self, rows: list) -> Table:
        """Build an aligned table of progress bars.

        Each *row* is a tuple:
          ``(label, total, passed, failed, skipped, running, pending, bar_color)``
        """
        from rich.progress_bar import ProgressBar
        from rich.table import Table

        BAR_WIDTH = 40

        tbl = Table(
            show_header=True,
            show_edge=False,
            show_lines=False,
            box=None,
            padding=(0, 1),
            expand=False,
        )
        tbl.add_column("", style=f"bold {Color.WHITE}", no_wrap=True, min_width=10)
        tbl.add_column("", no_wrap=True, min_width=BAR_WIDTH + 2)
        tbl.add_column(
            "Percentage",
            justify="right",
            no_wrap=True,
            min_width=7,
            header_style=Color.WHITE,
        )
        tbl.add_column(
            "Progress",
            justify="right",
            no_wrap=True,
            min_width=12,
            header_style=Color.WHITE,
        )
        tbl.add_column(
            "Passed",
            justify="right",
            no_wrap=True,
            min_width=10,
            header_style=Color.STATUS_DONE,
        )
        tbl.add_column(
            "Failed",
            justify="right",
            no_wrap=True,
            min_width=10,
            header_style=Color.STATUS_ERROR,
        )
        tbl.add_column(
            "Skipped",
            justify="right",
            no_wrap=True,
            min_width=10,
            header_style=Color.STATUS_PENDING,
        )
        tbl.add_column(
            "Running",
            justify="right",
            no_wrap=True,
            min_width=10,
            header_style=Color.STATUS_RUNNING,
        )
        tbl.add_column(
            "Pending",
            justify="right",
            no_wrap=True,
            min_width=10,
            header_style=Color.DIM,
        )

        for label, total, passed, failed, skipped, running, pending, bar_color in rows:
            done = passed + failed + skipped
            pct = (done / total * 100) if total > 0 else 0

            bar = ProgressBar(
                total=max(total, 1),
                completed=done,
                width=BAR_WIDTH,
                complete_style=bar_color,
                finished_style=bar_color,
            )

            pct_text = RichText(f"{pct:6.2f}%", style=f"bold {Color.WHITE}")
            ratio_text = RichText()
            ratio_text.append(f"{done}", style=Color.WHITE)
            ratio_text.append(f"/{total}", style=Color.DIM)

            passed_t = RichText(str(passed), style=Color.STATUS_DONE)
            failed_t = RichText(str(failed), style=Color.STATUS_ERROR)
            skipped_t = RichText(str(skipped), style=Color.STATUS_PENDING)
            running_t = RichText(str(running), style=Color.STATUS_RUNNING)
            pending_t = RichText(str(pending), style=Color.DIM)

            tbl.add_row(
                label,
                bar,
                pct_text,
                ratio_text,
                passed_t,
                failed_t,
                skipped_t,
                running_t,
                pending_t,
            )

        return tbl


# ---------------------------------------------------------------------------
# State circle + text helpers
# ---------------------------------------------------------------------------
def _colored_circle(style: str) -> RichText:
    """Return a left-padded ⬤ with the given color style."""
    t = RichText()
    t.append(" ")
    t.append(Emoji.CIRCLE, style=style)
    return t


_STATE_STYLES = {
    "pending": Emoji.CIRCLE_BLUE_STYLE,
    "active": Emoji.CIRCLE_ORANGE_STYLE,
    "done": Emoji.CIRCLE_GREEN_STYLE,
    "setup": Emoji.CIRCLE_ORANGE_STYLE,
    "teardown": Emoji.CIRCLE_ORANGE_STYLE,
    "passed": Emoji.CIRCLE_GREEN_STYLE,
    "failed": Emoji.CIRCLE_RED_STYLE,
    "error": Emoji.CIRCLE_RED_STYLE,
    "skipped": Emoji.CIRCLE_BLUE_STYLE,
    "unknown": Emoji.CIRCLE_PURPLE_STYLE,
}


def state_circle(status: str) -> RichText:
    """Return a colored ⬤ for a state value, left-padded."""
    style = _STATE_STYLES.get(status, Emoji.CIRCLE_PURPLE_STYLE)
    return _colored_circle(style)


# ---------------------------------------------------------------------------
# Phase display helpers
# ---------------------------------------------------------------------------
_PHASE_DISPLAY_NAMES = {
    "configure": "Configure",
    "session_start": "Session Start",
    "collection": "Collection",
    "run": "Run",
    "reporting": "Reporting",
    "session_finish": "Session Finish",
    "unconfigure": "Unconfigure",
}


def phase_display_name(phase: str) -> str:
    """Return human-readable phase name."""
    return _PHASE_DISPLAY_NAMES.get(phase, phase.replace("_", " ").title())


def phase_status_circle(status: str) -> str:
    """Return the colored circle for a phase status."""
    return state_circle(status)


def phase_status_text(status: str) -> str:
    """Return the text label for a phase status."""
    return status


# ---------------------------------------------------------------------------
# Fixture display helpers
# ---------------------------------------------------------------------------
def fixture_scope_icon(scope: str) -> str:
    """Return the scope emoji."""
    if scope == "session":
        return Emoji.SESSION
    elif scope in ("package", "module"):
        return Emoji.MODULE
    else:
        return Emoji.FUNCTION


def fixture_scope_text(scope: str) -> str:
    """Return the scope label with first letter uppercase."""
    return scope.capitalize()


def fixture_lifecycle_circle(lifecycle: str) -> str:
    """Return the colored circle for a fixture lifecycle state."""
    return state_circle(lifecycle)


def fixture_lifecycle_text(lifecycle: str) -> str:
    """Return the text label for a fixture lifecycle state."""
    return lifecycle


def fixture_state_circle(
    total: int, passed: int, failed: int, skipped: int
) -> RichText:
    """Return a colored ⬤ for a fixture's execution state."""
    executed = passed + failed + skipped
    if executed == 0:
        return _colored_circle(Emoji.CIRCLE_BLUE_STYLE)
    if executed < total:
        if failed > 0:
            return _colored_circle(Emoji.CIRCLE_RED_STYLE)
        return _colored_circle(Emoji.CIRCLE_ORANGE_STYLE)
    if failed > 0:
        return _colored_circle(Emoji.CIRCLE_RED_STYLE)
    return _colored_circle(Emoji.CIRCLE_GREEN_STYLE)


def fixture_sort_key(name: str, scope: str, lifecycle: str) -> tuple:
    """Sort key: scope order, lifecycle fixtures first, then name."""
    scope_idx = FIXTURE_SCOPE_ORDER.get(scope, 99)
    has_lifecycle = 0 if scope in ("session", "package", "module") else 1
    return (scope_idx, has_lifecycle, name)


# ---------------------------------------------------------------------------
# Marker display helpers
# ---------------------------------------------------------------------------
def marker_state_circle(total: int, passed: int, failed: int, skipped: int) -> RichText:
    """Return a colored ⬤ for a marker's execution state."""
    executed = passed + failed + skipped
    if executed == 0:
        return _colored_circle(Emoji.CIRCLE_BLUE_STYLE)
    if executed < total:
        if failed > 0:
            return _colored_circle(Emoji.CIRCLE_RED_STYLE)
        return _colored_circle(Emoji.CIRCLE_ORANGE_STYLE)
    if failed > 0:
        return _colored_circle(Emoji.CIRCLE_RED_STYLE)
    return _colored_circle(Emoji.CIRCLE_GREEN_STYLE)


# ---------------------------------------------------------------------------
# Plugin display helpers
# ---------------------------------------------------------------------------
def plugin_type_display(plugin_type: str) -> str:
    """Return styled plugin type."""
    if plugin_type == "first-party":
        return f"[{Color.GREEN}]first-party[/]"
    elif plugin_type == "builtin":
        return f"[{Color.DIM}]builtin[/]"
    else:
        return f"[{Color.YELLOW}]third-party[/]"


def plugin_type_icon(plugin_type: str) -> str:
    """Return an emoji for plugin type."""
    if plugin_type == "first-party":
        return "🏠"  # home — our own
    elif plugin_type == "third-party":
        return "📦"  # package — external
    else:
        return "🧱"  # bricks — builtin


# Sort order: first-party first, then third-party, then builtin
_PLUGIN_TYPE_ORDER = {
    "first-party": 0,
    "third-party": 1,
    "builtin": 2,
}


def plugin_sort_key(name: str, plugin_type: str) -> tuple:
    """Sort plugins by type (first-party, third-party, builtin) then name."""
    return (_PLUGIN_TYPE_ORDER.get(plugin_type, 9), name)


def plugin_active_circle(active: bool) -> RichText:
    """Return a colored ⬤ for plugin activity."""
    style = Emoji.CIRCLE_ORANGE_STYLE if active else Emoji.CIRCLE_BLUE_STYLE
    return _colored_circle(style)


def plugin_active_text(active: bool) -> str:
    """Return text for plugin activity."""
    return "active" if active else "inactive"


# ---------------------------------------------------------------------------
# Cell text helper
# ---------------------------------------------------------------------------

# Table index → text color mapping
_TABLE_COLORS = [
    Color.TABLE_PHASES,  # 0 — Session Phases
    Color.TABLE_FIXTURES,  # 1 — Fixtures
    Color.TABLE_MARKERS,  # 2 — Markers
    Color.TABLE_PLUGINS,  # 3 — Plugins
    Color.TABLE_RUNNING,  # 4 — Running Tests
    Color.TABLE_FAILED,  # 5 — Failed Tests
]

# Table index → header color mapping (brighter version of text color)
_HEADER_COLORS = [
    Color.HDR_PHASES,  # 0
    Color.HDR_FIXTURES,  # 1
    Color.HDR_MARKERS,  # 2
    Color.HDR_PLUGINS,  # 3
    Color.HDR_RUNNING,  # 4
    Color.HDR_FAILED,  # 5
]


def text_for(table_index: int):
    """Return a helper that wraps values with the color for *table_index*."""
    color = (
        _TABLE_COLORS[table_index] if table_index < len(_TABLE_COLORS) else Color.WHITE
    )

    def _wrap(value) -> RichText:
        return RichText(str(value), style=color)

    return _wrap


# Registry: column key → (label, color) for refresh_headers
_header_registry: dict[str, tuple[str, str]] = {}


# Maps column key string → (label_text, color_hex) for refresh_headers
_hdr_meta: dict[str, tuple[str, str]] = {}


def header_for(table_index: int):
    """Return a helper that wraps column header labels as ``"Label▁▁▁…"``.

    Call ``refresh_headers()`` after data is loaded to extend the ``▁``
    fill to match actual column widths.
    """
    color = (
        _HEADER_COLORS[table_index]
        if table_index < len(_HEADER_COLORS)
        else Color.WHITE
    )

    def _wrap(label: str) -> RichText:
        t = RichText()
        t.append(label, style=f"bold {color}")
        return t

    _wrap._hdr_color = color  # type: ignore[attr-defined]
    return _wrap


def register_header(col_key: str, label: str, color: str) -> None:
    """Register a column header for refresh_headers."""
    _hdr_meta[col_key] = (label, color)


def refresh_headers(dt: "DataTable") -> None:
    """Update column headers with ``▁`` fill matched to actual column widths."""
    for col_key, col in dt.columns.items():
        key_str = col_key.value if hasattr(col_key, "value") else str(col_key)
        meta = _hdr_meta.get(key_str)
        if meta is None:
            continue
        label, color = meta
        col_width = col.content_width
        fill_len = max(0, col_width - len(label))
        t = RichText()
        t.append(label, style=f"bold {color}")
        if fill_len > 0:
            t.append(" ", style=color)
            t.append("▁" * (fill_len - 1), style=color)
        col.label = t


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------
def format_duration(seconds: float | None) -> str:
    """Format seconds as a human-readable string."""
    if seconds is None:
        return ""
    s = seconds
    if s < 60:
        return f"{s:.2f}s"
    m = int(s) // 60
    remainder = s - m * 60
    if m < 60:
        return f"{m}m {remainder:.2f}s"
    h = m // 60
    m = m % 60
    return f"{h}h {m}m {remainder:.2f}s"


def format_time(ts: float | None) -> str:
    """Format a timestamp as HH:MM:SS."""
    if ts is None:
        return ""
    import time

    return time.strftime("%H:%M:%S", time.localtime(ts))
