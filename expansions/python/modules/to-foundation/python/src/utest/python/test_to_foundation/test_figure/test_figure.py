#
# Copyright 2026 Tabsdata Inc.
#

import io

import pytest
from PIL import Image

from tabsdata.expansions.foundation.figure import Figure, figure, image_protocol
from tabsdata.expansions.foundation.graphics import ImageProtocol

TEXT = ["tabsdata"]


class Terminal(io.StringIO):
    def isatty(self):
        return True


@pytest.fixture
def logo(tmp_path):
    path = tmp_path / "logo.png"
    Image.new("RGBA", (2016, 512), (89, 179, 89, 255)).save(path)
    return path


@pytest.mark.pyo3
@pytest.mark.parametrize(
    "environ, expected",
    [
        ({"TERM": "xterm-kitty"}, ImageProtocol.Kitty),
        ({"TERM": "xterm-ghostty"}, ImageProtocol.Kitty),
        ({"TERM_PROGRAM": "ghostty"}, ImageProtocol.Kitty),
        ({"TERM_PROGRAM": "iTerm.app"}, ImageProtocol.Iterm2),
        ({"TERM_PROGRAM": "WezTerm"}, ImageProtocol.Iterm2),
        ({"LC_TERMINAL": "iTerm2"}, ImageProtocol.Iterm2),
        ({"TERM_PROGRAM": "vscode", "LC_TERMINAL": "iTerm2"}, None),
        ({"TERM_PROGRAM": "iTerm.app", "TMUX": "/tmp/tmux"}, None),
        ({"TERM_PROGRAM": "iTerm.app", "PYCHARM_HOSTED": "1"}, None),
        ({"TERM": "dumb", "TERM_PROGRAM": "iTerm.app"}, None),
        ({"TERM": "xterm-256color"}, None),
        ({}, None),
    ],
)
def test_image_protocol_from_environment(environ, expected):
    assert image_protocol(Terminal(), environ) == expected


@pytest.mark.pyo3
def test_image_protocol_needs_a_terminal():
    assert image_protocol(io.StringIO(), {"TERM": "xterm-kitty"}) is None


@pytest.mark.pyo3
@pytest.mark.parametrize(
    "forced, expected",
    [
        ("kitty", ImageProtocol.Kitty),
        ("ITERM2", ImageProtocol.Iterm2),
        ("halfblocks", ImageProtocol.Halfblocks),
        ("none", None),
    ],
)
def test_image_protocol_can_be_forced(forced, expected):
    environ = {"TD_IMAGE_PROTOCOL": forced, "TMUX": "/tmp/tmux"}
    assert image_protocol(io.StringIO(), environ) == expected


@pytest.mark.pyo3
def test_figure_draws_the_image_centred(logo):
    environ = {"TERM_PROGRAM": "iTerm.app"}
    drawn = figure(logo, TEXT, 60, 40, Terminal(), environ)
    assert drawn.text == TEXT
    assert drawn.pixels is not None
    assert all(row.startswith(" " * 10) for row in drawn.pixels)


@pytest.mark.pyo3
def test_figure_falls_back_to_text_without_pixels(logo):
    assert figure(logo, TEXT, 60, 40, Terminal(), {}) == Figure(TEXT)


@pytest.mark.pyo3
def test_figure_falls_back_to_text_without_an_image():
    environ = {"TERM_PROGRAM": "iTerm.app"}
    assert figure(None, TEXT, 60, 40, Terminal(), environ) == Figure(TEXT)


@pytest.mark.pyo3
def test_figure_falls_back_to_text_when_rendering_fails(tmp_path):
    environ = {"TERM_PROGRAM": "iTerm.app"}
    missing = tmp_path / "missing.png"
    assert figure(missing, TEXT, 60, 40, Terminal(), environ) == Figure(TEXT)
