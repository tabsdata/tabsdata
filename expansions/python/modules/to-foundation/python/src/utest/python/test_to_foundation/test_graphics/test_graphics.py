#
# Copyright 2026 Tabsdata Inc.
#

import pytest
from PIL import Image

from tabsdata.expansions.foundation.graphics import ImageProtocol, render


@pytest.fixture
def logo(tmp_path):
    path = tmp_path / "logo.png"
    Image.new("RGBA", (2016, 512), (89, 179, 89, 255)).save(path)
    return path


@pytest.mark.pyo3
def test_render_iterm2_rows(logo):
    rows = render(str(logo), 40, ImageProtocol.Iterm2, (10, 20))
    assert len(rows) == 5
    assert rows[:4] == ["\x1b[40C"] * 4
    assert "]1337;File=inline=1;" in rows[4]


@pytest.mark.pyo3
def test_render_kitty_rows(logo):
    rows = render(str(logo), 40, ImageProtocol.Kitty, (10, 20))
    assert len(rows) == 5
    assert all(row.count("\U0010eeee") == 40 for row in rows)


@pytest.mark.pyo3
def test_render_defaults_the_cell_size(logo):
    assert len(render(str(logo), 40, ImageProtocol.Halfblocks)) == 5


@pytest.mark.pyo3
def test_render_rejects_a_missing_file(tmp_path):
    with pytest.raises(OSError):
        render(str(tmp_path / "missing.png"), 40, ImageProtocol.Kitty)


@pytest.mark.pyo3
def test_render_rejects_a_non_image(tmp_path):
    path = tmp_path / "text.png"
    path.write_text("not an image")
    with pytest.raises(ValueError):
        render(str(path), 40, ImageProtocol.Kitty)
