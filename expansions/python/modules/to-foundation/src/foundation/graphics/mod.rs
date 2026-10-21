//
// Copyright 2026 Tabsdata Inc.
//

use base64::prelude::{BASE64_STANDARD, Engine};
use image::imageops::FilterType;
use image::{DynamicImage, ImageFormat, ImageReader};
use pyo3::exceptions::PyValueError;
use pyo3::{PyResult, pyclass, pyfunction};
use pyo3_stub_gen::derive::{gen_stub_pyclass_enum, gen_stub_pyfunction};
use ratatui::buffer::{Buffer, CellDiffOption};
use ratatui::layout::{Rect, Size};
use ratatui::style::Color;
use ratatui::widgets::Widget;
use ratatui_image::errors::Errors;
use ratatui_image::picker::{Picker, ProtocolType};
use ratatui_image::{FontSize, Image, Resize};
use std::fmt::Write;
use std::io::Cursor;

const DEFAULT_CELL: (u16, u16) = (10, 20);

const SAVE_CURSOR: &str = "\x1b7";
const RESTORE_CURSOR: &str = "\x1b8";
const RESET: &str = "\x1b[0m";

#[gen_stub_pyclass_enum]
#[pyclass(
    eq,
    eq_int,
    mapping,
    from_py_object,
    module = "tabsdata.expansions.foundation.graphics"
)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ImageProtocol {
    Kitty,
    Iterm2,
    Halfblocks,
}

#[gen_stub_pyfunction(module = "tabsdata.expansions.foundation.graphics")]
#[pyfunction]
#[pyo3(signature = (path, columns, protocol, cell=None))]
pub fn render(
    path: &str,
    columns: u16,
    protocol: ImageProtocol,
    cell: Option<(u16, u16)>,
) -> PyResult<Vec<String>> {
    let image = ImageReader::open(path)?
        .with_guessed_format()?
        .decode()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    rows(&image, columns, protocol, cell.unwrap_or(DEFAULT_CELL))
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

pub fn rows(
    image: &DynamicImage,
    columns: u16,
    protocol: ImageProtocol,
    cell: (u16, u16),
) -> Result<Vec<String>, Errors> {
    let cell = (cell.0.max(1), cell.1.max(1));
    let (image, size) = fitted(image, columns.max(1), cell);
    match protocol {
        ImageProtocol::Iterm2 => iterm2(&image, size),
        ImageProtocol::Kitty => cells(image, size, cell, ProtocolType::Kitty),
        ImageProtocol::Halfblocks => cells(image, size, cell, ProtocolType::Halfblocks),
    }
}

fn fitted(image: &DynamicImage, columns: u16, (width, height): (u16, u16)) -> (DynamicImage, Size) {
    let pixels = u32::from(columns) * u32::from(width);
    let rows = (f64::from(pixels) * f64::from(image.height())
        / f64::from(image.width())
        / f64::from(height))
    .round()
    .clamp(1.0, f64::from(u16::MAX)) as u16;
    let resized = image.resize_exact(
        pixels,
        u32::from(rows) * u32::from(height),
        FilterType::Lanczos3,
    );
    (resized, Size::new(columns, rows))
}

fn cells(
    image: DynamicImage,
    size: Size,
    (width, height): (u16, u16),
    kind: ProtocolType,
) -> Result<Vec<String>, Errors> {
    #[allow(deprecated)]
    let mut picker = Picker::from_fontsize(FontSize::new(width, height));
    picker.set_protocol_type(kind);
    let protocol = picker.new_protocol(image, size, Resize::Fit(None))?;

    let area = Rect::new(0, 0, size.width, size.height);
    let mut buffer = Buffer::empty(area);
    Image::new(&protocol).render(area, &mut buffer);
    Ok((0..area.height).map(|y| line(&buffer, y)).collect())
}

fn line(buffer: &Buffer, y: u16) -> String {
    let mut line = String::new();
    let mut colours = None;
    for x in 0..buffer.area.width {
        let cell = &buffer[(x, y)];
        if matches!(cell.diff_option, CellDiffOption::Skip) {
            line.push_str("\x1b[C");
            continue;
        }
        if colours != Some((cell.fg, cell.bg)) {
            let _ = write!(line, "\x1b[{};{}m", sgr(cell.fg, 38), sgr(cell.bg, 48));
            colours = Some((cell.fg, cell.bg));
        }
        line.push_str(cell.symbol());
    }
    line.push_str(RESET);
    line
}

fn sgr(colour: Color, layer: u8) -> String {
    match colour {
        Color::Rgb(red, green, blue) => format!("{layer};2;{red};{green};{blue}"),
        Color::Indexed(index) => format!("{layer};5;{index}"),
        _ => format!("{}", layer + 1),
    }
}

fn iterm2(image: &DynamicImage, size: Size) -> Result<Vec<String>, Errors> {
    let mut png = Vec::new();
    image.write_to(&mut Cursor::new(&mut png), ImageFormat::Png)?;

    let Size { width, height } = size;
    let forward = format!("\x1b[{width}C");
    let up = if height > 1 {
        format!("\x1b[{}A", height - 1)
    } else {
        String::new()
    };
    let mut rows = vec![forward.clone(); usize::from(height)];
    rows[usize::from(height) - 1] = format!(
        "{SAVE_CURSOR}{up}\x1b]1337;File=inline=1;size={};width={width};height={height};preserveAspectRatio=0:{}\x07{RESTORE_CURSOR}{forward}",
        png.len(),
        BASE64_STANDARD.encode(&png),
    );
    Ok(rows)
}

#[cfg(test)]
mod tests;
