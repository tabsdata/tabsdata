//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use image::{Rgba, RgbaImage};

const PLACEHOLDER: char = '\u{10EEEE}';

fn logo() -> DynamicImage {
    DynamicImage::ImageRgba8(RgbaImage::from_pixel(2016, 512, Rgba([89, 179, 89, 255])))
}

fn visible(row: &str) -> String {
    let mut visible = String::new();
    let mut chars = row.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            visible.push(c);
        }
    }
    visible
}

#[test]
fn fitted_rows_follow_the_aspect_ratio() {
    let (image, size) = fitted(&logo(), 40, (10, 20));
    assert_eq!(size, Size::new(40, 5));
    assert_eq!((image.width(), image.height()), (400, 100));
}

#[test]
fn fitted_rows_are_at_least_one() {
    let wide = DynamicImage::ImageRgba8(RgbaImage::new(4000, 1));
    let (_, size) = fitted(&wide, 10, (10, 20));
    assert_eq!(size.height, 1);
}

#[test]
fn kitty_fills_every_cell_with_a_placeholder() {
    let rows = rows(&logo(), 40, ImageProtocol::Kitty, (10, 20)).unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows[0].contains("\x1b_G"));
    for row in &rows {
        assert_eq!(row.chars().filter(|c| *c == PLACEHOLDER).count(), 40);
        assert!(!row.contains('\n'));
        assert!(row.ends_with(RESET));
    }
}

#[test]
fn iterm2_sends_the_image_with_the_last_row() {
    let rows = rows(&logo(), 40, ImageProtocol::Iterm2, (10, 20)).unwrap();
    assert_eq!(rows.len(), 5);
    for row in &rows[..4] {
        assert_eq!(row, "\x1b[40C");
    }
    let last = &rows[4];
    assert!(last.starts_with("\x1b7\x1b[4A\x1b]1337;File=inline=1;"));
    assert!(last.contains(";width=40;height=5;preserveAspectRatio=0:"));
    assert!(last.ends_with("\x07\x1b8\x1b[40C"));
    assert!(!last.contains('\n'));
}

#[test]
fn iterm2_single_row_does_not_move_up() {
    let flat = DynamicImage::ImageRgba8(RgbaImage::new(400, 10));
    let rows = rows(&flat, 40, ImageProtocol::Iterm2, (10, 20)).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].starts_with("\x1b7\x1b]1337;"));
}

#[test]
fn halfblocks_draws_one_symbol_per_cell() {
    let rows = rows(&logo(), 40, ImageProtocol::Halfblocks, (10, 20)).unwrap();
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(visible(row).chars().count(), 40);
    }
}
