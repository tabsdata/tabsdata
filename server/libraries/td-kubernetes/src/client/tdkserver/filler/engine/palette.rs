//
// Copyright 2026 Tabsdata Inc.
//

use std::sync::OnceLock;

use ratatui::style::Color;

const ENV_COLORTERM: &str = "COLORTERM";
const TRUECOLOR_VALUES: [&str; 2] = ["truecolor", "24bit"];

pub struct Palette {
    pub base: Color,
    pub mantle: Color,
    pub surface: Color,
    pub overlay: Color,
    pub subtext: Color,
    pub text: Color,
    pub key: Color,
    pub section: Color,
    pub punctuation: Color,
    pub string: Color,
    pub scalar: Color,
    pub reference: Color,
    pub variable: Color,
    pub comment: Color,
    pub required: Color,
    pub error: Color,
    pub selection: Color,
}

pub fn palette() -> &'static Palette {
    static PALETTE: OnceLock<Palette> = OnceLock::new();
    PALETTE.get_or_init(|| {
        let truecolor = std::env::var(ENV_COLORTERM)
            .map(|value| TRUECOLOR_VALUES.contains(&value.as_str()))
            .unwrap_or(false);
        let color = |hex: u32| {
            let (red, green, blue) = ((hex >> 16) as u8, (hex >> 8) as u8, hex as u8);
            if truecolor {
                Color::Rgb(red, green, blue)
            } else {
                Color::Indexed(indexed(red, green, blue))
            }
        };
        Palette {
            base: color(0x1e1e2e),
            mantle: color(0x181825),
            surface: color(0x313244),
            overlay: color(0x585b70),
            subtext: color(0xbac2de),
            text: color(0xcdd6f4),
            key: color(0x89b4fa),
            section: color(0xcba6f7),
            punctuation: color(0x9399b2),
            string: color(0xa6e3a1),
            scalar: color(0xfab387),
            reference: color(0xf5c2e7),
            variable: color(0x94e2d5),
            comment: color(0x7f849c),
            required: color(0xf9e2af),
            error: color(0xf38ba8),
            selection: color(0x45475a),
        }
    })
}

pub fn indexed(red: u8, green: u8, blue: u8) -> u8 {
    let level = |value: u8| -> u8 {
        match value {
            0..48 => 0,
            48..115 => 1,
            _ => (value - 35) / 40,
        }
    };
    16 + 36 * level(red) + 6 * level(green) + level(blue)
}
