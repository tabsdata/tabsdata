//
// Copyright 2026 Tabsdata Inc.
//

use ratatui::layout::Rect;
use ratatui::style::Color;

use super::engine::clipboard::line_breaks;
use super::engine::editor::View;
use super::engine::highlight::{empty_values, first_empty_value, highlight, locate};
use super::engine::palette::{indexed, palette};

fn lines(text: &str) -> Vec<String> {
    text.lines().map(str::to_owned).collect()
}

fn foreground(text: &str, position: usize) -> Option<Color> {
    highlight(text)[position].fg
}

#[test]
fn a_key_its_colon_and_its_value_get_distinct_colors() {
    let colors = palette();
    let text = "  host: db.internal";
    assert_eq!(foreground(text, 2), Some(colors.key));
    assert_eq!(foreground(text, 6), Some(colors.punctuation));
    assert_eq!(foreground(text, 8), Some(colors.string));
}

#[test]
fn a_key_without_a_value_is_a_section() {
    assert_eq!(foreground("files:", 0), Some(palette().section));
}

#[test]
fn an_environment_reference_colors_its_prefix_and_its_variable() {
    let colors = palette();
    let text = "password: env:POSTGRES_PASSWORD";
    assert_eq!(foreground(text, 10), Some(colors.reference));
    assert_eq!(foreground(text, 14), Some(colors.variable));
}

#[test]
fn a_braced_reference_colors_its_variable_apart_from_the_text_around_it() {
    let colors = palette();
    let text = "uri: env:${BUCKET}/root";
    assert_eq!(foreground(text, 5), Some(colors.reference));
    assert_eq!(foreground(text, 9), Some(colors.variable));
    assert_eq!(foreground(text, 17), Some(colors.variable));
    assert_eq!(foreground(text, 18), Some(colors.string));
}

#[test]
fn an_empty_required_value_is_marked() {
    let style = highlight("host: \"\"")[6];
    assert_eq!(style.bg, Some(palette().required));
}

#[test]
fn numbers_booleans_and_comments_have_their_own_colors() {
    let colors = palette();
    assert_eq!(foreground("port: 5432", 6), Some(colors.scalar));
    assert_eq!(foreground("enabled: true", 9), Some(colors.scalar));
    assert_eq!(foreground("  # A note: here", 2), Some(colors.comment));
}

#[test]
fn a_selector_key_with_a_colon_inside_brackets_stays_a_key() {
    let text = "    storage.mounts[id=ROOT]:";
    assert_eq!(foreground(text, 4), Some(palette().section));
    assert_eq!(highlight(text).len(), text.chars().count());
}

#[test]
fn empty_values_are_counted_and_found() {
    let content = lines("values:\n  host: \"\"\n  port: ''\n  name: tabsdata\n");
    assert_eq!(empty_values(&content), 2);
    assert_eq!(first_empty_value(&content), Some(1));
}

#[test]
fn an_error_is_located_by_the_key_it_names() {
    let content = lines("values:\n  tdk_database_host: \"\"\n");
    let message = "Setting 'tdk_database_host' in 'stub.yaml' gives placeholder \
                   'literal:tdk_database_host' an empty value, but it is mandatory.";
    assert_eq!(locate(message, &content), Some(1));
}

#[test]
fn an_error_is_located_by_the_last_segment_of_a_key_path() {
    let content = lines("files:\n  tabsdata:\n    pods.service.resources.cpu: x\n");
    let message = "Setting 'tabsdata:pods.service.resources.cpu' is not valid.";
    assert_eq!(locate(message, &content), Some(2));
}

#[test]
fn an_error_naming_a_line_is_located_at_that_line() {
    let content = lines("a: 1\nb: [\nc: 3\n");
    assert_eq!(
        locate("The file did not parse at line 2 column 4.", &content),
        Some(1)
    );
    assert_eq!(
        locate("The file did not parse at line 9 column 1.", &content),
        None
    );
}

#[test]
fn colors_map_to_the_nearest_indexed_color_without_truecolor() {
    assert_eq!(indexed(0, 0, 0), 16);
    assert_eq!(indexed(255, 255, 255), 231);
    assert_eq!(indexed(0x89, 0xb4, 0xfa), 16 + 36 * 2 + 6 * 3 + 5);
}

#[test]
fn a_click_maps_to_the_text_position_under_it() {
    let view = View {
        top: 2,
        text: Rect::new(1, 2, 40, 10),
        gutter: 4,
    };
    let text = lines("a: 1\nb: 2\nkey: value\nlast");
    assert_eq!(view.position(&text, 7, 2), (2, 2));
    assert_eq!(view.position(&text, 2, 3), (3, 0));
    assert_eq!(view.position(&text, 39, 2), (2, 10));
    assert_eq!(view.position(&text, 7, 11), (3, 2));
}

#[test]
fn pasted_line_breaks_of_any_platform_become_new_lines() {
    assert_eq!(line_breaks("a\rb\r\nc\nd"), "a\nb\nc\nd");
}
