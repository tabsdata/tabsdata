//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

#[test]
fn spinner_returns_unfinished_progress_bar() {
    let bar = spinner("loading");
    assert!(!bar.is_finished());
    bar.finish_and_clear();
}

#[test]
fn spinner_carries_the_provided_message() {
    let bar = spinner("starting kminus cluster");
    assert_eq!(bar.message(), "starting kminus cluster");
    bar.finish_and_clear();
}

#[test]
fn spinner_accepts_empty_message() {
    let bar = spinner("");
    assert_eq!(bar.message(), "");
    bar.finish_and_clear();
}

#[test]
fn spinner_accepts_unicode_message() {
    let bar = spinner("café — starting");
    assert_eq!(bar.message(), "café — starting");
    bar.finish_and_clear();
}

#[test]
fn spinner_finish_and_clear_marks_finished() {
    let bar = spinner("any");
    bar.finish_and_clear();
    assert!(bar.is_finished());
}

#[test]
fn show_box_does_not_panic_on_short_text() {
    show_box("hello", 80);
}

#[test]
fn show_box_does_not_panic_on_multiline_text() {
    show_box("first line\nsecond line\nthird line", 80);
}

#[test]
fn show_box_does_not_panic_on_unicode_text() {
    show_box("café — instância — λ", 80);
}

#[test]
fn show_box_does_not_panic_on_long_single_line() {
    let text: String = "lorem ipsum dolor sit amet ".repeat(20);
    show_box(&text, 80);
}

#[test]
fn show_box_does_not_panic_on_zero_min_width() {
    show_box("hello", 0);
}

#[test]
fn show_banner_does_not_panic() {
    show_banner();
}
