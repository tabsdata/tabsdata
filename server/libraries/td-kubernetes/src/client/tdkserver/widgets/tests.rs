//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

#[test]
fn spinner_returns_unfinished_progress_bar() {
    let bar = spinner("loading");
    assert!(!bar.bar.is_finished());
    bar.finish_and_clear();
}

#[test]
fn spinner_carries_the_provided_message() {
    let bar = spinner("starting kminus cluster");
    assert_eq!(bar.bar.message(), "starting kminus cluster");
    bar.finish_and_clear();
}

#[test]
fn spinner_accepts_empty_message() {
    let bar = spinner("");
    assert_eq!(bar.bar.message(), "");
    bar.finish_and_clear();
}

#[test]
fn spinner_accepts_unicode_message() {
    let bar = spinner("café — starting");
    assert_eq!(bar.bar.message(), "café — starting");
    bar.finish_and_clear();
}

#[test]
fn spinner_finish_and_clear_marks_finished() {
    let bar = spinner("any");
    let inner = bar.bar.clone();
    bar.finish_and_clear();
    assert!(inner.is_finished());
}

#[test]
fn spinner_finish_marks_bar_as_finished() {
    let bar = spinner("any");
    let inner = bar.bar.clone();
    bar.finish("Done with any");
    assert!(inner.is_finished());
}

#[test]
fn show_box_does_not_panic_on_short_text() {
    let outcome = std::panic::catch_unwind(|| show_box("hello", 80));
    assert!(outcome.is_ok(), "show_box panicked on short text");
}

#[test]
fn show_box_does_not_panic_on_multiline_text() {
    let outcome = std::panic::catch_unwind(|| show_box("first line\nsecond line\nthird line", 80));
    assert!(outcome.is_ok(), "show_box panicked on multiline text");
}

#[test]
fn show_box_does_not_panic_on_unicode_text() {
    let outcome = std::panic::catch_unwind(|| show_box("café — instância — λ", 80));
    assert!(outcome.is_ok(), "show_box panicked on unicode text");
}

#[test]
fn show_box_does_not_panic_on_long_single_line() {
    let text: String = "lorem ipsum dolor sit amet ".repeat(20);
    let outcome = std::panic::catch_unwind(|| show_box(&text, 80));
    assert!(outcome.is_ok(), "show_box panicked on long single line");
}

#[test]
fn show_box_does_not_panic_on_zero_min_width() {
    let outcome = std::panic::catch_unwind(|| show_box("hello", 0));
    assert!(outcome.is_ok(), "show_box panicked on zero min width");
}

#[test]
fn show_banner_does_not_panic() {
    let outcome = std::panic::catch_unwind(show_banner);
    assert!(outcome.is_ok(), "show_banner panicked");
}

#[test]
fn phase_spinner_creates_unfinished_header_with_message() {
    let phase = phase_spinner("Applying init jobs");
    assert_eq!(phase.header.message(), "Applying init jobs");
    assert!(!phase.header.is_finished());
    drop(phase);
}

#[test]
fn phase_spinner_add_resource_returns_unfinished_bar_with_message() {
    let phase = phase_spinner("phase");
    let bar = phase.add_resource("init-bootstrap");
    assert_eq!(bar.message(), "init-bootstrap");
    assert!(!bar.is_finished());
    drop(phase);
}

#[test]
fn phase_spinner_drop_finishes_header() {
    let phase = phase_spinner("phase");
    let header = phase.header.clone();
    drop(phase);
    assert!(header.is_finished());
}

#[test]
fn spinner_exposes_its_multi_progress() {
    let bar = spinner("running");
    let multi = bar.multi();
    let phase = phase_spinner_in(multi, "nested");
    assert_eq!(phase.header.message(), "nested");
    assert!(!phase.header.is_finished());
    drop(phase);
    bar.finish_and_clear();
}

#[test]
fn phase_spinner_in_shares_parent_canvas() {
    let bar = spinner("outer");
    let phase = phase_spinner_in(bar.multi(), "inner phase");
    let resource = phase.add_resource("resource-one");
    assert!(!resource.is_finished(), "resource bar starts unfinished");
    resource.finish_and_clear();
    drop(phase);
    bar.finish_and_clear();
}

#[test]
fn standalone_phase_spinner_still_works() {
    let phase = phase_spinner("standalone");
    let resource = phase.add_resource("standalone-resource");
    assert!(!resource.is_finished());
    resource.finish_and_clear();
    drop(phase);
}

#[test]
fn nested_phase_drop_does_not_panic_when_parent_is_alive() {
    let bar = spinner("outer");
    let outer_inner = bar.bar.clone();
    {
        let phase = phase_spinner_in(bar.multi(), "inner");
        let _resource = phase.add_resource("x");
    }
    assert!(
        !outer_inner.is_finished(),
        "parent bar must outlive the first nested phase"
    );
    let phase2 = phase_spinner_in(bar.multi(), "another");
    assert_eq!(phase2.header.message(), "another");
    drop(phase2);
    assert!(
        !outer_inner.is_finished(),
        "parent bar must outlive the second nested phase"
    );
    bar.finish_and_clear();
    assert!(outer_inner.is_finished(), "parent finish must complete");
}
