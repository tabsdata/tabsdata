//
// Copyright 2026 Tabs Data Inc.
//

use td_kubernetes::client::tdkserver::widgets::{phase_spinner, phase_spinner_in, spinner};

#[test]
fn spinner_exposes_its_multi_progress() {
    let bar = spinner("running");
    let multi = bar.multi();
    let _phase = phase_spinner_in(multi, "nested");
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
fn spinner_finish_with_message_marks_bar_as_finished() {
    let bar = spinner("running");
    bar.finish("Done");
}

#[test]
fn nested_phase_drop_does_not_panic_when_parent_is_alive() {
    let bar = spinner("outer");
    {
        let phase = phase_spinner_in(bar.multi(), "inner");
        let _resource = phase.add_resource("x");
    }
    let phase2 = phase_spinner_in(bar.multi(), "another");
    drop(phase2);
    bar.finish_and_clear();
}
