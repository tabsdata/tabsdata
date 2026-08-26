//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn a_step_carries_its_label_to_completion() {
    let reporter = SilentReporter;
    let step = reporter.step("Stopping the service");
    step.done("Stopped the service");
}

#[test]
fn a_failed_step_carries_its_label() {
    let reporter = SilentReporter;
    let step = reporter.step("Stopping the service");
    step.fail();
}

#[test]
fn a_substep_carries_its_own_label() {
    let reporter = SilentReporter;
    let step = reporter.step("Upgrading the instance");
    let inner = step.substep("Rebuilding the environments");
    inner.done("Rebuilt the environments");
    step.done("Upgraded the instance");
}

#[test]
fn an_abandoned_step_is_reported_on_drop() {
    let reporter = SilentReporter;
    {
        let _step = reporter.step("Never completed");
    }
}
