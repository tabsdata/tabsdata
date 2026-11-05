//
// Copyright 2026 Tabsdata Inc.
//

//! What the passes did, logged as one summary a minute: how many ran and
//! overran their budget, what each step cost, which steps failed, which
//! parked items, what became of the workers' reports, and the runs dispatch
//! cancelled.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::LazyLock;
use std::time::Duration;
use td_common::summary::Summary;
use tracing::info;

const SUMMARY_EVERY: Duration = Duration::from_secs(60);

#[derive(Default)]
struct Counts {
    passes: u64,
    timed_out: u64,
    steps: BTreeMap<String, StepTime>,
    failed_steps: BTreeMap<String, u64>,
    parked: BTreeMap<String, u64>,
    reports_applied: u64,
    reports_stale: u64,
    cancelled_unmintable: u64,
}

#[derive(Default)]
struct StepTime {
    runs: u64,
    total: Duration,
    max: Duration,
}

impl fmt::Debug for StepTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} runs, {:?} total, {:?} max",
            self.runs, self.total, self.max
        )
    }
}

static SUMMARY: LazyLock<Summary<Counts>> = LazyLock::new(|| Summary::new(SUMMARY_EVERY));

pub fn pass_ran() {
    count(|c| c.passes += 1);
}

pub fn pass_timed_out() {
    count(|c| c.timed_out += 1);
}

pub fn step_took(step: &str, elapsed: Duration) {
    count(|c| {
        let time = c.steps.entry(step.to_owned()).or_default();
        time.runs += 1;
        time.total += elapsed;
        time.max = time.max.max(elapsed);
    });
}

pub fn step_failed(step: &str) {
    count(|c| *c.failed_steps.entry(step.to_owned()).or_default() += 1);
}

pub fn parked(step: &str, items: u64) {
    count(|c| *c.parked.entry(step.to_owned()).or_default() += items);
}

pub fn report_applied() {
    count(|c| c.reports_applied += 1);
}

pub fn report_stale() {
    count(|c| c.reports_stale += 1);
}

pub fn cancelled_unmintable(runs: u64) {
    count(|c| c.cancelled_unmintable += runs);
}

fn count(event: impl FnOnce(&mut Counts)) {
    let Some(c) = SUMMARY.record(event) else {
        return;
    };
    info!(
        passes = c.passes,
        timed_out = c.timed_out,
        steps = ?c.steps,
        failed_steps = ?c.failed_steps,
        parked = ?c.parked,
        reports_applied = c.reports_applied,
        reports_stale = c.reports_stale,
        cancelled_unmintable = c.cancelled_unmintable,
        "scheduler.pass: summary"
    );
}
