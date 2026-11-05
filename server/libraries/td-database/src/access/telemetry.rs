//
// Copyright 2026 Tabsdata Inc.
//

//! What a unit of work leaves in the logs: which code opened it, on which
//! pool, holding what, for how long, and how it ended. Each unit logs its
//! end at debug, and at warn when it waited on or held its project's lock
//! longer than [`SLOW`]; a summary a minute counts the units by where they
//! were opened and the pools by how full they ran, so an anomaly shows up
//! even when no request failed.

use crate::pool::{PoolKind, TdPool};
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::panic::Location;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use td_common::summary::Summary;
use tracing::{debug, info, warn};

/// A lock waited on, or held, for longer than this is logged where it
/// happens, with the unit that did it.
const SLOW: Duration = Duration::from_secs(1);
const SUMMARY_EVERY: Duration = Duration::from_secs(60);

/// Who opened a unit: the code that began it, or the service whose
/// parameter it is, known once the service ends it.
#[derive(Clone, Copy)]
pub(crate) enum Unit {
    At(&'static Location<'static>),
    Service(&'static str),
    Extracted,
}

impl Display for Unit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Unit::At(location) => write!(f, "{}:{}", location.file(), location.line()),
            Unit::Service(name) => write!(f, "service:{name}"),
            Unit::Extracted => f.write_str("service"),
        }
    }
}

/// What a unit holds while it runs.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnitLock {
    None,
    Project,
}

impl UnitLock {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            UnitLock::None => "none",
            UnitLock::Project => "project",
        }
    }
}

/// What a unit gave up waiting for before it began.
#[derive(Clone, Copy)]
pub(crate) enum GaveUpOn {
    ProjectLock,
    Connection,
}

/// How a unit ended, for its end event and the summary.
#[derive(Clone, Copy)]
pub(crate) enum Outcome {
    Committed,
    RolledBack,
    /// Dropped without being ended: rolled back as its connection closes.
    Dropped,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Committed => "committed",
            Outcome::RolledBack => "rolled_back",
            Outcome::Dropped => "dropped",
        }
    }
}

/// One unit's account, logged and counted when it is dropped: after its
/// transaction ended and its lock was released.
pub(crate) struct UnitLog {
    unit: Unit,
    pool: PoolKind,
    project: Option<String>,
    lock: UnitLock,
    lock_wait: Duration,
    began: Instant,
    outcome: Outcome,
}

impl UnitLog {
    pub(crate) fn began(
        unit: Unit,
        pool: PoolKind,
        project: Option<String>,
        lock: UnitLock,
        lock_wait: Duration,
    ) -> Self {
        UnitLog {
            unit,
            pool,
            project,
            lock,
            lock_wait,
            began: Instant::now(),
            outcome: Outcome::Dropped,
        }
    }

    pub(crate) fn of_service(&mut self, name: &'static str) {
        if matches!(self.unit, Unit::Extracted) {
            self.unit = Unit::Service(name);
        }
    }

    pub(crate) fn entered(&mut self, project: &str) {
        self.project = Some(project.to_owned());
    }

    pub(crate) fn ended(&mut self, outcome: Outcome) {
        self.outcome = outcome;
    }
}

impl Drop for UnitLog {
    fn drop(&mut self) {
        let held = self.began.elapsed();
        let unit = self.unit.to_string();
        let lock = self.lock.as_str();
        debug!(
            unit,
            pool = %self.pool,
            lock,
            project_id = self.project.as_deref(),
            held_ms = held.as_millis() as u64,
            outcome = self.outcome.as_str(),
            "db.tx: ended"
        );
        if self.lock == UnitLock::Project && held > SLOW {
            warn!(
                unit,
                pool = %self.pool,
                project_id = self.project.as_deref(),
                held_ms = held.as_millis() as u64,
                outcome = self.outcome.as_str(),
                "db.tx: held the project lock long"
            );
        }
        let (lock_wait, outcome) = (self.lock_wait, self.outcome);
        count(|s| {
            let c = s.units.entry(unit).or_default();
            c.runs += 1;
            match outcome {
                Outcome::Committed => c.committed += 1,
                Outcome::RolledBack => c.rolled_back += 1,
                Outcome::Dropped => c.dropped += 1,
            }
            c.max_lock_wait = c.max_lock_wait.max(lock_wait);
            c.max_held = c.max_held.max(held);
        });
    }
}

/// A unit that waited on its project's lock longer than [`SLOW`] before it
/// began.
pub(crate) fn waited(unit: Unit, project: &str, waited: Duration) {
    if waited > SLOW {
        warn!(
            unit = %unit,
            project_id = project,
            lock_wait_ms = waited.as_millis() as u64,
            "db.tx: waited long for the project lock"
        );
    }
}

/// A unit that gave up before it began: waiting for its project's lock, or
/// for a connection.
pub(crate) fn gave_up(unit: Unit, on: GaveUpOn) {
    count(|s| {
        let c = s.units.entry(unit.to_string()).or_default();
        match on {
            GaveUpOn::ProjectLock => c.lock_busy += 1,
            GaveUpOn::Connection => c.pool_busy += 1,
        }
    });
}

/// A connection taken from `pool`: how long it took and how full the pool
/// was, for the summary, and a warning when the wait was long.
pub(crate) fn acquired(unit: Unit, pool: &TdPool, kind: PoolKind, waited: Duration) {
    let size = pool.pool.size();
    let in_use = size.saturating_sub(pool.pool.num_idle() as u32);
    let max = pool.pool.options().get_max_connections();
    if waited > SLOW {
        warn!(
            unit = %unit,
            component = pool.component,
            pool = %kind,
            in_use,
            max,
            acquire_ms = waited.as_millis() as u64,
            "db.pool: waited long for a connection"
        );
    }
    count(|s| {
        let c = s
            .pools
            .entry(format!("{}:{kind}", pool.component))
            .or_default();
        c.acquires += 1;
        c.max = max;
        c.max_in_use = c.max_in_use.max(in_use);
        c.max_acquire = c.max_acquire.max(waited);
        if waited > SLOW {
            c.slow_acquires += 1;
        }
    });
}

/// A connection the pool could not give within its acquire timeout.
pub(crate) fn pool_timed_out(pool: &TdPool, kind: PoolKind) {
    count(|s| {
        s.pools
            .entry(format!("{}:{kind}", pool.component))
            .or_default()
            .timeouts += 1;
    });
}

#[derive(Default)]
struct UnitCounts {
    runs: u64,
    committed: u64,
    rolled_back: u64,
    dropped: u64,
    lock_busy: u64,
    pool_busy: u64,
    max_lock_wait: Duration,
    max_held: Duration,
}

#[derive(Default)]
struct PoolCounts {
    acquires: u64,
    slow_acquires: u64,
    timeouts: u64,
    max: u32,
    max_in_use: u32,
    max_acquire: Duration,
}

#[derive(Default)]
struct Counts {
    units: BTreeMap<String, UnitCounts>,
    pools: BTreeMap<String, PoolCounts>,
}

static SUMMARY: LazyLock<Summary<Counts>> = LazyLock::new(|| Summary::new(SUMMARY_EVERY));

/// Counts an event, and logs every unit's and pool's counts once a summary
/// is due.
fn count(event: impl FnOnce(&mut Counts)) {
    let Some(counts) = SUMMARY.record(event) else {
        return;
    };
    for (unit, c) in counts.units {
        info!(
            unit,
            runs = c.runs,
            committed = c.committed,
            rolled_back = c.rolled_back,
            dropped = c.dropped,
            lock_busy = c.lock_busy,
            pool_busy = c.pool_busy,
            max_lock_wait_ms = c.max_lock_wait.as_millis() as u64,
            max_held_ms = c.max_held.as_millis() as u64,
            "db.units: summary"
        );
    }
    for (pool, c) in counts.pools {
        info!(
            pool,
            acquires = c.acquires,
            slow_acquires = c.slow_acquires,
            timeouts = c.timeouts,
            max = c.max,
            max_in_use = c.max_in_use,
            max_acquire_ms = c.max_acquire.as_millis() as u64,
            "db.pools: summary"
        );
    }
}
