//
// Copyright 2026 Tabsdata Inc.
//

//! What a write unit locks before it begins, and what holding it lets the
//! unit run. A lock waited on inside a `SERIALIZABLE` transaction still ends
//! in an abort once its holder commits; waited on before `BEGIN`, the
//! snapshot the unit's first statement fixes follows that commit instead.
//!
//! One lock: a project's, held exclusively ([`ProjectLock`]) by the units
//! that decide over the project's rows. Every other write holds nothing and
//! reads nothing another unit decides on, so no two units wait on each other
//! in a cycle: a value two such units can both claim, such as a name, is
//! the unique index's to refuse, not a read's before the write. What a unit holds is part of its connection's type, `RW<L>`,
//! and work that needs the lock asks for it with [`Holds`].
//!
//! The lock kind is also the unit's isolation. A unit holding nothing is
//! `SERIALIZABLE`: the database proves it against every other write. A unit
//! holding its project is `REPEATABLE READ`: it is the only writer of the
//! project's run state while it holds, so the lock proves what `SERIALIZABLE`
//! would, and predicate locks would only make it abort on pages it shares
//! with other projects' rows. That makes the lock part of the unit's
//! correctness, not of its speed: a locked unit must hold its project
//! exclusively wherever the server runs. Global rows a locked unit writes
//! (a project's own row and roles, as it is created, renamed or deleted) are
//! written at `REPEATABLE READ` too, so what they decide on must be held by
//! constraints.
//!
//! Who keeps the locks is a [`Locker`]: [`InProcessLocker`] for one server
//! process.

use crate::access::sealed::Sealed;
use crate::access::telemetry::Unit;
use crate::access::{Attempt, DbPool, GatedConnection, InProject, RW, WriteMode};
use crate::domain::ScopedBy;
use crate::pool::{ServingPool, TdPool};
use futures_util::future::BoxFuture;
use std::panic::Location;
use std::time::Duration;
use td_error::TdError;

pub use crate::access::in_process::InProcessLocker;

/// What a write unit holds: nothing ([`Unlocked`]) or its project
/// exclusively ([`ProjectLock`]), and the isolation that holding it needs.
pub trait LockKind: Sealed + Send + Sync + 'static {
    const ISOLATION: Isolation;
    const LOCKS: bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// Commits only what some serial order of the concurrent units would
    /// have; one that cannot fails with a conflict.
    Serializable,
    /// One snapshot for every statement; a row another unit changed since
    /// fails the update with a conflict. What the unit decides from is
    /// kept still by its lock, not by the database.
    RepeatableRead,
}

pub struct Unlocked;
pub struct ProjectLock;

impl Sealed for Unlocked {}

impl LockKind for Unlocked {
    const ISOLATION: Isolation = Isolation::Serializable;
    const LOCKS: bool = false;
}

impl Sealed for ProjectLock {}

impl LockKind for ProjectLock {
    const ISOLATION: Isolation = Isolation::RepeatableRead;
    const LOCKS: bool = true;
}

/// Work that must run under `K`'s lock asks its connection for it.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not hold the lock `{K}` needs",
    note = "open the unit with `lock_project()` before `begin`"
)]
pub trait Holds<K: LockKind>: WriteMode {}

/// The locks a unit holds until it ends; dropping it releases them.
#[derive(Default)]
pub struct Held(Option<Box<dyn Send + Sync>>);

impl Held {
    pub fn new(guard: impl Send + Sync + 'static) -> Self {
        Self(Some(Box::new(guard)))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

/// A project's lock in one component. Built only from the pool a unit opens
/// on and the project its scope is bound to, so a unit locks the project it
/// is scoped to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectLockKey(String);

impl ProjectLockKey {
    pub(crate) fn of(pool: &TdPool, project: &str) -> Self {
        Self(format!("{}:{project}", pool.component))
    }

    #[cfg(test)]
    pub(crate) fn in_component(component: &str, project: &str) -> Self {
        Self(format!("{component}:{project}"))
    }
}

/// Keeps the project locks of a component's units. Waits queue fairly, hold
/// no connection, and give up after `within`, holding nothing.
pub trait Locker: Send + Sync + 'static {
    fn take<'a>(&'a self, key: &'a ProjectLockKey, within: Duration)
    -> BoxFuture<'a, Option<Held>>;
}

impl<D: ScopedBy<InProject>, P: ServingPool> DbPool<InProject, P, D> {
    /// The project's lock, exclusively: the unit runs alone among the
    /// project's locked units, reads what the one before it committed, and
    /// its instant follows that one's. For every unit that writes the
    /// project's rows. The unit is `REPEATABLE READ`: the lock, not the
    /// database, keeps still what it decides from, so it must be the
    /// project's only writer of the rows it decides on.
    #[track_caller]
    pub fn lock_project(&self) -> Locking<'_, D, P> {
        Locking {
            db: self,
            unit: Location::caller(),
        }
    }
}

/// A project-exclusive unit to open once it holds its lock, from
/// [`DbPool::lock_project`].
#[must_use = "nothing is locked until the unit begins"]
pub struct Locking<'a, D: ScopedBy<InProject>, P: ServingPool> {
    db: &'a DbPool<InProject, P, D>,
    unit: &'static Location<'static>,
}

impl<D: ScopedBy<InProject>, P: ServingPool> Locking<'_, D, P> {
    /// [`begin`](DbPool::begin), holding the lock from before it begins
    /// until it ends.
    pub async fn begin(
        self,
        _attempt: Attempt,
    ) -> Result<GatedConnection<RW<ProjectLock>, InProject, D>, TdError> {
        self.db.open(Unit::At(self.unit)).await
    }
}
