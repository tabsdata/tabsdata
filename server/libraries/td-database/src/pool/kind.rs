//
// Copyright 2026 Tabsdata Inc.
//

//! [`PoolKind`] — which of a component's serving pools a unit runs on.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolKind {
    /// The requests' pool: every API read and write.
    Regular,
    /// The server's own units, the services whose handle says [`SystemPool`]:
    /// the scheduler, recovery, the probe dispatcher and the workers'
    /// internal calls. Its own connections, so the API cannot starve them nor
    /// they the API; a component without one runs them on `Regular`.
    System,
}

impl PoolKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            PoolKind::Regular => "regular",
            PoolKind::System => "system",
        }
    }
}

/// Which of a component's serving pools a
/// [`DbPool`](crate::access::DbPool) draws from, as its type: a service names
/// it in its signature, and every unit it opens uses that pool.
pub trait ServingPool: sealed::Sealed + Clone + Send + Sync + 'static {
    const KIND: PoolKind;
}

/// [`PoolKind::Regular`]: the requests' pool, every handle's by default.
#[derive(Debug, Clone, Copy)]
pub struct RegularPool;

/// [`PoolKind::System`]: the server's own units' pool.
#[derive(Debug, Clone, Copy)]
pub struct SystemPool;

impl ServingPool for RegularPool {
    const KIND: PoolKind = PoolKind::Regular;
}

impl ServingPool for SystemPool {
    const KIND: PoolKind = PoolKind::System;
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::RegularPool {}
    impl Sealed for super::SystemPool {}
}

impl fmt::Display for PoolKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
