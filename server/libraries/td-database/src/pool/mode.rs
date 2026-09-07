//
// Copyright 2026 Tabsdata Inc.
//

//! [`PoolKind`] — which of a component's pools is meant. Crate-wide
//! vocabulary: config blocks ([`PoolsConfig`](crate::config::PoolsConfig)),
//! pool slots ([`ComponentPools`](crate::pool::ComponentPools)), the pools a
//! component declares ([`DbDomain::POOLS`](crate::domain::DbDomain::POOLS)),
//! and the log fields all key on it. The type-level counterparts a service
//! declares are the access markers ([`RO`](crate::access::RO) /
//! [`RW`](crate::access::RW) on the regular pool, [`XW`](crate::access::XW) on
//! the exclusive one).

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolKind {
    /// Every request that does not serialize: reads and ordinary writes.
    Regular,
    /// Serialized work only, and only where a component declares it: a
    /// connection from here holds its project's lock for its whole
    /// transaction, so its budget bounds how many can queue.
    Exclusive,
}

impl PoolKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            PoolKind::Regular => "regular",
            PoolKind::Exclusive => "exclusive",
        }
    }
}

impl fmt::Display for PoolKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
